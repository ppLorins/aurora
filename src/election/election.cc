/*
*   <Aurora. A raft based distributed KV storage system.>
*   Copyright (C) <2019>  <arthur> <pplorins@gmail.com>

*   This program is free software: you can redistribute it and/or modify
*   it under the terms of the GNU General Public License as published by
*   the Free Software Foundation, either version 3 of the License, or
*   (at your option) any later version.

*   This program is distributed in the hope that it will be useful,
*   but WITHOUT ANY WARRANTY; without even the implied warranty of
*   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*   GNU General Public License for more details.

*   You should have received a copy of the GNU General Public License
*   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <thread>
#include <random>
#include <vector>
#include <fstream>

#include "grpc/grpc.h"
#include "grpc++/grpc++.h"
#include "boost/filesystem.hpp"

#include "protocol/raft.grpc.pb.h"

#include "config/config.h"
#include "binlog/binlog_singleton.h"
#include "member/member_manager.h"
#include "global/global_env.h"
#include "client/client_impl.h"
#include "storage/storage.h"
#include "tools/timer.h"
#include "leader/leader_view.h"
#include "election/election.h"

#define _AURORA_ELECTION_TERM_PREFIX_    "current term:"
#define _AURORA_ELECTION_VOTEFOR_PREFIX_ "I [tried] voted for:"
#define _AURORA_ELECTION_KNOWN_VOTING_PREFIX_ "known others [tried] voted terms:"

namespace RaftCore::Election {

namespace  fs = ::boost::filesystem;
using ::RaftCore::BinLog::BinLogGlobal;
using ::raft::VoteResponse;
using ::raft::ErrorCode;
using ::RaftCore::Common::ReadLock;
using ::RaftCore::Common::WriteLock;
using ::RaftCore::State::StateMgr;
using ::RaftCore::Global::GlobalEnv;
using ::RaftCore::Timer::GlobalTimer;
using ::RaftCore::Leader::LeaderView;
using ::RaftCore::Member::MemberMgr;
using ::RaftCore::Member::JointConsensusMask;
using ::RaftCore::Member::EJointStatus;
using ::RaftCore::Client::PrevoteAsyncClient;
using ::RaftCore::Client::VoteAsyncClient;
using ::RaftCore::Client::WriteSyncClient;
using ::RaftCore::Storage::StorageMgr;

std::atomic<uint32_t>   ElectionMgr::m_cur_term;

std::map<uint32_t, std::string>   ElectionMgr::m_voted;

std::shared_timed_mutex  ElectionMgr::m_voted_mutex;

ElectionMgr::NewLeaderEvent   ElectionMgr::m_new_leader_event;

uint32_t   ElectionMgr::m_cur_cluster_size;

std::shared_timed_mutex    ElectionMgr::m_election_mutex;

std::map<uint32_t, std::set<std::string>>     ElectionMgr::m_known_voting;

std::shared_timed_mutex    ElectionMgr::m_known_voting_mutex;

std::thread*   ElectionMgr::m_p_thread = nullptr;

#ifdef _ELECTION_TEST_
volatile bool  ElectionMgr::m_candidate_routine_running = false;
#endif

MemberMgr::JointSummary  ElectionMgr::m_joint_snapshot;

uint32_t  ElectionMgr::m_cur_cluster_vote_counter;

uint32_t  ElectionMgr::m_new_cluster_vote_counter;

volatile bool   ElectionMgr::m_leader_debut = false;

LogIdentifier   ElectionMgr::m_pre_term_lrl;

TwoPhaseCommitBatchTask<std::string>     ElectionMgr::m_phaseI_task;

TwoPhaseCommitBatchTask<std::string>     ElectionMgr::m_phaseII_task;

void ElectionMgr::Initialize() noexcept {
    LoadFile();
}

void ElectionMgr::UnInitialize() noexcept {
    SaveFile();
}

void ElectionMgr::SaveFile() noexcept{

    //Destroy contents if file exists, and since the content is human readable , no need to open in binary mode.
    std::FILE* f_handler = std::fopen(_AURORA_ELECTION_CONFIG_FILE_, "w+");
    CHECK(f_handler != nullptr) << "open BaseState file " << _AURORA_ELECTION_CONFIG_FILE_ << "fail..,errno:" << errno;

    std::string _voted_info = "";
    {
        ReadLock _r_lock(m_voted_mutex);
        for (auto iter = m_voted.crbegin(); iter != m_voted.crend(); ++iter)
            _voted_info += (std::to_string(iter->first) + "|" + iter->second + ",");
    }

    std::string _known_voting = "";
    {
        ReadLock _r_lock(m_known_voting_mutex);
        for (auto &_pair_kv : m_known_voting) {
            std::string _votings = "";
            for (auto& _item : _pair_kv.second)
                _votings += _item + "%";
            _known_voting += (std::to_string(_pair_kv.first) + "|" + _votings + ",");
        }
    }

    std::string buf =  _AURORA_ELECTION_TERM_PREFIX_ + std::to_string(m_cur_term.load()) + "\n" +
                       _AURORA_ELECTION_VOTEFOR_PREFIX_ + _voted_info + "\n" +
                       _AURORA_ELECTION_KNOWN_VOTING_PREFIX_ + _known_voting + "\n";

    std::size_t written = fwrite(buf.data(), 1, buf.size(), f_handler);
    CHECK(written == buf.size()) << "fwrite BaseState file fail...,errno:" << errno << ",written:" << written << ",expected:" << buf.size();

    CHECK(!std::fclose(f_handler)) << "close BaseState file fail...,errno:" << errno;
}

void ElectionMgr::Reset() noexcept {
    m_new_leader_event.m_notify_flag = false;
}

void ElectionMgr::LoadFile() noexcept{

    m_cur_term.store(_MAX_UINT32_);

    {
        WriteLock _w_lock(m_voted_mutex);
        m_voted.clear();
    }

    {
        WriteLock _w_lock(m_known_voting_mutex);
        m_known_voting.clear();
    }

    //The local scope is to release the handle by std::ifstream.
    std::ifstream f_input(_AURORA_ELECTION_CONFIG_FILE_);

    for (std::string _ori_line; std::getline(f_input, _ori_line); ) {
        std::string _line = "";
        _line.reserve(_ori_line.length());
        std::copy_if(_ori_line.begin(), _ori_line.end(), std::back_inserter(_line), [](char c) { return c != '\r' && c != '\n'; });

        if (_line.find(_AURORA_ELECTION_TERM_PREFIX_) != std::string::npos) {
            std::size_t pos = _line.find(":");
            CHECK (pos != std::string::npos)  << "cannot find delimiter[:] in state file, _line:" << _line;
            m_cur_term.store(std::atol(_line.substr(pos + 1).c_str()));
            continue;
        }

        if (_line.find(_AURORA_ELECTION_VOTEFOR_PREFIX_) != std::string::npos) {
            std::size_t pos = _line.find(":");
            CHECK (pos != std::string::npos)  << "cannot find delimiter[:] in state file, _line:" << _line;

            std::list<std::string> _output;
            ::RaftCore::Tools::StringSplit(_line.substr(pos + 1),',',_output);
            for (const auto &_item : _output) {
                std::list<std::string> _inner_output;
                ::RaftCore::Tools::StringSplit(_item,'|',_inner_output);
                CHECK(_inner_output.size() == 2);

                auto _iter = _inner_output.cbegin();
                uint32_t _term = std::atol((*_iter++).c_str());

                WriteLock _w_lock(m_voted_mutex);
                m_voted[_term] = *_iter;
            }

            continue;
        }

        if (_line.find(_AURORA_ELECTION_KNOWN_VOTING_PREFIX_) != std::string::npos) {
            std::size_t pos = _line.find(":");
            CHECK (pos != std::string::npos) << "cannot find delimiter[:] in state file, _line:" << _line;

            std::list<std::string> _term_list;
            ::RaftCore::Tools::StringSplit(_line.substr(pos + 1),',',_term_list);
            for (const auto &_item : _term_list) {
                std::list<std::string> _voting_list;
                ::RaftCore::Tools::StringSplit(_item,'|', _voting_list);
                CHECK(_voting_list.size() == 2);

                auto _iter = _voting_list.cbegin();
                uint32_t _term = std::atol((*_iter++).c_str());

                std::list<std::string> _votings;
                ::RaftCore::Tools::StringSplit(*_iter,'%',_votings);

                WriteLock _w_lock(m_known_voting_mutex);
                if (m_known_voting.find(_term) == m_known_voting.cend())
                    m_known_voting[_term] = std::set<std::string>();

                for (const auto& _item : _votings)
                    m_known_voting[_term].emplace(_item);
            }

            continue;
        }
    }

    f_input.close();

    //Give default values if not found in the state file
    bool give_default = false;
    if (m_cur_term.load() == _MAX_UINT32_) {
        m_cur_term.store(0); //term started from 0
        give_default = true;
    }

    if (give_default)
        ElectionMgr::SaveFile();
}

void ElectionMgr::ElectionThread() noexcept{

    //Reset election environment before doing it.
    Reset();

    auto _entrance = [&]() ->void{

#ifdef _ELECTION_TEST_
        m_candidate_routine_running = true;
#endif

        LOG(INFO) << "start electing,switch role:[Follower --> Candidate]";
        SwitchRole(RaftRole::CANDIDATE);
        CandidateRoutine();

#ifdef _ELECTION_TEST_
        m_candidate_routine_running = false;
#endif
    };

    if (m_p_thread)
        delete m_p_thread;

    m_p_thread = new std::thread(_entrance);
    m_p_thread->detach();
}

#ifdef _ELECTION_TEST_
void ElectionMgr::WaitElectionThread()noexcept {
    while (m_candidate_routine_running)
        std::this_thread::sleep_for(std::chrono::microseconds(::RaftCore::Config::FLAGS_election_thread_wait_us));
}
#endif

std::string ElectionMgr::TryVote(uint32_t term, const std::string &addr)noexcept {

    {
        WriteLock _w_lock(m_voted_mutex);
        if (m_voted.find(term) != m_voted.end()) {
            LOG(WARNING) << "voting in a term that is already been voted before:"
                << term << ",original voted addr:" << m_voted[term] << ",current trying to vote addr:" << addr;
            return m_voted[term];
        }
        m_voted[term] = addr;
    }

    SaveFile();

    return "";
}

void ElectionMgr::RenameBinlogNames(RaftRole old_role, RaftRole target_role) noexcept {
    const char* _old_role_str    = StateMgr::GetRoleStr(old_role);
    const char* _target_role_str = StateMgr::GetRoleStr(target_role);

    std::list<std::string>   _binlog_files;
    StorageMgr::FindRoleBinlogFiles(_old_role_str, _binlog_files);

    for (const auto&file_name : _binlog_files) {
        std::string _target_file_name = file_name;

        auto _pos = _target_file_name.find(_old_role_str);
        CHECK(_pos != std::string::npos) << "rename binlog fail when switching role, old_file_name:" << file_name << ",old_role:" << _old_role_str;

        _target_file_name.replace(_pos, std::strlen(_old_role_str), _target_role_str);

        fs::path _target_file(_target_file_name);
        if (fs::exists(_target_file)){
            LOG(WARNING) << "target binlog exist, delete it:" << _target_file_name;
            fs::remove(_target_file);
        }

        CHECK(std::rename(file_name.c_str(), _target_file_name.c_str()) == 0) << "rename binlog file fail...,errno:" << errno;

        LOG(INFO) << "Switching role, rename binlog name from :" << file_name << " to " << _target_file_name;
    }
}

void ElectionMgr::SwitchRole(RaftRole target_role, const std::string &new_leader) noexcept {

    auto _old_role = StateMgr::GetRole();
    if (_old_role == target_role) {
        LOG(WARNING) << "same role switching detected ,from " << StateMgr::GetRoleStr(_old_role)
            << " to " << StateMgr::GetRoleStr(target_role);
        return;
    }

    StateMgr::SwitchTo(target_role,new_leader);

    //Re-initialize global env.
    GlobalEnv::UnInitialEnv(_old_role);

    //Switch role also needs to rename binlog file names.
    RenameBinlogNames(_old_role, target_role);

    /*Only after `GlobalEnv::UnInitialEnv` could StateMgr::SwitchTo be called since otherwise
       `GlobalEnv::UnInitialEnv`  would read the modified current state.  */
    GlobalEnv::InitialEnv(true);
}

void ElectionMgr::CandidateRoutine() noexcept{

    std::random_device rd;
    std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()

    int _sleep_min = ::RaftCore::Config::FLAGS_election_term_interval_min_ms;
    int _sleep_max = ::RaftCore::Config::FLAGS_election_term_interval_max_ms;
    std::uniform_int_distribution<> dis(_sleep_min, _sleep_max);

    //For a consistent read , we need  a snap shot.
    m_joint_snapshot.Reset();
    {
        ReadLock _r_lock(MemberMgr::m_mutex);
        m_joint_snapshot = MemberMgr::m_joint_summary;
    }

    Topology _topo;
    CTopologyMgr::Read(&_topo);

    m_cur_cluster_size = _topo.GetClusterSize();
    PreparePrevoteTask(_topo);

    std::shared_ptr<VoteRequest> _shp_req(new VoteRequest());

    _shp_req->mutable_base()->set_addr(StateMgr::GetMyAddr());
    _shp_req->mutable_base()->set_term(m_cur_term.load());

    auto _lrl = BinLogGlobal::m_instance.GetLastReplicated();
    _shp_req->mutable_last_log_entity()->set_term(_lrl.m_term);
    _shp_req->mutable_last_log_entity()->set_idx(_lrl.m_index);

    _shp_req->set_member_version(MemberMgr::GetVersion());

    /*IMPORTANT : This is the term before the first round of election.We need a backing term mechanism
       otherwise candidate doesn't have a change to be instructed by the leader which is either
       newly elected or the old spurious failed one.  */
    auto _start_term = m_cur_term.load();

    while (true) {
        int _sleep_random = dis(gen);
        std::this_thread::sleep_for(std::chrono::milliseconds(_sleep_random));

        LOG(INFO) << "[Candidate] Just slept " << _sleep_random << " milliseconds under term :" << m_cur_term.load();

        if (IncreaseToMaxterm())
            break;

        LOG(INFO) << "[Candidate] I'm successfully increased to term:" << m_cur_term.load() << ",start issue pre-voting requests to the other nodes.";

        //Update term in the request, too.
        _shp_req->mutable_base()->set_term(m_cur_term.load());

        if (!BroadcastVoting(_shp_req, _topo, VoteType::PreVote)) {
            LOG(INFO) << "[Candidate] pre-vote rejected term: " << m_cur_term.load() << ",now back term to " << _start_term
                << " ,may because the current node temporarily losing heartbeat messages from leader, "
                << "yet the leader is still alive to the other nodes,switch role:[Candidate --> Follwoer]";
            //A role switching from Candidate-->Follower should also revert the term to avoid infinite starting new round of election.
            m_cur_term.store(_start_term);
            SwitchRole(RaftRole::FOLLOWER);
            break;
        }

        LOG(INFO) << "[Candidate] pre-voting succeed under term: " << m_cur_term.load()
            << ",start issue voting requests to the other nodes.";

        if (!BroadcastVoting(_shp_req, _topo, VoteType::Vote)) {
            LOG(INFO) << "[Candidate] vote rejected term: " << m_cur_term.load() << ",starting a new round, "
                    <<"and back my term to the starting term:" <<_start_term;
            /*Note: There is a term going back policy, to prevent candidate with a term always greater than the
              newly elected leader ,resulting in infinite starting new election round.  */
            m_cur_term.store(_start_term);
            continue;
        }

        LOG(INFO) << "[Candidate] voting success! Become the new leader of term: " << m_cur_term.load()
                    << ",switch role:[Candidate --> Leader]" ;

        //Record the snapshot of LRL for being used in leader's new term.
        m_pre_term_lrl.Set(_lrl);
        m_leader_debut = true;

        SwitchRole(RaftRole::LEADER);

        /*After successfully elected as leader, we submit a non-op log to ensure logs consistent
         amid the new topology in advance. To simplify, we just sent a write request as a usual
         client and this operation could be time consuming due to the possible log-resync process.
         Adding that the non-op isn't necessary in aurora's design(normal subsequent requests will
         also trigger the resync process if it's required), this is just to finish the resync job
         ASAP after a new leader elected..  */

        auto _heartbeat = []()->bool {
            LeaderView::BroadcastHeatBeat();
            return false; //One shot.
        };
        GlobalTimer::AddTask(0, _heartbeat); //Intend to execute immediately.

        //Wait sometime to ensure the heartbeat has been sent & acknowledged by the followers.
        std::this_thread::sleep_for(std::chrono::milliseconds(::RaftCore::Config::FLAGS_election_wait_non_op_finish_ms));

        LOG(INFO) << "[Leader] start issue non-op request";

        //First one is to make the lag behind followers to catch up.
        std::string _tag = std::to_string(_sleep_random) + "_prepare";
        SentNonOP(_tag);

        //Wait sometime to ensure the followers has caught up.
        std::this_thread::sleep_for(std::chrono::milliseconds(::RaftCore::Config::FLAGS_election_wait_non_op_finish_ms));

        //Second one is to truncate the overstepped followers to truncate the additional logs.
        _tag = std::to_string(_sleep_random) + "_commit";
        SentNonOP(_tag);

        break;
    }
}

void ElectionMgr::SentNonOP(const std::string &tag) noexcept {

    std::string _local_add = _AURORA_LOCAL_IP_ + std::string(":") + std::to_string(::RaftCore::Config::FLAGS_port);
    std::shared_ptr<::grpc::Channel>  _channel = grpc::CreateChannel(_local_add, grpc::InsecureChannelCredentials());

    WriteSyncClient  _write_client(_channel);

    auto _setter = [&](std::shared_ptr<::raft::ClientWriteRequest>& req) {
        req->mutable_req()->set_key("aurora-reserved-non-op-key_" + tag);
        req->mutable_req()->set_value("aurora-reserved-non-op-value_" + tag);
    };

    auto _rpc = std::bind(&::raft::RaftService::Stub::Write, _write_client.GetStub().get(),
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

    ::grpc::Status  _status;
    auto &_rsp = _write_client.DoRPC(_setter, _rpc, ::RaftCore::Config::FLAGS_election_non_op_timeo_ms, _status);

    if (!_status.ok()) {
        LOG(ERROR) << "submit non-op fail,err msg:" << _status.error_message();
        return;
    }

    if (_rsp.client_comm_rsp().result() != ::raft::ErrorCode::SUCCESS) {
        LOG(ERROR) << "submit non-op fail,err msg:" << _rsp.client_comm_rsp().err_msg();
        return;
    }

    LOG(INFO) << "submit non-op succeed.";
}

bool ElectionMgr::IncreaseToMaxterm() noexcept{

    auto _cur_term = m_cur_term.load();

    //Quickly find the next votable term.
    while (true) {

        if (m_new_leader_event.m_notify_flag) {
            LOG(INFO) << "[Candidate] a higher term found , switch role:[Candidate --> Follwoer]. ";
            m_cur_term.store(m_new_leader_event.m_new_leader_term);
            SwitchRole(RaftRole::FOLLOWER,m_new_leader_event.m_new_leader_addr);
            return true;
        }

        auto _old_term = _cur_term;
        _cur_term = _cur_term + 1;

        LOG(INFO) << "[Candidate] term increased normally : " << _old_term << " --> " << _cur_term;

        auto _find_to_max_term = [&]()->void {
            ReadLock _r_lock(m_known_voting_mutex);
            auto _last_iter = m_known_voting.crbegin();
            if (_last_iter == m_known_voting.crend())
                return;

            /*Make sure the term to be used is greater than the largest known one at present, to get
              rid of term conflict as far as possible.*/
            if (_cur_term <= _last_iter->first) {
                _old_term = _cur_term;
                _cur_term = _last_iter->first + 1;
                LOG(INFO) << "[Candidate] term increased jumping: " << _old_term << " --> " << _cur_term;
            }
        };
        _find_to_max_term();

        auto _voted_addr = TryVote(_cur_term, StateMgr::GetMyAddr());
        if (!_voted_addr.empty()) {
            LOG(INFO) << "[Candidate] I'm candidate,voting myself at term " << _cur_term
                    << " fail,found I've voted some other nodes under this term ,that is:" << _voted_addr;
            continue;
        }

        break;
    }

    m_cur_term.store(_cur_term);

    return false;
}

void ElectionMgr::NotifyNewLeaderEvent(uint32_t term,const std::string addr)noexcept {
    //The field updating order is important.
    m_new_leader_event.m_new_leader_term = term;
    m_new_leader_event.m_new_leader_addr = addr;
    m_new_leader_event.m_notify_flag = true;
}

void ElectionMgr::PreparePrevoteTask(const Topology &topo)noexcept {

    auto _add = [&](const std::string & node_addr) ->void{
        if (node_addr == StateMgr::GetMyAddr())
            return;

        m_phaseI_task.m_todo.emplace_back(node_addr);

        uint32_t _flag = int(JointConsensusMask::IN_OLD_CLUSTER);

        if (m_joint_snapshot.m_joint_status == EJointStatus::JOINT_CONSENSUS) {
            const auto& _new_cluster = m_joint_snapshot.m_joint_topology.m_new_cluster;
            if (_new_cluster.find(node_addr) != _new_cluster.cend())
                _flag |= int(JointConsensusMask::IN_NEW_CLUSTER);
        }

        m_phaseI_task.m_flags.emplace_back(_flag);
    };
    std::for_each(topo.m_followers.cbegin(), topo.m_followers.cend(), _add);
    std::for_each(topo.m_candidates.cbegin(), topo.m_candidates.cend(), _add);

    if (m_joint_snapshot.m_joint_status == EJointStatus::JOINT_CONSENSUS) {
        const auto& _new_nodes = m_joint_snapshot.m_joint_topology.m_added_nodes;
        for (auto _iter = _new_nodes.cbegin(); _iter != _new_nodes.cend(); ++_iter) {
            m_phaseI_task.m_todo.emplace_back(_iter->first);
            m_phaseI_task.m_flags.emplace_back(uint32_t(JointConsensusMask::IN_NEW_CLUSTER));
        }
    }
}

bool ElectionMgr::BroadcastVoting(std::shared_ptr<VoteRequest> shp_req, const Topology &topo,
    VoteType vote_type) noexcept{

    TwoPhaseCommitBatchTask<std::string>* _p_task_list = &m_phaseI_task;
    if (vote_type == VoteType::Vote)
        _p_task_list = &m_phaseII_task;

    std::shared_ptr<::grpc::CompletionQueue> _shp_cq(new ::grpc::CompletionQueue());

    auto _req_setter = [&shp_req](std::shared_ptr<::raft::VoteRequest>& _target)->void {
        _target = shp_req;
    };

    auto _entrust_prevote_client = [&](auto &_shp_channel,std::size_t idx){
        auto _shp_client = new PrevoteAsyncClient(_shp_channel, _shp_cq);
        _shp_client->PushCallBackArgs(reinterpret_cast<void*>(idx));
        auto _f_prepare =  std::bind(&::raft::RaftService::Stub::PrepareAsyncPreVote,
                                    _shp_client->GetStub().get(), std::placeholders::_1,
                                    std::placeholders::_2, std::placeholders::_3);
        _shp_client->EntrustRequest(_req_setter, _f_prepare, ::RaftCore::Config::FLAGS_election_vote_rpc_timeo_ms);
    };

    auto _entrust_vote_client = [&](auto &_shp_channel,std::size_t idx){
        auto _shp_client = new VoteAsyncClient(_shp_channel, _shp_cq);
        _shp_client->PushCallBackArgs(reinterpret_cast<void*>(idx));
        auto _f_prepare =  std::bind(&::raft::RaftService::Stub::PrepareAsyncVote,
                                    _shp_client->GetStub().get(), std::placeholders::_1,
                                    std::placeholders::_2, std::placeholders::_3);
        _shp_client->EntrustRequest(_req_setter, _f_prepare, ::RaftCore::Config::FLAGS_election_vote_rpc_timeo_ms);
    };

    int _entrust_total_num = 0;
    for (std::size_t i = 0; i < _p_task_list->m_todo.size(); ++i) {
        auto _shp_channel = ::grpc::CreateChannel(_p_task_list->m_todo[i], ::grpc::InsecureChannelCredentials());
        if (vote_type == VoteType::PreVote)
            _entrust_prevote_client(_shp_channel,i);
        else
            _entrust_vote_client(_shp_channel,i);
        _entrust_total_num++;
    }

    //initialized to 1 means including myself.
    m_cur_cluster_vote_counter = 1;
    m_new_cluster_vote_counter = 1;

    PollingCQ(_shp_cq,_entrust_total_num);

    uint32_t _cur_cluster_majority  = m_cur_cluster_size / 2 + 1;

    bool _succeed = m_cur_cluster_vote_counter >= _cur_cluster_majority;

    if (m_joint_snapshot.m_joint_status == EJointStatus::JOINT_CONSENSUS) {
        std::size_t _new_cluster_total_nodes = m_joint_snapshot.m_joint_topology.m_new_cluster.size();
        std::size_t _new_cluster_majority    = _new_cluster_total_nodes / 2 + 1;
        _succeed &= (m_new_cluster_vote_counter >= _new_cluster_majority);
    }

    return _succeed;
}

void ElectionMgr::PollingCQ(std::shared_ptr<::grpc::CompletionQueue> shp_cq,int entrust_num)noexcept {
    void* tag;
    bool ok;

    int _counter = 0;
    while (_counter < entrust_num) {
        if (!shp_cq->Next(&tag, &ok))
            break;

        ::RaftCore::Common::ReactBase* _p_ins = (::RaftCore::Common::ReactBase*)tag;
        _p_ins->React(ok);
        _counter++;
    }
}

void ElectionMgr::AddVotingTerm(uint32_t term,const std::string &addr) noexcept{

    WriteLock _w_lock(m_known_voting_mutex);

    LOG(INFO) << "add known voting term " << term << " from " << addr;

    if (m_known_voting.find(term) == m_known_voting.cend())
        m_known_voting[term] = std::set<std::string>();

    m_known_voting[term].emplace(addr);
}

void ElectionMgr::CallBack(const ::grpc::Status &status, const ::raft::VoteResponse& rsp,
    VoteType vote_type,uint32_t idx) noexcept {

    TwoPhaseCommitBatchTask<std::string>* _p_task_list = &m_phaseI_task;
    if (vote_type == VoteType::Vote)
        _p_task_list = &m_phaseII_task;

    if (!status.ok()){
        LOG(ERROR) << "rpc status fail,idx:" << idx << ",addr:" << _p_task_list->m_todo[idx]
            << ",error code:" << status.error_code() << ",error msg:" << status.error_message() ;
        return;
    }

    std::string _vote_type_str = (vote_type == VoteType::Vote) ?"vote" : "prevote";

    ErrorCode _err_code = (vote_type == VoteType::Vote) ? ErrorCode::VOTE_YES : ErrorCode::PREVOTE_YES;
    if (rsp.comm_rsp().result() != _err_code) {
        LOG(INFO) << "peer " << _p_task_list->m_todo[idx] << " rejected " << _vote_type_str
                 << ",error message:" << rsp.comm_rsp().err_msg();
        return;
    }

    LOG(INFO) << "peer " << _p_task_list->m_todo[idx] << " approved,vote type: " << _vote_type_str;

    if (_p_task_list->m_flags[idx] & int(JointConsensusMask::IN_OLD_CLUSTER))
        m_cur_cluster_vote_counter++;

    if (_p_task_list->m_flags[idx] & int(JointConsensusMask::IN_NEW_CLUSTER))
        m_new_cluster_vote_counter++;

    if (vote_type == VoteType::PreVote) {
        m_phaseII_task.m_todo.emplace_back(_p_task_list->m_todo[idx]);
        m_phaseII_task.m_flags.emplace_back(_p_task_list->m_flags[idx]);
    }
}

}

