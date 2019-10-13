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

#include <string>

#include "grpc/grpc.h"
#include "grpc++/grpc++.h"

#include "protocol/raft.grpc.pb.h"

#include "common/comm_defs.h"
#include "config/config.h"
#include "topology/topology_mgr.h"
#include "storage/storage_singleton.h"
#include "leader/follower_entity.h"
#include "leader/leader_view.h"
#include "leader/client_pool.h"
#include "tools/lock_free_priority_queue.h"
#include "tools/utilities.h"
#include "state/state_mgr.h"
#include "election/election.h"
#include "global/global_env.h"
#include "client/client_impl.h"
#include "member/member_manager.h"

#define _AURORA_MEMBER_CLUSTER_STATUS_PREFIX_ "cluster status:"
#define _AURORA_MEMBER_NEW_CLUSTER_PREFIX_ "new cluster:"
#define _AURORA_MEMBER_VERSION_PREFIX_ "version:"

namespace RaftCore::Member {

std::condition_variable   MemberMgr::m_resync_data_cv;

std::mutex   MemberMgr::m_resync_data_cv_mutex;

MemberMgr::JointSummary   MemberMgr::m_joint_summary;

std::shared_timed_mutex    MemberMgr::m_mutex;

std::atomic<bool>     MemberMgr::m_in_processing;

MemberMgr::JointTopology MemberMgr::m_joint_topo_snapshot;

const char*  MemberMgr::m_macro_names[] = {"STABLE","JOINT_CONSENSUS"};

MemberMgr::MemberChangeContext   MemberMgr::m_memchg_ctx;

TwoPhaseCommitBatchTask<TypePtrFollowerEntity>       MemberMgr::m_phaseI_task;

TwoPhaseCommitBatchTask<TypePtrFollowerEntity>       MemberMgr::m_phaseII_task;

#ifdef _MEMBER_MANAGEMENT_TEST_
bool    MemberMgr::m_execution_flag = false;
#endif

using ::raft::ErrorCode;
using ::grpc::CompletionQueue;
using ::RaftCore::Common::WriteLock;
using ::RaftCore::Common::ReadLock;
using ::RaftCore::Common::FinishStatus;
using ::RaftCore::Leader::FollowerEntity;
using ::RaftCore::Leader::FollowerStatus;
using ::RaftCore::Storage::StorageGlobal;
using ::RaftCore::Topology;
using ::RaftCore::CTopologyMgr;
using ::RaftCore::Leader::LeaderView;
using ::RaftCore::Leader::BackGroundTask::ReSyncLogContext;
using ::RaftCore::DataStructure::LockFreePriotityQueue;
using ::RaftCore::State::StateMgr;
using ::RaftCore::State::RaftRole;
using ::RaftCore::Election::ElectionMgr;
using ::RaftCore::Global::GlobalEnv;
using ::RaftCore::Tools::TypeSysTimePoint;
using ::RaftCore::Client::MemberChangePrepareAsyncClient;
using ::RaftCore::Client::MemberChangeCommitAsyncClient;

const MemberMgr::JointTopology& MemberMgr::JointTopology::operator=(const MemberMgr::JointTopology &one) {
    this->m_new_cluster      = one.m_new_cluster;
    this->m_added_nodes      = one.m_added_nodes;
    this->m_removed_nodes    = one.m_removed_nodes;
    this->m_leader_gone_away = one.m_leader_gone_away;
    this->m_old_leader       = one.m_old_leader;

    return *this;
}

const MemberMgr::JointTopology& MemberMgr::JointTopology::operator=(MemberMgr::JointTopology &&one) {
    this->m_new_cluster      = std::move(one.m_new_cluster);
    this->m_added_nodes      = std::move(one.m_added_nodes);
    this->m_removed_nodes    = std::move(one.m_removed_nodes);
    this->m_leader_gone_away = one.m_leader_gone_away;
    this->m_old_leader       = one.m_old_leader;

    return *this;
}

void MemberMgr::JointTopology::Reset() noexcept{
    this->m_new_cluster.clear();
    this->m_added_nodes.clear();
    this->m_removed_nodes.clear();
    this->m_leader_gone_away = false;
    this->m_old_leader = "";
}

void MemberMgr::JointTopology::Update(const std::set<std::string> * p_new_cluster)noexcept {

    if (p_new_cluster)
        this->m_new_cluster = *p_new_cluster;

    //Topology should be ready for reading.
    Topology    _cur_topo;
    CTopologyMgr::Read(&_cur_topo);

    bool _is_leader = StateMgr::GetRole() == RaftRole::LEADER;

    //Find added nodes.
    this->m_added_nodes.clear();

    for (const auto& _item : this->m_new_cluster) {

        if (_cur_topo.InCurrentCluster(_item))
            continue;

        FollowerEntity* _p_follower = nullptr;
        if (_is_leader)
            _p_follower = new FollowerEntity(_item, FollowerStatus::RESYNC_LOG,
                uint32_t(JointConsensusMask::IN_NEW_CLUSTER));

        this->m_added_nodes.emplace(_item, _p_follower);
    }

    //Find removed nodes.
    this->m_removed_nodes.clear();
    {
        ReadLock _r_lock(LeaderView::m_hash_followers_mutex);
        for (const auto& _pair : LeaderView::m_hash_followers)
            if (this->m_new_cluster.find(_pair.first) == this->m_new_cluster.cend())
                this->m_removed_nodes.emplace(_pair.first);
    }

    this->m_leader_gone_away = (this->m_new_cluster.find(_cur_topo.m_leader)==this->m_new_cluster.cend());
    this->m_old_leader = _cur_topo.m_leader;
}

void MemberMgr::JointSummary::Reset()noexcept {
    this->m_joint_status = EJointStatus::STABLE;
    this->m_joint_topology.Reset();
    //m_version is monotonic,shouldn't been reset in any time.
}

void MemberMgr::Initialize() noexcept {
    m_in_processing.store(false);
    m_joint_summary.Reset();
    ResetMemchgEnv();
    LoadFile();
}

void MemberMgr::UnInitialize() noexcept {
    m_joint_summary.Reset();
}

void MemberMgr::ResetMemchgEnv() noexcept {
    //Reset all the followings before using them.
    m_joint_topo_snapshot.Reset();
    m_memchg_ctx.Reset();
}

void MemberMgr::LoadFile() noexcept {
    //Read the config file.
    std::ifstream f_input(_AURORA_MEMBER_CONFIG_FILE_);

    for (std::string _ori_line; std::getline(f_input, _ori_line); ) {

        std::string _line = "";
        std::copy_if(_ori_line.begin(), _ori_line.end(), std::back_inserter(_line), [](char c) { return c != '\r' && c != '\n'; });

        if (_line.find(_AURORA_MEMBER_CLUSTER_STATUS_PREFIX_) != std::string::npos) {
            std::size_t pos = _line.find(":");
            CHECK (pos != std::string::npos) << "cannot find delimiter[:] in member config file, _line:" << _line;
            m_joint_summary.m_joint_status = StringToMacro(_line.substr(pos + 1).c_str());
            continue;
        }

        if (_line.find(_AURORA_MEMBER_NEW_CLUSTER_PREFIX_) != std::string::npos) {
            std::size_t pos = _line.find(":");
            CHECK (pos != std::string::npos)  << "cannot find delimiter[:] in member config file, _line:" << _line;
            ::RaftCore::Tools::StringSplit(_line.substr(pos + 1),',',m_joint_summary.m_joint_topology.m_new_cluster);
            continue;
        }

        if (_line.find(_AURORA_MEMBER_VERSION_PREFIX_) != std::string::npos) {
            std::size_t pos = _line.find(":");
            CHECK (pos != std::string::npos)  << "cannot find delimiter[:] in member config file, _line:" << _line;
            m_joint_summary.m_version = std::atol(_line.substr(pos + 1).c_str());
            continue;
        }
    }

    m_joint_summary.m_joint_topology.Update();
}

void MemberMgr::SaveFile() noexcept{

    ReadLock _r_lock(m_mutex);
    std::FILE* f_handler = std::fopen(_AURORA_MEMBER_CONFIG_FILE_, "w+");
    CHECK(f_handler != nullptr) << "open BaseState file " << _AURORA_MEMBER_CONFIG_FILE_ << "fail..,errno:" << errno;

    auto &_cluster_topo = m_joint_summary.m_joint_topology;

    std::string _new_cluster = "";
    for (auto iter = _cluster_topo.m_new_cluster.crbegin(); iter != _cluster_topo.m_new_cluster.crend(); ++iter)
        _new_cluster += ((*iter) +  ",");

    std::string buf = _AURORA_MEMBER_CLUSTER_STATUS_PREFIX_ + std::string(MacroToString(m_joint_summary.m_joint_status)) + "\n"
                    + _AURORA_MEMBER_NEW_CLUSTER_PREFIX_ + _new_cluster + "\n"
                    + _AURORA_MEMBER_VERSION_PREFIX_ + std::to_string(m_joint_summary.m_version) + "\n";

    std::size_t written = fwrite(buf.data(), 1, buf.size(), f_handler);
    CHECK(written == buf.size()) << "fwrite BaseState file fail...,errno:" << errno << ",written:" << written << ",expected:" << buf.size();
    CHECK(!std::fclose(f_handler)) << "close BaseState file fail...,errno:" << errno;
}

void MemberMgr::NotifyOnSynced(TypePtrFollowerEntity &shp_follower) noexcept {

    LOG(INFO) << "[Membership Change] peer " << shp_follower->my_addr << " notify called,switched to NORMAL status";

    shp_follower->m_status = FollowerStatus::NORMAL;

    std::unique_lock<std::mutex> _lock(m_resync_data_cv_mutex);
    m_resync_data_cv.notify_all();
}

void MemberMgr::SwitchToJointConsensus(JointTopology &updated_topo,uint32_t version)noexcept {

    WriteLock _w_lock(m_mutex);

    m_joint_summary.m_joint_status   = EJointStatus::JOINT_CONSENSUS;
    m_joint_summary.m_joint_topology = std::move(updated_topo);

    //Update old cluster followers' status.
    if (StateMgr::GetRole() == RaftRole::LEADER) {
        for (const auto& _item : m_joint_summary.m_joint_topology.m_new_cluster) {
            ReadLock _r_lock(LeaderView::m_hash_followers_mutex);
            auto _iter = LeaderView::m_hash_followers.find(_item);
            if (_iter != LeaderView::m_hash_followers.cend())
                _iter->second->m_joint_consensus_flag |= uint32_t(JointConsensusMask::IN_NEW_CLUSTER);
        }
    }

    m_joint_summary.m_version++;
    if (version != _MAX_UINT32_)
        m_joint_summary.m_version = version;
    _w_lock.unlock();

    //Persist changes.
    SaveFile();
}

uint32_t MemberMgr::GetVersion()noexcept {
    ReadLock _r_lock(m_mutex);
    return m_joint_summary.m_version;
}

bool MemberMgr::SwitchToStable()noexcept {

    /*Since topology config and membership-change config are in separated files, they cannot be updated
      atomically, but if we update topology first ,the leader or follower server can still serve normally
      after recovered from a crash.  */
    ::RaftCore::Topology    _old_topo;
    ::RaftCore::CTopologyMgr::Read(&_old_topo);

    //Update to the latest topology.
    ::RaftCore::Topology    _new_topo;

    {
        ReadLock _w_lock(m_mutex);
        auto &_cluster_topo = m_joint_summary.m_joint_topology;
        for (const auto &_node : _cluster_topo.m_new_cluster) {
            if (_node == _old_topo.m_leader) {
                _new_topo.m_leader = _node;
                continue;
            }
            _new_topo.m_followers.emplace(_node);
        }
        _new_topo.m_my_addr = _old_topo.m_my_addr;
        ::RaftCore::CTopologyMgr::Update(_new_topo);

        //Update to latest follower list in leader's view.
        if (StateMgr::GetRole() == RaftRole::LEADER) {
            WriteLock _r_lock(LeaderView::m_hash_followers_mutex);
            LeaderView::m_hash_followers.clear();
            for (const auto &_node : _cluster_topo.m_new_cluster)
                if (_node != _new_topo.m_leader)
                    LeaderView::m_hash_followers[_node] = std::shared_ptr<FollowerEntity>(new FollowerEntity(_node));
        }
    }

    {
        WriteLock _w_lock(m_mutex);
        m_joint_summary.Reset();
        m_joint_summary.m_version++;
    }

    //Persist changes.
    SaveFile();

    return _new_topo.InCurrentCluster(StateMgr::GetMyAddr());
}

std::string MemberMgr::FindPossibleAddress(const std::list<std::string> &nic_addrs)noexcept{
    ReadLock _r_lock(m_mutex);

    auto &_new_nodes = m_joint_summary.m_joint_topology.m_added_nodes;
    for (const auto &_item : nic_addrs) {
        auto _iter = _new_nodes.find(_item);
        if (_iter != _new_nodes.cend())
            return _iter->first;
    }

    return "";
}

const char* MemberMgr::MacroToString(EJointStatus enum_val) noexcept {
    return m_macro_names[int(enum_val)];
}

EJointStatus MemberMgr::StringToMacro(const char* src) noexcept {
    int _size = sizeof(m_macro_names) / sizeof(const char*);
    for (int i = 0; i < _size; ++i)
        if (std::strncmp(src, m_macro_names[i], std::strlen(m_macro_names[i])) == 0)
            return (EJointStatus)i;

    CHECK(false) << "convert string to enum fail,unknown cluster status :" << src;

    //Just for erase compile warnings.
    return EJointStatus::STABLE;
}

const char* MemberMgr::PullTrigger(const std::set<std::string> &new_cluster)noexcept {

    bool _in_processing = false;
    if (!m_in_processing.compare_exchange_strong(_in_processing, true)) {
        static const char * _p_err_msg = "I'm changing the membership now,cannot process another changing request.";
        LOG(ERROR) << "[MembershipChange] " << _p_err_msg;
        return _p_err_msg;
    }

    /*Issuing a resync log task to the background threads.This will cover all cases:
        1. the new node's log doesn't lag too far behind ,a few resync log operations is enough.
        2. the new node's log do lag too far behind , will trigger the resync-data operation eventually.
        3. the new node's log is empty , trigger resync-data operation eventually.  */

    /*Using ID-LCL instead of ID-LRL to reduce the amount of log entries each resync log RPC may carry,
      even though both the two options will eventually triggered the SYNC-DATA process. */
    auto _id_lcl = StorageGlobal::m_instance.GetLastCommitted();

    m_joint_topo_snapshot.Update(&new_cluster);

    {
        ReadLock _r_lock(m_mutex);
        for (auto _iter = m_joint_topo_snapshot.m_added_nodes.cbegin(); _iter != m_joint_topo_snapshot.m_added_nodes.cend(); ++_iter) {
            std::shared_ptr<ReSyncLogContext> _shp_task(new ReSyncLogContext());
            _shp_task->m_last_sync_point = _id_lcl;
            _shp_task->m_follower = _iter->second;
            _shp_task->m_on_success_cb = &MemberMgr::NotifyOnSynced;

            auto _ret_code = LeaderView::m_priority_queue.Push(LockFreePriotityQueue::TaskType::RESYNC_LOG, &_shp_task);
            if (_ret_code != QUEUE_SUCC)
                LOG(INFO) << "[Membership Change] Add RESYNC-LOG task ret code:" << _ret_code << ",logID:" << _shp_task->m_last_sync_point << ",remote peer:" << _iter->first;
        }
    }

    /*Note:Waiting for the log replication task to finish is a time consuming operation, we'd better entrust that
      to a dedicated thread, returning to client immediately. It's the client's(usually the administrator)
      duty to check when the membership change job will be done.  */
    std::thread _th_member_changing(MemberMgr::Routine);
    _th_member_changing.detach();

    return nullptr;
}

void MemberMgr::WaitForSyncDataDone() noexcept {
    std::size_t _required_synced_size  = m_joint_topo_snapshot.m_added_nodes.size();

    std::unique_lock<std::mutex> _lock(m_resync_data_cv_mutex);

    auto _wait_cond = [&]()->bool{
        std::size_t _counter = 0;
        //Calculate #followers that are fully synced.
        {
            ReadLock _r_lock(m_mutex);
            for (auto iter = m_joint_topo_snapshot.m_added_nodes.cbegin(); iter != m_joint_topo_snapshot.m_added_nodes.cend(); ++iter)
                if (iter->second->m_status == FollowerStatus::NORMAL) {
                    _counter++;
                    LOG(INFO) << "[Membership Change] new node " << iter->second->my_addr << ",finished sync data";
                }
        }

        /*Need to wait until all new nodes get synchronized,reason for this is that there will be no change for
         nodes who lag behind to catch up in the future , in the current implementation.  */
        return _counter >= _required_synced_size ;
    };

    auto _wait_sec = std::chrono::seconds(::RaftCore::Config::FLAGS_memchg_sync_data_wait_seconds);
    while (!m_resync_data_cv.wait_for(_lock, _wait_sec, _wait_cond))
        LOG(WARNING) << "[Membership Change] syncing data is not finished yet,continue waiting...";

    LOG(INFO) << "[Membership Change] finish to sync data & logs to all the new added nodes.";

}

void MemberMgr::Routine() noexcept {

    LOG(INFO) << "[Membership Change] Routine started, waiting for the majority of new cluster get synced.";

    WaitForSyncDataDone();

    uint32_t _next_verion = 1;
    {
        ReadLock _r_lock(m_mutex);
        _next_verion += m_joint_summary.m_version;
    }

    auto& _ctx_phaseI = m_memchg_ctx.m_phaseI_state;
    auto &_joint_new_cluster = m_joint_topo_snapshot.m_new_cluster;

    {
        ReadLock _r_lock(LeaderView::m_hash_followers_mutex);
        for (auto &_pair_kv : LeaderView::m_hash_followers) {
            m_phaseI_task.m_todo.emplace_back(_pair_kv.second);
            bool _in_new = _joint_new_cluster.find(_pair_kv.first) != _joint_new_cluster.cend();

            uint32_t _flag = uint32_t(JointConsensusMask::IN_OLD_CLUSTER);
            if (_joint_new_cluster.find(_pair_kv.first) != _joint_new_cluster.cend())
                _flag |= uint32_t(JointConsensusMask::IN_NEW_CLUSTER);

            m_phaseI_task.m_flags.emplace_back(_flag);
        }

        _ctx_phaseI.m_cur_cluster.m_cq_entrust_num = (int)LeaderView::m_hash_followers.size();
        m_memchg_ctx.m_cluster_size = LeaderView::m_hash_followers.size() + 1;
        m_memchg_ctx.m_cluster_majority = m_memchg_ctx.m_cluster_size / 2 + 1;
    }

    auto &_joint_added_nodes = m_joint_topo_snapshot.m_added_nodes;
    for (auto _iter = _joint_added_nodes.begin(); _iter != _joint_added_nodes.end(); ++_iter) {
        m_phaseI_task.m_todo.emplace_back(_iter->second);
        m_phaseI_task.m_flags.emplace_back(uint32_t(JointConsensusMask::IN_NEW_CLUSTER));
    }

    _ctx_phaseI.m_new_cluster.m_cq_entrust_num = (int)_joint_new_cluster.size();
    if (!m_joint_topo_snapshot.m_leader_gone_away)
        _ctx_phaseI.m_new_cluster.m_cq_entrust_num--;

    m_memchg_ctx.m_new_cluster_size = _joint_new_cluster.size();
    m_memchg_ctx.m_new_cluster_majority = m_memchg_ctx.m_new_cluster_size / 2 + 1;

    //Requests in the two phase rpc are the same.
    auto cur_term = ElectionMgr::m_cur_term.load();
    TypePtrMemberChangReq _shp_req(new MemberChangeInnerRequest());
    _shp_req->mutable_base()->set_addr(StateMgr::GetMyAddr());
    _shp_req->mutable_base()->set_term(cur_term);
    _shp_req->set_version(_next_verion);

    for (const auto& _node : _joint_new_cluster)
        _shp_req->add_node_list(_node);

    CHECK(PropagateMemberChange(_shp_req, PhaseID::PhaseI)) << "[Membership Change] prepare phase fail "
        << "of membership changing,cannot revert,check this.";

    //m_joint_topology is moved after this call.
    SwitchToJointConsensus(m_joint_topo_snapshot);

    LOG(INFO) << "switching to joint consensus done. going to do phaseII.";

#ifdef _MEMBER_MANAGEMENT_TEST_
    PendingExecution();
#endif

    CHECK(PropagateMemberChange(_shp_req, PhaseID::PhaseII)) << "[Membership Change] commit phase fail "
        << "of membership changing,cannot revert,check this.";

    bool _still_in_new_cluster =  SwitchToStable();

    LOG(INFO) << "phaseII and switching to stable done.";

    /*After successfully changed the membership from the C-old to C-new , there is still one more thing to do :
      If the current leader , aka this node, is not belonging to the C-new cluster,it need to be stepped down to follower according to the RAFT paper,
      but in this implementation ,we just shut it down which is also correct, but quite simple and directly.  */
    if (!_still_in_new_cluster) {
        LOG(INFO) << "I'm no longer in the new cluster , shutdown myself ,goodbye and have a good time.";
        GlobalEnv::ShutDown();
    }

    bool _in_processing = true;
    CHECK(m_in_processing.compare_exchange_strong(_in_processing, false)) << "[MembershipChange] cannot switch in processing status back to true,check this." ;
}

bool MemberMgr::PropagateMemberChange(TypePtrMemberChangReq &shp_req, PhaseID phase_id) noexcept {

    TypePtrMemberChangReq* _p_newbie_req = &shp_req;

    auto *_p_phase_task = &m_phaseI_task;
    if (phase_id == PhaseID::PhaseII) {
        _p_phase_task = &m_phaseII_task;
        MemberChangeInnerRequest *_newbie_req = new MemberChangeInnerRequest(*shp_req);
        _newbie_req->set_flag(::raft::MembershipFlag::NEWBIE);
        _p_newbie_req = new TypePtrMemberChangReq(_newbie_req);
    }

    std::vector<TypePtrFollowerEntity>  &_todo_set = _p_phase_task->m_todo;
    std::vector<uint32_t>  &_flags = _p_phase_task->m_flags;

    auto _req_setter = [&](std::shared_ptr<::raft::MemberChangeInnerRequest>& _target,
        bool newbie = false)->void { _target = newbie ? *_p_newbie_req : shp_req; };

    std::shared_ptr<::grpc::CompletionQueue> _shp_cq(new ::grpc::CompletionQueue());

    auto _entrust_prepare_client = [&](auto &_shp_channel,auto &shp_follower, std::size_t idx){
        auto _shp_client = new MemberChangePrepareAsyncClient(_shp_channel, _shp_cq);
        auto _f_prepare =  std::bind(&::raft::RaftService::Stub::PrepareAsyncMemberChangePrepare,
                                    _shp_client->GetStub().get(), std::placeholders::_1,
                                    std::placeholders::_2, std::placeholders::_3);
        auto _bind_setter = std::bind(_req_setter, std::placeholders::_1, false);

        _shp_client->EntrustRequest(_bind_setter, _f_prepare, ::RaftCore::Config::FLAGS_memchg_rpc_timeo_ms);
        _shp_client->PushCallBackArgs(shp_follower.get());
        _shp_client->PushCallBackArgs(reinterpret_cast<void*>(idx));
    };

    auto _entrust_commit_client = [&](auto &_shp_channel, auto &shp_follower, std::size_t idx) {
        auto _shp_client = new MemberChangeCommitAsyncClient(_shp_channel, _shp_cq);

        auto &_added_nodes = m_joint_topo_snapshot.m_added_nodes;
        bool _im_new_node = m_joint_topo_snapshot.m_leader_gone_away;
        _im_new_node &= _added_nodes.find(shp_follower->my_addr) != _added_nodes.cend();

        auto _f_prepare =  std::bind(&::raft::RaftService::Stub::PrepareAsyncMemberChangeCommit,
                                    _shp_client->GetStub().get(), std::placeholders::_1,
                                    std::placeholders::_2, std::placeholders::_3);

        auto _bind_setter = std::bind(_req_setter, std::placeholders::_1, _im_new_node);

        _shp_client->EntrustRequest(_bind_setter, _f_prepare, ::RaftCore::Config::FLAGS_memchg_rpc_timeo_ms);
        _shp_client->PushCallBackArgs(shp_follower.get());
        _shp_client->PushCallBackArgs(reinterpret_cast<void*>(idx));
    };

    int _entrust_total_num = 0;
    for (std::size_t i = 0; i < _todo_set.size(); ++i) {
        auto &_shp_follower = _todo_set[i];

        if (_shp_follower->m_status != FollowerStatus::NORMAL) {
            LOG(WARNING) << "[Membership Change] follower " << _shp_follower->my_addr << " is under "
                << FollowerEntity::MacroToString(_shp_follower->m_status)
                << ",won't propagate member change prepare request to it";
            continue;
        }

        auto _shp_channel = _shp_follower->m_shp_channel_pool->GetOneChannel();
        if (phase_id == PhaseID::PhaseI)
            _entrust_prepare_client(_shp_channel, _shp_follower, i);
        else {
            _entrust_commit_client(_shp_channel, _shp_follower, i);
        }

        _entrust_total_num++;
    }

    //Polling for phaseI.
    PollingCQ(_shp_cq,_entrust_total_num);

    if (phase_id == PhaseID::PhaseII) {
        delete _p_newbie_req;
        return m_memchg_ctx.JudgeAllFinished();
    }

    //phase_id == PhaseID::PhaseI
    return m_memchg_ctx.JudgePhaseIDetermined() == FinishStatus::POSITIVE_FINISHED;
}

void MemberMgr::Statistic(const ::grpc::Status &status,
    const ::raft::MemberChangeInnerResponse& rsp, void* ptr_follower, uint32_t joint_flag,
    PhaseID phase_id) noexcept {

    auto* _ptr_follower = (FollowerEntity*)ptr_follower;
    const auto& _addr = _ptr_follower->my_addr;

    auto *_phase_state = &m_memchg_ctx.m_phaseI_state;
    if (phase_id == PhaseID::PhaseII)
        _phase_state = &m_memchg_ctx.m_phaseII_state;

    std::string _phase_str = (phase_id == PhaseID::PhaseI) ? "prepare" : "commit";

    if (!status.ok()) {
       LOG(ERROR) << "[Membership Change]" << _phase_str << "fail,error code:" << status.error_code()
                << ",error msg:" << status.error_message() << ",follower joint consensus flag:"
                << joint_flag << ",remote peer:" << _addr;
       _phase_state->IncreaseExplicitFail(joint_flag);
       return;
    }

    const auto &comm_rsp = rsp.comm_rsp();
    auto _error_code = comm_rsp.result();
    if (_error_code!=ErrorCode::SUCCESS) {
        LOG(INFO) << "[Membership Change] peer " << _addr << " " << _phase_str
                  << " fail,follower joint consensus flag:" << joint_flag << ",remote peer:" << _addr;
       _phase_state->IncreaseExplicitFail(joint_flag);
        return;
    }

    _phase_state->IncreaseSuccess(joint_flag);

    LOG(INFO) << "[Membership Change] peer " << _addr << " " << _phase_str
              << " successfully,follower joint consensus flag:" << joint_flag << ",remote peer:" << _addr;
}

void MemberMgr::MemberChangePrepareCallBack(const ::grpc::Status &status,
    const ::raft::MemberChangeInnerResponse& rsp, void* ptr_follower, uint32_t idx) noexcept {

    Statistic(status, rsp, ptr_follower, m_phaseI_task.m_flags[idx], PhaseID::PhaseI);

    for (std::size_t i = 0; i < m_phaseI_task.m_todo.size(); ++i) {
        auto &_shp_follower = m_phaseI_task.m_todo[i];
        if (_shp_follower->my_addr == ((FollowerEntity*)ptr_follower)->my_addr) {
            m_phaseII_task.m_todo.emplace_back(_shp_follower);

            uint32_t _node_flag = m_phaseI_task.m_flags[i];
            m_phaseII_task.m_flags.emplace_back(_node_flag);
            m_memchg_ctx.m_phaseII_state.IncreaseEntrust(_node_flag);
        }
    }
}

void MemberMgr::MemberChangeCommitCallBack(const ::grpc::Status &status,
    const ::raft::MemberChangeInnerResponse& rsp, void* ptr_follower, uint32_t idx)noexcept {

    Statistic(status, rsp, ptr_follower, m_phaseII_task.m_flags[idx], PhaseID::PhaseII);

    if (m_memchg_ctx.JudgeAllFinished())
        LOG(INFO) << "[Membership Change] done";
}

void MemberMgr::PollingCQ(std::shared_ptr<::grpc::CompletionQueue> shp_cq,int entrust_num)noexcept {
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

#ifdef _MEMBER_MANAGEMENT_TEST_
void MemberMgr::PendingExecution()noexcept {
    while(!m_execution_flag)
        std::this_thread::sleep_for(std::chrono::seconds(1));
    m_execution_flag = false;
}

void MemberMgr::ContinueExecution()noexcept {
    m_execution_flag = true;
}
#endif

}

