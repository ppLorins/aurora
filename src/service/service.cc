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

#include <functional>

#include "common/log_identifier.h"
#include "common/error_code.h"
#include "state/state_mgr.h"
#include "global/global_env.h"
#include "follower/follower_view.h"
#include "follower/follower_request.h"
#include "follower/memory_log_follower.h"
#include "binlog/binlog_singleton.h"
#include "storage/storage_singleton.h"
#include "leader/follower_entity.h"
#include "leader/memory_log_leader.h"
#include "tools/lock_free_priority_queue.h"
#include "tools/utilities.h"
#include "election/election.h"
#include "member/member_manager.h"
#include "service/service.h"
#include "client/client_impl.h"

namespace RaftCore::Service {

using grpc::CompletionQueue;
using ::raft::Entity;
using ::raft::ErrorCode;
using ::RaftCore::State::RaftRole;
using ::RaftCore::State::StateMgr;
using ::RaftCore::Common::CommonView;
using ::RaftCore::Common::ReadLock;
using ::RaftCore::Common::WriteLock;
using ::RaftCore::Common::LogIdentifier;
using ::RaftCore::Common::TypeEntityList;
using ::RaftCore::Follower::MemoryLogItemFollower;
using ::RaftCore::Follower::CmpMemoryLogFollower;
using ::RaftCore::Follower::FollowerView;
using ::RaftCore::Follower::TypeMemlogFollowerList;
using ::RaftCore::BinLog::BinLogGlobal;
using ::RaftCore::BinLog::BinLogOperator;
using ::RaftCore::Leader::CmpMemoryLogLeader;
using ::RaftCore::Leader::LeaderView;
using ::RaftCore::Leader::FollowerEntity;
using ::RaftCore::Leader::TypePtrFollowerEntity;
using ::RaftCore::Leader::FollowerStatus;
using ::RaftCore::Leader::BackGroundTask::ReSyncLogContext;
using ::RaftCore::DataStructure::UnorderedSingleListNode;
using ::RaftCore::DataStructure::DoubleListNode;
using ::RaftCore::DataStructure::LockFreePriotityQueue;
using ::RaftCore::DataStructure::DoubleListNode;
using ::RaftCore::Election::ElectionMgr;
using ::RaftCore::Member::MemberMgr;
using ::RaftCore::Member::EJointStatus;
using ::RaftCore::Member::JointConsensusMask;
using ::RaftCore::Tools::TypeSysTimePoint;
using ::RaftCore::Global::GlobalEnv;
using ::RaftCore::Client::AppendEntriesAsyncClient;
using ::RaftCore::Storage::StorageGlobal;

const char*  RPCBase::m_status_macro_names[] = {"NORMAL","HALTED","SHUTTING_DOWN"};

RPCBase::RPCBase() {}

RPCBase::~RPCBase() {}

bool RPCBase::LeaderCheckVailidity( ::raft::ClientCommonResponse* response) noexcept {

    response->set_result(ErrorCode::SUCCESS);

    auto _current_role = StateMgr::GetRole();
    if ( _current_role != RaftRole::LEADER) {

        response->set_result(ErrorCode::FAIL);
        if (_current_role == RaftRole::CANDIDATE) {
            response->set_err_msg("I'm not a leader ,tell you the right leader.");
            return false;
        }

        ::RaftCore::Topology    _topo;
        ::RaftCore::CTopologyMgr::Read(&_topo);

        //I'm a follower
        response->set_err_msg("I'm not a leader ,tell you the right leader.");
        response->set_redirect_to(_topo.m_leader);
        return false;
    }

    auto _status = LeaderView::m_status;
    if (_status != LeaderView::ServerStatus::NORMAL) {
        response->set_result(ErrorCode::FAIL);
        response->set_err_msg(std::string("I'm in a status of:") + this->MacroToString(_status));
        return false;
    }

    return true;
}

std::string RPCBase::FollowerCheckValidity(const ::raft::RequestBase &req_base, TypeTimePoint* p_tp, LogIdentifier *p_cur_id) noexcept {

    //Check current node status
    auto _current_role = StateMgr::GetRole();
    if ( _current_role != RaftRole::FOLLOWER)
        return "I'm not a follower, I'm a:" + std::string(StateMgr::GetRoleStr());

    //if (p_tp != nullptr)
    //    ::RaftCore::Tools::EndTiming(*p_tp, "start processing debugpos1.2", p_cur_id);

    //Check leader address validity
    ::RaftCore::Topology    _topo;
    ::RaftCore::CTopologyMgr::Read(&_topo);

    //if (p_tp != nullptr)
    //    ::RaftCore::Tools::EndTiming(*p_tp, "start processing debugpos1.3", p_cur_id);

    if (_topo.m_leader != req_base.addr())
        return "Sorry,my leader is[" + _topo.m_leader + "],not you" + "[" + req_base.addr() +"]";

    //Check leader term validity
    if (req_base.term() < ElectionMgr::m_cur_term.load())
        return "your term " + std::to_string(req_base.term()) + " is smaller than mine:" + std::to_string(ElectionMgr::m_cur_term.load());

    if (req_base.term() > ElectionMgr::m_cur_term.load())
        return "your term " + std::to_string(req_base.term()) + " is greater than mine:" + std::to_string(ElectionMgr::m_cur_term.load())
              + ",waiting for you heartbeat msg only by which I could upgrade my term.";

    //if (p_tp != nullptr)
    //    ::RaftCore::Tools::EndTiming(*p_tp, "start processing debugpos1.4", p_cur_id);

    return "";
}

bool RPCBase::ValidClusterNode(const std::string &peer_addr) noexcept {

    ::RaftCore::Topology    _topo;
    ::RaftCore::CTopologyMgr::Read(&_topo);
    if (_topo.InCurrentCluster(peer_addr))
        return true;

    ReadLock _r_lock(MemberMgr::m_mutex);
    if (MemberMgr::m_joint_summary.m_joint_status != EJointStatus::JOINT_CONSENSUS)
        return false;

    const auto &_new_cluster = MemberMgr::m_joint_summary.m_joint_topology.m_new_cluster;

    return _new_cluster.find(peer_addr)!=_new_cluster.cend();
}

Write::Write(std::shared_ptr<RaftService::AsyncService> shp_svc,
    std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
    std::shared_ptr<ServerCompletionQueue> &shp_call_cq) noexcept {

    /*Set parent delegator's managed ownership, need to ahead of the following 'Initialize' since otherwise
      this object will serving request promptly yet not ready for that.  */
    this->ResetOwnership(this);

    this->m_phaseI_determined_point.store(false);
    this->m_phaseII_ready_list.store(nullptr);

#ifdef _SVC_WRITE_TEST_
    this->m_epoch = std::chrono::system_clock::from_time_t(std::mktime(&m_start_tm));
#endif

    this->Initialize(shp_svc, shp_notify_cq, shp_call_cq);
    this->m_async_service->RequestWrite(&this->m_server_context, &this->m_request, &this->m_responder,
                                        this->m_server_call_cq.get(), this->m_server_notify_cq.get(), this);
}

Write::~Write() {}

void Write::FinishRequest(WriteProcessStage state) noexcept {
    this->m_write_stage = state;
    this->m_responder.Finish(this->m_response, ::grpc::Status::OK, this);
}

void Write::React(bool cq_result) noexcept {

    if (!cq_result) {
        /*Only when m_shp_req_ctx containing something, it's worthy to log, otherwise it's from the
            default pool's request.*/
        if (this->m_shp_req_ctx)
            LOG(ERROR) << "Server WriteRequest got false result from CQ, log:"
                        << this->m_shp_req_ctx->m_cur_log_id;
        this->ReleaseOwnership();
        return;
    }

    bool _result = true;
    switch (this->m_write_stage) {
    case WriteProcessStage::CREATE:

        new Write(this->m_async_service, this->m_server_notify_cq,this->m_server_call_cq);

        _result = this->BeforeReplicate();
        if (!_result)
            this->FinishRequest(WriteProcessStage::ABOURTED);

        break;

    case WriteProcessStage::FRONT_FINISH:
        this->ReleaseOwnership();
        break;

    case WriteProcessStage::ABOURTED:
        this->ReleaseOwnership();
        break;

    default:
        CHECK(false) << "Unexpected tag " << int(this->m_write_stage);
        break;
    }
}

::grpc::Status Write::Process() noexcept {
    return ::grpc::Status::OK;
}

bool Write::PrepareReplicationStatistic(std::list<std::shared_ptr<AppendEntriesAsyncClient>> &entrust_list) noexcept {

    int _entrusted_client_num = 0;
    auto &_phaseI_state  = this->m_shp_req_ctx->m_phaseI_state;

    uint32_t _total_us = 0;

    auto _prepare_statistic = [&](TypePtrFollowerEntity& shp_follower) {
        if (shp_follower->m_status != FollowerStatus::NORMAL) {
            LOG(WARNING) << "follower " << shp_follower->my_addr << " is under "
                << FollowerEntity::MacroToString(shp_follower->m_status)
                << ",won't appending entries to it";
            return;
        }

        void* _p_pool = nullptr;
        auto _shp_client = shp_follower->FetchAppendClient(_p_pool);

        VLOG(90) << "AppendEntriesAsyncClient fetched:" << shp_follower->my_addr;

        CHECK(_shp_client) << "no available AppendEntries clients, may need a bigger pool.";

        /*The self-delegated ownership will be existing at the mean time, we can just copy it from the
          delegator.  */
        _shp_client->OwnershipDelegator<Write>::CopyOwnership(this->GetOwnership());
        _shp_client->PushCallBackArgs(_p_pool);
        entrust_list.emplace_back(_shp_client);

        _entrusted_client_num++;
        _phaseI_state.IncreaseEntrust(shp_follower->m_joint_consensus_flag);
    };

    //Prepare the commit request ahead of time.
    this->m_shp_commit_req.reset(new ::raft::CommitEntryRequest());
    this->m_shp_commit_req->mutable_base()->set_addr(StateMgr::GetMyAddr());
    this->m_shp_commit_req->mutable_base()->set_term(this->m_shp_req_ctx->m_cur_log_id.m_term);

    auto _p_entity_id = this->m_shp_commit_req->mutable_entity_id();
    _p_entity_id->set_term(this->m_shp_req_ctx->m_cur_log_id.m_term);
    _p_entity_id->set_idx(this->m_shp_req_ctx->m_cur_log_id.m_index);

    std::size_t follower_num = 0;
    {
        ReadLock _r_lock(LeaderView::m_hash_followers_mutex);
        for (auto &_pair_kv : LeaderView::m_hash_followers)
            _prepare_statistic(_pair_kv.second);
        follower_num = LeaderView::m_hash_followers.size();
    }
    this->m_shp_req_ctx->m_cluster_size = follower_num + 1;
    this->m_shp_req_ctx->m_cluster_majority = (follower_num + 1) / 2 + 1; // +1 means including the leader.
    if ((std::size_t)_entrusted_client_num < this->m_shp_req_ctx->m_cluster_majority) {
        LOG(ERROR) << "can't get majority client entrusted for the stable cluster,log:" << this->m_shp_req_ctx->m_cur_log_id;
        return false;
    }

    _entrusted_client_num = 0;

    uint32_t _leader_joint_consensus_flag = (uint32_t)JointConsensusMask::IN_OLD_CLUSTER;

    std::size_t _new_cluster_node_num = 0;
    do{
        ReadLock _r_lock(MemberMgr::m_mutex);
        if (MemberMgr::m_joint_summary.m_joint_status != EJointStatus::JOINT_CONSENSUS)
            break;

        for (auto &_pair_kv : MemberMgr::m_joint_summary.m_joint_topology.m_added_nodes)
            _prepare_statistic(_pair_kv.second);

        if (!MemberMgr::m_joint_summary.m_joint_topology.m_leader_gone_away)
            _leader_joint_consensus_flag |= (uint32_t)JointConsensusMask::IN_NEW_CLUSTER;

        _new_cluster_node_num = MemberMgr::m_joint_summary.m_joint_topology.m_new_cluster.size();
        *((MemberMgr::JointSummary*)this->m_shp_req_ctx->m_p_joint_snapshot) = MemberMgr::m_joint_summary;
    } while (false);
    this->m_shp_req_ctx->m_new_cluster_size = _new_cluster_node_num;
    this->m_shp_req_ctx->m_new_cluster_majority = (_new_cluster_node_num > 0) ? (_new_cluster_node_num / 2 + 1) : 0;

    if ((std::size_t)_entrusted_client_num < this->m_shp_req_ctx->m_new_cluster_majority) {
        LOG(ERROR) << "can't get majority client entrusted for the joint cluster,log:" << this->m_shp_req_ctx->m_cur_log_id;
        return false;
    }

    //Count the leader to the majority.
    _phaseI_state.IncreaseSuccess(_leader_joint_consensus_flag);

    return true;
}

bool Write::PrepareReplicationContext(uint32_t cur_term, uint32_t pre_term) noexcept {

    std::shared_ptr<::raft::AppendEntriesRequest> _shp_req(new ::raft::AppendEntriesRequest());
    _shp_req->mutable_base()->set_addr(StateMgr::GetMyAddr());
    _shp_req->mutable_base()->set_term(cur_term);

    auto _p_entry = _shp_req->add_replicate_entity();
    auto _p_entity_id = _p_entry->mutable_entity_id();
    _p_entity_id->set_term(cur_term);
    _p_entity_id->set_idx(this->m_guid_pair.m_cur_guid);

    auto _p_pre_entity_id = _p_entry->mutable_pre_log_id();
    _p_pre_entity_id->set_term(pre_term);
    _p_pre_entity_id->set_idx(this->m_guid_pair.m_pre_guid);

    auto _p_wop = _p_entry->mutable_write_op();

#ifdef _SVC_WRITE_TEST_
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - this->m_epoch);
    _shp_req->set_debug_info(std::to_string(us.count()));
#endif

    //Memory copy overhead happened here, no way to avoid,background threads and phaseII need this too.
    //TODO: memory copy overhead can be optimized out since there is no 'background threads' in the async mode.
    _p_wop->set_key(this->m_client_request->req().key());
    _p_wop->set_value(this->m_client_request->req().value());

    this->m_shp_req_ctx.reset(new LogReplicationContext());
    this->m_shp_req_ctx->m_cur_log_id.m_term  = cur_term;
    this->m_shp_req_ctx->m_cur_log_id.m_index = this->m_guid_pair.m_cur_guid;

    auto &_phaseI_state = this->m_shp_req_ctx->m_phaseI_state;

    auto _req_setter = [&_shp_req](std::shared_ptr<::raft::AppendEntriesRequest>& _target)->void {
        _target = _shp_req;
    };

    //Require get current replication context prepared  before entrusting any of the request.
    std::list<std::shared_ptr<AppendEntriesAsyncClient>> _entrust_list;
    if (!this->PrepareReplicationStatistic(_entrust_list)) {
        LOG(ERROR) << "can't get majority client entrusted,log:" << this->m_shp_req_ctx->m_cur_log_id;
        return false;
    }

    for (auto &_shp_client : _entrust_list) {
        auto _f_prepare =  std::bind(&::raft::RaftService::Stub::PrepareAsyncAppendEntries,
                                    _shp_client->GetStub().get(), std::placeholders::_1,
                                    std::placeholders::_2, std::placeholders::_3);
        _shp_client->EntrustRequest(_req_setter, _f_prepare, ::RaftCore::Config::FLAGS_leader_append_entries_rpc_timeo_ms);
    }

    return true;
}

bool Write::BeforeReplicate() noexcept {

    this->m_tp_start = ::RaftCore::Tools::StartTimeing();

    this->m_rsp = this->m_response.mutable_client_comm_rsp();

    if (!this->LeaderCheckVailidity(this->m_rsp))
        return false;

    /* Area X */

    //----------------Step 0: get the unique log entry term and index by guid----------------//
    this->m_guid_pair = GuidGenerator::GenerateGuid();
    auto _cur_term = ElectionMgr::m_cur_term.load();

    VLOG(89) << "Generating GUID done,idx:" << this->m_guid_pair.m_cur_guid;

#ifdef _SVC_WRITE_TEST_
    auto _p_test_wop = this->m_request.mutable_req();
    std::string _idx = std::to_string(this->m_guid_pair.m_cur_guid);

    const uint32_t &_val = this->m_request.timestamp();

    this->m_rsp->set_err_msg(std::to_string(_val));
    _p_test_wop->set_key("test_client_key_" + _idx);

#endif

    this->m_client_request = &this->m_request;

    //----------------Step 1: Add the current request to the pending list----------------//
    this->m_shp_entity.reset(new MemoryLogItemLeader(_cur_term,this->m_guid_pair.m_cur_guid));
    this->m_shp_entity->GetEntity()->set_allocated_write_op(const_cast<::raft::WriteRequest*>(&this->m_client_request->req()));

    auto _p_entity_id = this->m_shp_entity->GetEntity()->mutable_entity_id();
    _p_entity_id->set_term(_cur_term);
    _p_entity_id->set_idx(this->m_guid_pair.m_cur_guid);

    /*If this leader has just been elected out, it's term  will be different from the latest log entry
      in the binlog file, needing to make sure pre_term is correct.*/
    uint32_t  _pre_term = _cur_term;

    //VLOG(89) << "my pre guid:" << this->m_guid_pair.m_pre_guid << ",debut:" << ElectionMgr::m_leader_debut << ",debut LRL:" << ElectionMgr::m_pre_term_lrl;

    this->m_first_of_cur_term = ElectionMgr::m_leader_debut && (ElectionMgr::m_pre_term_lrl.m_index == this->m_guid_pair.m_pre_guid);

    //Current guid is the first released guid under the leader's new term.
    if (this->m_first_of_cur_term)
        _pre_term = ElectionMgr::m_pre_term_lrl.m_term;

    this->m_p_pre_entity_id = this->m_shp_entity->GetEntity()->mutable_pre_log_id();
    this->m_p_pre_entity_id->set_term(_pre_term);
    this->m_p_pre_entity_id->set_idx(this->m_guid_pair.m_pre_guid);

    LeaderView::m_entity_pending_list.Insert(this->m_shp_entity);

    //Test..
    //this->m_shp_entity->GetEntity()->release_write_op();
    //this->FinishRequest(WriteProcessStage::FRONT_FINISH);
    //return true;

    //Note: all get good result(~5w/s tp, ~2ms lt.) before here.

    //----------------Step 2: replicated to the majority of cluster----------------//
    if (!this->PrepareReplicationContext(_cur_term, _pre_term)) {
        LeaderView::m_entity_pending_list.Delete(this->m_shp_entity);
        this->m_rsp->set_result(ErrorCode::FAIL);
        this->m_rsp->set_err_msg("PrepareReplicationContext fail.");
        return false;
    }

    //Note: get a bad result(~2w/s tp, ~15ms lt.) if reach here.

    ::RaftCore::Tools::EndTiming(this->m_tp_start, "finished entrust phaseI clients:", &this->m_shp_req_ctx->m_cur_log_id);

    //Test
    //this->FinishRequest(WriteProcessStage::FRONT_FINISH);

#ifdef _SVC_WRITE_TEST_
    auto _now_us = (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - this->m_epoch)).count();
    uint64_t _lantency_us = (uint64_t)(_now_us - _val);
    VLOG(2) << "server side single req latency(us):" << _lantency_us << ",idx:"
            << this->m_shp_req_ctx->m_cur_log_id;
#endif

    return true;
}

void Write::CommitDoneCallBack(const ::grpc::Status &status, const ::raft::CommitEntryResponse& rsp,
    FollowerEntity* ptr_follower) noexcept {

    VLOG(89) << "CommitDoneCallBack called,log:" << this->m_shp_req_ctx->m_cur_log_id << ",addr:" << ptr_follower->my_addr;

    auto _joint_consensus_state = ptr_follower->m_joint_consensus_flag;
    auto &_phaseII_state = this->m_shp_req_ctx->m_phaseII_state;

    if (!status.ok()) {
       LOG(ERROR) << "CommitEntries:RPC fail,error code:" << status.error_code()
                << ",error msg:" << status.error_message() << ",logID:"
                <<  this->m_shp_req_ctx->m_cur_log_id
                << ",remote peer:" << ptr_follower->my_addr;

        if (status.error_code() == ::grpc::StatusCode::DEADLINE_EXCEEDED)
            _phaseII_state.IncreaseImplicitFail(_joint_consensus_state);
        else
            _phaseII_state.IncreaseExplicitFail(_joint_consensus_state);

        return;
    }

    const ::raft::CommonResponse& comm_rsp = rsp.comm_rsp();
    auto _error_code = comm_rsp.result();
    if (_error_code!=ErrorCode::SUCCESS && _error_code!=ErrorCode::ALREADY_COMMITTED) {
        LOG(ERROR) << "CommitEntries:RPC return fail,error code:" << comm_rsp.result()
                << ",error msg:" << comm_rsp.err_msg() << ",logID" << this->m_shp_req_ctx->m_cur_log_id;
        _phaseII_state.IncreaseExplicitFail(_joint_consensus_state);
        return;
    }

    _phaseII_state.IncreaseSuccess(_joint_consensus_state);
}

const std::shared_ptr<LogReplicationContext>&  Write::GetReqCtx() noexcept {
    return this->m_shp_req_ctx;
}

void Write::ProcessReplicateFailure(const ::raft::CommonResponse& comm_rsp,
    TwoPhaseCommitContext::PhaseState &phaseI_state, FollowerEntity* ptr_follower,
    uint32_t joint_consensus_state) noexcept {

    LOG(ERROR) << "AppendEntries:RPC return fail,detail:" << comm_rsp.DebugString() << ",logID"
                <<  this->m_shp_req_ctx->m_cur_log_id << ",remote peer:" << ptr_follower->my_addr;

    auto _error_code = comm_rsp.result();
    if (_error_code == ErrorCode::FAIL) {
        phaseI_state.IncreaseExplicitFail(joint_consensus_state);
        return;
    }

    if (_error_code == ErrorCode::IMPLICIT_FAIL) {
        phaseI_state.IncreaseImplicitFail(joint_consensus_state);
        return;
    }

    if (_error_code != ErrorCode::APPEND_ENTRY_CONFLICT && _error_code != ErrorCode::WAITING_TIMEOUT
        && _error_code != ErrorCode::OVERSTEP_LCL ) {
        LOG(ERROR) << "unexpected returned value: "  << _error_code << ",logID:"
                   <<  this->m_shp_req_ctx->m_cur_log_id;
        phaseI_state.IncreaseExplicitFail(joint_consensus_state);
        return;
    }

    if (_error_code == ErrorCode::APPEND_ENTRY_CONFLICT || _error_code == ErrorCode::OVERSTEP_LCL)
        phaseI_state.IncreaseExplicitFail(joint_consensus_state);
    else
        phaseI_state.IncreaseImplicitFail(joint_consensus_state);

    LogIdentifier _sync_point = (_error_code == ErrorCode::APPEND_ENTRY_CONFLICT) ? \
        this->m_shp_req_ctx->m_cur_log_id : BinLogGlobal::m_instance.GetLastReplicated();

    this->AddResyncLogTask(ptr_follower, _sync_point);

    return;
}

void Write::AddResyncLogTask(FollowerEntity* ptr_follower, const LogIdentifier &sync_point) noexcept {

    /*Follower status has already been set 2o resync, some other threads must have started
        resyncing-log no need to do more.*/
    if (ptr_follower->m_status == FollowerStatus::RESYNC_LOG) {
        LOG(INFO) << "a RESYNC_LOG task already in progress for follower:" << ptr_follower->my_addr
            << ", no need to generate a new one, just return";
        return;
    }

    // Set follower status
    ptr_follower->m_status = FollowerStatus::RESYNC_LOG;

    // Generate a task
    std::shared_ptr<ReSyncLogContext> _shp_task(new ReSyncLogContext());
    _shp_task->m_last_sync_point = sync_point;

    //Find the follower's shared_ptr and copy the ownership.
    {
        ReadLock _r_lock(LeaderView::m_hash_followers_mutex);

        auto _cmp = [&](const std::pair<std::string, TypePtrFollowerEntity> &_pair) {
            return _pair.first == ptr_follower->my_addr;
        };
        auto _iter = std::find_if(LeaderView::m_hash_followers.cbegin(),
                            LeaderView::m_hash_followers.cend(),_cmp);
        if (_iter != LeaderView::m_hash_followers.cend())
            _shp_task->m_follower = _iter->second;
    }

    if (!_shp_task->m_follower) {
        LOG(ERROR) << "Can't find the corresponding follower in leader's view " << ptr_follower->my_addr
                    << ",remote peer:" << ptr_follower->my_addr;
        return;
    }

    auto _ret_code = LeaderView::m_priority_queue.Push(LockFreePriotityQueue::TaskType::RESYNC_LOG, &_shp_task);
    if (_ret_code != QUEUE_SUCC) {
        LOG(ERROR) << "Add RESYNC-LOG task fail,ret:" << _ret_code << ",logID:" << _shp_task->m_last_sync_point << ",remote peer:" << ptr_follower->my_addr;
        return;
    }

    LOG(ERROR) << "Add RESYNC-LOG succeed,sync point" << _shp_task->m_last_sync_point << ",remote peer:" << ptr_follower->my_addr;
}

void Write::EntrustCommitRequest(FollowerEntity* ptr_follower, AppendEntriesAsyncClient* ptr_client) noexcept {

    //Must update statistic data before really do entrust.
    auto &_phaseII_state = this->m_shp_req_ctx->m_phaseII_state;
    _phaseII_state.IncreaseEntrust(ptr_follower->m_joint_consensus_flag);

    void* _p_pool = nullptr;
    auto _shp_client = ptr_follower->FetchCommitClient(_p_pool);

    VLOG(90) << "CommitEntriesAsyncClient fetched:" << ptr_follower->my_addr << ",log:" << this->m_shp_req_ctx->m_cur_log_id;

    CHECK(_shp_client) << "no available Commit clients, may need a bigger pool.";

    /*The commit-client-delegated ownership will be existing at the mean time, we can just copy it
      from the delegator. */
    auto _shp_write = ptr_client->OwnershipDelegator<Write>::GetOwnership();
    _shp_client->OwnershipDelegator<Write>::CopyOwnership(_shp_write);
    _shp_client->PushCallBackArgs(_p_pool);

    auto _req_setter = [&](std::shared_ptr<::raft::CommitEntryRequest>& _target)->void {
        _target = this->m_shp_commit_req;
    };
    auto _f_prepare =  std::bind(&::raft::RaftService::Stub::PrepareAsyncCommitEntries,
                                _shp_client->GetStub().get(), std::placeholders::_1,
                                std::placeholders::_2, std::placeholders::_3);
    _shp_client->EntrustRequest(_req_setter, _f_prepare,
                                ::RaftCore::Config::FLAGS_leader_commit_entries_rpc_timeo_ms);
};

bool Write::UpdatePhaseIStatistic(const ::grpc::Status &status,
    const ::raft::AppendEntriesResponse& rsp,
    FollowerEntity* ptr_follower) noexcept {

    auto _joint_consensus_state = ptr_follower->m_joint_consensus_flag;
    auto &_phaseI_state = this->m_shp_req_ctx->m_phaseI_state;
    if (!status.ok()) {
        LOG(ERROR) << "AppendEntries:RPC fail,error code:" << status.error_code()
                << ",error msg:" << status.error_message() << ",logID:"
                <<  this->m_shp_req_ctx->m_cur_log_id
                << ",remote peer:" << ptr_follower->my_addr;

        if (status.error_code() == ::grpc::StatusCode::DEADLINE_EXCEEDED) {
            _phaseI_state.IncreaseImplicitFail(_joint_consensus_state);

            LogIdentifier _sync_point = BinLogGlobal::m_instance.GetLastReplicated();
            this->AddResyncLogTask(ptr_follower, _sync_point);
            return false;
        }

        _phaseI_state.IncreaseExplicitFail(_joint_consensus_state);
        return false;
    }

    const ::raft::CommonResponse& comm_rsp = rsp.comm_rsp();
    auto _error_code = comm_rsp.result();
    if (_error_code!=ErrorCode::SUCCESS && _error_code!=ErrorCode::SUCCESS_MERGED) {
        this->ProcessReplicateFailure(comm_rsp, _phaseI_state, ptr_follower, _joint_consensus_state);
        return false;
    }

    //Here succeed.
    _phaseI_state.IncreaseSuccess(_joint_consensus_state);

    if (_error_code == ErrorCode::SUCCESS_MERGED) {
        LOG(INFO) << "This log has been unsuccessfully merged, no need to entrust a commit client, "
                 << "logID:" <<  this->m_shp_req_ctx->m_cur_log_id << ",remote peer:"
                 << ptr_follower->my_addr;
        return false;
    }

    return true;
}

void Write::ReplicateDoneCallBack(const ::grpc::Status &status, const ::raft::AppendEntriesResponse& rsp,
        FollowerEntity* ptr_follower, AppendEntriesAsyncClient* ptr_client) noexcept {

    //Test.
    //return;

    ::RaftCore::Tools::EndTiming(this->m_tp_start, "replication callback comes:", &this->m_shp_req_ctx->m_cur_log_id);

    bool _phaseI_result  = this->UpdatePhaseIStatistic(status,rsp,ptr_follower);
    if (!_phaseI_result)
        LOG(INFO) << "replication callback won't entrust a commit client due to above described"
                  << " reasons, logID:" <<  this->m_shp_req_ctx->m_cur_log_id << ",remote peer:"
                  << ptr_follower->my_addr;

    const auto &_cur_log_id = this->m_shp_req_ctx->m_cur_log_id;
    uint32_t _diff = _cur_log_id.GreaterThan(ptr_follower->m_last_sent_committed.load());
    bool _group_commit_reached = _diff >= ::RaftCore::Config::FLAGS_group_commit_count;

    bool _need_entrust = _phaseI_result && _group_commit_reached;

    auto _push_list_if_necessary = [&]() ->void {

        if (!_need_entrust)
            return;

        if (!ptr_follower->UpdateLastSentCommitted(_cur_log_id))
            return;

        auto *_p_cur_client_head = this->m_phaseII_ready_list.load();
        auto * _p_new_node = new UnorderedSingleListNode<FollowerEntity>(ptr_follower);
        _p_new_node->m_next = _p_cur_client_head;
        while (!this->m_phaseII_ready_list.compare_exchange_strong(_p_cur_client_head, _p_new_node))
            _p_new_node->m_next = _p_cur_client_head;
    };

    //Judge if replication result has been determined.
    FinishStatus _determined_value = this->m_shp_req_ctx->JudgePhaseIDetermined();
    if (_determined_value == FinishStatus::UNFINISHED) {
        //If not determined, just push the current client(if any) to the entrust list.
        _push_list_if_necessary();
        return;
    }

    _need_entrust &= (_determined_value == FinishStatus::POSITIVE_FINISHED);

    bool _determined = false;
    if (!this->m_phaseI_determined_point.compare_exchange_strong(_determined, true)) {
        /*Only determined with a success result, can we do further processing(aka,pushing the client
          to the entrust list) */
        if (_need_entrust)
            if (ptr_follower->UpdateLastSentCommitted(_cur_log_id))
                this->EntrustCommitRequest(ptr_follower, ptr_client);
        return;
    }

    //Only one thread could reach here for a certain log entry.
    _push_list_if_necessary(); //Push current request to list.

    this->AfterDetermined(ptr_client);
}

FinishStatus Write::JudgeReplicationResult() noexcept{

    const auto &_entity_id  = this->m_shp_entity->GetEntity()->entity_id();

    //If majority succeed.
    FinishStatus _ret_val = this->m_shp_req_ctx->JudgePhaseIDetermined();
    if (_ret_val == FinishStatus::POSITIVE_FINISHED) {
        /*No matter what's the reason, just return FAIL to the client ,and don't distinguish the
            IMPLICIT_FAIL case from all the other failure cases. */
        if (LeaderView::m_status == LeaderView::ServerStatus::HALTED) {
            //Waiting in a conservative manner.

            LeaderView::m_last_log_waiting_num.fetch_add(1);

            auto _last_released_guid = this->WaitForLastGuidReleasing();
            if (_entity_id.idx() == _last_released_guid)
                this->LastlogResolve(true, _last_released_guid);
        }

        return _ret_val;
    }

    this->m_rsp->set_result(ErrorCode::FAIL);
    this->m_rsp->set_err_msg("cannot replicate to the majority");

    //Push the [implicit] failed request to the bg queue.
    auto _shp_ctx = std::shared_ptr<CutEmptyContext>(new CutEmptyContext());
    _shp_ctx->m_write_request = this->GetOwnership();
    LeaderView::m_cut_empty_list.Insert(_shp_ctx);

    VLOG(89) << "Write Request failed, pushed it to bg list:" << this->m_shp_req_ctx->m_cur_log_id.m_index;

    /*Note:The failure cases, regardless explicit or implicit, are indicating the advent of errors,
       making it reasonable for the server to stop and take a look at what happened and choose the best
       way to deal with the causes, only after that the server can continue serving the clients.
       Besides ,the 'UpdateServerStatus' function returning false is just okay, because other threads
       with a larger guid_pair may have already set server status to LeaderView::ServerStatus::HALTED.  */
    this->UpdateServerStatus(this->m_guid_pair.m_cur_guid, LeaderView::ServerStatus::HALTED);

    /*There is a time windows during which one thread can still generating guids even the server status
       already been set to HALT(the corresponding code is marked as 'Area X' in the above code). The
       following code aim at waiting it to elapse.*/
    auto _last_released_guid = this->WaitForLastGuidReleasing();

    LOG(ERROR) << "AppendEntries:cannot replicate to the majority of cluster,write fail ,idx:"
        << this->m_shp_req_ctx->m_cur_log_id.m_index
        << ",context details:" << this->m_shp_req_ctx->Dump();

    CHECK(this->m_guid_pair.m_cur_guid <= _last_released_guid) << "guid issue :" << this->m_guid_pair.m_cur_guid << "|" << _last_released_guid;

    //Increasing the waiting num.
    LeaderView::m_last_log_waiting_num.fetch_add(1);

    //Current log id is the LRG server halting on.
    if (this->m_guid_pair.m_cur_guid == _last_released_guid) {
        //latest log update overall info. And there are no potential failures for the latest issued log id.
        this->LastlogResolve(false, _last_released_guid);
    }

    return _ret_val;
}

void Write::ReleasePhaseIIReadyList()noexcept {
    auto *_p_cur_client = this->m_phaseII_ready_list.load();
    while (_p_cur_client != nullptr) {
        auto *_p_tmp = _p_cur_client;
        _p_cur_client = _p_cur_client->m_next;

        //Shouldn't delete the _p_tmp->m_data, it's the ptr_follower, just detach it.
        _p_tmp->m_data = nullptr;

        /*Note: just delete the outer side wrapper(aka the 'UnorderedSingleListNode'), rather than the inner
                data, which will be released by itself in the future.*/
        delete _p_tmp;
    }

    this->m_phaseII_ready_list.store(nullptr);
}

bool Write::AppendBinlog(AppendEntriesAsyncClient* ptr_client) noexcept{

    //Only one thread could reach here for a certain log entry.
    const auto &_log_id = this->m_shp_req_ctx->m_cur_log_id;

    auto _cmp = [&](const MemoryLogItemLeader &one) ->bool{
        return !::RaftCore::Common::EntityIDLarger(one.GetEntity()->entity_id(), _log_id);
    };

    DoubleListNode<MemoryLogItemLeader> *_p_head = LeaderView::m_entity_pending_list.CutHead(_cmp);

    if (_p_head == nullptr) {
        VLOG(89) << "CutHead empty occur, transfer to bg list:" << _log_id;

        //Push unfinished requests to background singlist_ordered_queue.
        auto _shp_ctx = std::shared_ptr<CutEmptyContext>(new CutEmptyContext());
        _shp_ctx->m_write_request = ptr_client->OwnershipDelegator<Write>::GetOwnership();

        LeaderView::m_cut_empty_list.Insert(_shp_ctx);

        return false;
    }

    std::list<std::shared_ptr<Entity>> _input_list;
    auto _push = [&](decltype(_p_head) p_cur)->void {
        _input_list.emplace_back(p_cur->m_val->GetEntity());
    };
    DoubleListNode<MemoryLogItemLeader>::Apply(_p_head, _push);

    //Note: Multiple thread appending could happen
    CHECK(BinLogGlobal::m_instance.AppendEntry(_input_list)) << "AppendEntry to binlog fail,never should this happen,something terribly wrong.";

    ::RaftCore::Tools::EndTiming(this->m_tp_start, "Append binlog done.", &this->m_shp_req_ctx->m_cur_log_id);

    //No shared resource between the waiting threads and the notifying thread(s), no mutex is needed here.
    LeaderView::m_cv.notify_all();

    LeaderView::m_garbage.PushFront(_p_head);

    return true;
}

void Write::AfterDetermined(AppendEntriesAsyncClient* ptr_client) noexcept {

    ::RaftCore::Tools::EndTiming(this->m_tp_start, "log entry is determined now.", &this->m_shp_req_ctx->m_cur_log_id);

    //Only one thread could reach here for a certain log entry.
    FinishStatus _ret_val = this->JudgeReplicationResult();
    CHECK(_ret_val != FinishStatus::UNFINISHED) << "got an undetermined result in AfterDetermined.";

    if (_ret_val == FinishStatus::NEGATIVE_FINISHED) {
        this->ReleasePhaseIIReadyList();
        return;
    }

    //Now, phaseI succeed, entrust all the pending phaseII request.
    auto *_p_cur_client = this->m_phaseII_ready_list.load();
    while (_p_cur_client != nullptr) {
        auto *_p_tmp = _p_cur_client;
        _p_cur_client = _p_cur_client->m_next;
        this->EntrustCommitRequest(_p_tmp->m_data, ptr_client);

        /*Note: just delete the outer side wrapper(aka the 'UnorderedSingleListNode'), rather than the inner
                data, which will be released by itself in the future.*/
        _p_tmp->m_data = nullptr;
        delete _p_tmp;
    }

    ::RaftCore::Tools::EndTiming(this->m_tp_start, "entrust all phaseII [necessary] clients done.", &this->m_shp_req_ctx->m_cur_log_id);

    //----------------Step 3: append to local binlog----------------//
    if (!this->AppendBinlog(ptr_client))
        return;

    this->AfterAppendBinlog();
}

void Write::AfterAppendBinlog() noexcept {

    this->m_shp_entity->GetEntity()->release_write_op();

    if (this->m_rsp->result() != ErrorCode::SUCCESS) {
        this->FinishRequest(WriteProcessStage::ABOURTED);
        return;
    }

    if (this->m_first_of_cur_term)
        ElectionMgr::m_leader_debut = false;

    //----------------Step 4: update local storage----------------//
    //Note: Out of order setting could happen , but is acceptable for blind writing operations.

    const auto &_log_id = this->m_shp_req_ctx->m_cur_log_id;

    WriteProcessStage _final_status = WriteProcessStage::FRONT_FINISH;

    if (!StorageGlobal::m_instance.Set(_log_id, this->m_client_request->req().key(), this->m_client_request->req().value())) {
        LOG(ERROR) << "Write to storage fail,logID:" << _log_id;
        this->m_rsp->set_result(ErrorCode::FAIL);
        this->m_rsp->set_err_msg("Log replicated succeed , but cannot write to storage.");
        WriteProcessStage _final_status = WriteProcessStage::ABOURTED;
    }
    else
        ::RaftCore::Tools::EndTiming(this->m_tp_start, "Update storage done.", &this->m_shp_req_ctx->m_cur_log_id);

    this->FinishRequest(_final_status);
}

void Write::CutEmptyRoutine() noexcept {

    LOG(INFO) << "leader CutEmpty msg processor thread started.";

    while (true) {

        if (!CommonView::m_running_flag)
            return;

        auto _wait_cond = [&]()->bool { return !LeaderView::m_cut_empty_list.Empty(); };

        auto _wait_timeo_us = std::chrono::microseconds(::RaftCore::Config::FLAGS_iterating_wait_timeo_us);
        std::unique_lock<std::mutex>  _unique_wrapper(LeaderView::m_cv_mutex);
        bool _waiting_result = LeaderView::m_cv.wait_for(_unique_wrapper, _wait_timeo_us, _wait_cond);

        //There is no shared state among different threads, so it's better to release this lock ASAP.
        _unique_wrapper.unlock();

        if (!_waiting_result)
            continue;

        auto _now = std::chrono::system_clock::now();
        std::shared_ptr<CutEmptyContext> _shp_last_return;

        bool _recheck = false;
        auto _lambda = [&](std::shared_ptr<CutEmptyContext> &one) {
            auto &_p_req = one->m_write_request;

            auto _upper = BinLogGlobal::m_instance.GetLastReplicated();

            if (_p_req->ProcessCutEmptyRequest(_now, _upper, one, _recheck)) {
                _shp_last_return = one;
                return true;
            }

            return false;//No need to go further, stop iterating over the list.
        };

        LeaderView::m_cut_empty_list.Iterate(_lambda);
        if (!_shp_last_return)
            continue;

        auto* _p_head = LeaderView::m_cut_empty_list.CutHeadByValue(*_shp_last_return);
        if (_p_head == nullptr)
            continue;

        /* Double check here for 2 reasons:
           1. For the way of TrivialLockSingleList' work, to get rid of missing elements haven't finish
              inserting during the first iterating of _lambda.
           2. failed write requests need a recheck, and reset its result to SUCCESS if necessary.  */
        LeaderView::m_cut_empty_list.IterateCutHead(_lambda, _p_head);

        LeaderView::m_cut_empty_garbage.PushFront(_p_head);
    }
}

bool Write::ProcessCutEmptyRequest(const TypeSysTimePoint &tp, const LogIdentifier &current_lrl,
    std::shared_ptr<CutEmptyContext> &one, bool recheck) noexcept {

    if (one->m_processed_flag.load())
        return true;

    const auto &_cur_log_id = this->m_shp_req_ctx->m_cur_log_id;
    if (recheck)
        CHECK(current_lrl >= _cur_log_id);

    //::RaftCore::Tools::EndTiming(this->m_tp_start, "entry process disorder.", &_cur_log_id);

    auto _diff = std::chrono::duration_cast<std::chrono::milliseconds>(tp - one->m_generation_tp);
    if (!one->m_log_flag && _diff.count() >= ::RaftCore::Config::FLAGS_cut_empty_timeos_ms) {

        LOG(ERROR) << "waiting for CutHead append to binlog timeout,cur_log_id:"
            << _cur_log_id << ",lrl:" << current_lrl << ", wait ms:" << ::RaftCore::Config::FLAGS_cut_empty_timeos_ms;

        one->m_log_flag = true;
    }

    //Current request's log hasn't been appended to the binlog file.
    if (current_lrl < _cur_log_id)
        return false;

    //Once the current disorder message has already been processed by other iterating threads.
    bool _processed = false;
    if (!one->m_processed_flag.compare_exchange_strong(_processed, true)) {
        VLOG(89) << "cutEmpty req processing permission has been taken:" << _cur_log_id;
        return true;
    }

    //All requests go through here are successfully processed at end.
    this->m_rsp->set_result(ErrorCode::SUCCESS);

    this->AfterAppendBinlog();

    //::RaftCore::Tools::EndTiming(this->m_tp_start, "process CutEmpty done ,has responded to client.", &_cur_log_id);

    return true;
}

uint32_t Write::GetConservativeTimeoutValue(uint64_t idx,bool last_guid) const noexcept {
    auto _snapshot = BinLogGlobal::m_instance.GetLastReplicated();

    int _minimum_factor = 2;
    if (!last_guid)
        _minimum_factor += 1;
    int _rpc_timeout = ::RaftCore::Config::FLAGS_leader_append_entries_rpc_timeo_ms;
    return uint32_t(((idx - _snapshot.m_index) / 2 + _minimum_factor) * _rpc_timeout);
}

bool Write::UpdateServerStatus(uint64_t guid,LeaderView::ServerStatus status) noexcept {

    //Only bigger guids are allowed to modify the server status.Since what we need is the most updated status.
    {
        ReadLock   _r_lock(this->m_mutex);
        if (guid < this->m_last_trigger_guid)
            return false;
    }

    WriteLock   _w_lock(this->m_mutex);

    this->m_last_trigger_guid = guid;

    auto _old_status = LeaderView::m_status;

    if (_old_status == status)
        return true;

    LeaderView::m_status = status;

    if (status == LeaderView::ServerStatus::HALTED)
        this->m_wait_time_point = std::chrono::steady_clock::now() + std::chrono::microseconds(::RaftCore::Config::FLAGS_cgg_wait_for_last_released_guid_finish_us);

    LOG(INFO) << "update server status from " << this->MacroToString(_old_status) << " to "
        << this->MacroToString(status) << " with the guid of " << guid;

    return true;
}

void Write::LastlogResolve(bool result, uint64_t last_released_guid) noexcept {

    /*This is a relative accurate method for judging if all the logs before the LRG has been
      determined, false negative might occur but it's acceptable.*/

    LogIdentifier _cur_lrl = BinLogGlobal::m_instance.GetLastReplicated();
    uint64_t _gap = last_released_guid - _cur_lrl.m_index;

    do {
        VLOG(89) << "Waiting all logs before LRG resolved, last_released_guid:" << last_released_guid
            << ", cur_lrl:" << _cur_lrl.m_index << ",gap:" << _gap << ", waiting_num:"
            << LeaderView::m_last_log_waiting_num.load() << ", should be quickly resolved.";

    /*Note: _gap could < m_last_log_waiting_num at last whereas they should be equal, since
        _cur_lrl is a relative accurate calculated value.*/
    } while (LeaderView::m_last_log_waiting_num.load() < _gap);

    //To get around the issues(CHECK failed) caused by above deviation, wait an additional time.
    uint32_t _wait_ms = ::RaftCore::Config::FLAGS_leader_last_log_resolve_additional_wait_ms;
    std::this_thread::sleep_for(std::chrono::milliseconds(_wait_ms));

    if (!result) {
        //Get the latest LRL as the new base guid.
        _cur_lrl = BinLogGlobal::m_instance.GetLastReplicated();
        GuidGenerator::SetNextBasePoint(_cur_lrl.m_index);
        LOG(INFO) << "base point of guid has been set to:" << _cur_lrl.m_index;
    }

    /*Once reach here, the false negative requests should have been reset to SUCCESS, and all elements
        in the 'm_cut_empty_list' now are the ones that are true negative fail, what we need to do is
        just send them home, aka return to the client.*/
    auto *_p_remains = LeaderView::m_cut_empty_list.SetEmpty();

    auto _lambda = [&](std::shared_ptr<CutEmptyContext> &one) {
        auto &_p_req = one->m_write_request;

#ifdef _SVC_WRITE_TEST_
        const auto &_entity_id  = _p_req->GetInnerLog()->GetEntity()->entity_id();
        VLOG(89) << "start processing the bg list after last log resolved:" << _entity_id.idx();
#endif

        _p_req->AfterAppendBinlog();
        return true;
    };

    if (_p_remains != nullptr) {
        LeaderView::m_cut_empty_list.IterateCutHead(_lambda, _p_remains);
        LeaderView::m_cut_empty_garbage.PushFront(_p_remains);
    }

    //It's the last log thread's duty to clear all the remaining item in the pending list.
    LeaderView::m_entity_pending_list.Clear();

    LOG(INFO) << "last log resolved with the last release guid:" << last_released_guid;

    LeaderView::m_last_log_waiting_num.store(0);

    CHECK(this->UpdateServerStatus(last_released_guid, LeaderView::ServerStatus::NORMAL)) << "latest log:"
            << last_released_guid <<" update server status to NORMAL fail.";
}

uint64_t Write::WaitForLastGuidReleasing() const noexcept {
    //Business logic guaranteeing that there are no race conditions for 'm_wait_time_point'.
    std::this_thread::sleep_until(this->m_wait_time_point);
    return GuidGenerator::GetLastReleasedGuid();
}

Read::Read(std::shared_ptr<RaftService::AsyncService> shp_svc,
    std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
    std::shared_ptr<ServerCompletionQueue> &shp_call_cq) noexcept {
    this->Initialize(shp_svc, shp_notify_cq, shp_call_cq);
    this->m_async_service->RequestRead(&this->m_server_context, &this->m_request, &this->m_responder,
        this->m_server_call_cq.get(), this->m_server_notify_cq.get(), this);
}

::grpc::Status Read::Process() noexcept {
    auto _p_rsp = this->m_response.mutable_client_comm_rsp();

    if (!this->LeaderCheckVailidity(_p_rsp))
        return ::grpc::Status::OK;

    auto *_p_val = this->m_response.mutable_value();
    if (!StorageGlobal::m_instance.Get(this->m_request.key(), *_p_val))
        LOG(INFO) << "val doesn't exist for key :" << this->m_request.key();

    return ::grpc::Status::OK;
}

MembershipChange::MembershipChange(std::shared_ptr<RaftService::AsyncService> shp_svc,
        std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
        std::shared_ptr<ServerCompletionQueue> &shp_call_cq) noexcept {
    this->Initialize(shp_svc, shp_notify_cq, shp_call_cq);
    this->m_async_service->RequestMembershipChange(&this->m_server_context, &this->m_request,
            &this->m_responder, this->m_server_call_cq.get(), this->m_server_notify_cq.get(), this);
}

::grpc::Status MembershipChange::Process() noexcept {
    //TODO: There should have some authentications here.

    auto *_p_rsp = this->m_response.mutable_client_comm_rsp();
    if (!this->LeaderCheckVailidity(_p_rsp))
        return ::grpc::Status::OK;

    std::set<std::string>   _new_cluster;
    for (int i = 0; i < this->m_request.node_list_size(); ++i)
        _new_cluster.emplace(this->m_request.node_list(i));

    const char* _p_err_msg = MemberMgr::PullTrigger(_new_cluster);
    if (_p_err_msg) {
        LOG(ERROR) << "[Membership Change] pull the trigger fail:" << _p_err_msg;
        _p_rsp->set_result(ErrorCode::FAIL);
        _p_rsp->set_err_msg("pull trigger fail,check the log for details.");
        return ::grpc::Status::OK;
    }

    _p_rsp->set_result(ErrorCode::SUCCESS);
    return ::grpc::Status::OK;
}

AppendEntries::AppendEntries(std::shared_ptr<RaftService::AsyncService> shp_svc,
        std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
        std::shared_ptr<ServerCompletionQueue> &shp_call_cq) noexcept {

    this->ResetOwnership(this);

    this->Initialize(shp_svc, shp_notify_cq, shp_call_cq);

    this->m_async_service->RequestAppendEntries(&this->m_server_context, &this->m_request,
        &this->m_responder, this->m_server_call_cq.get(), this->m_server_notify_cq.get(), this);
}

AppendEntries::~AppendEntries()noexcept {}

std::string AppendEntries::ComposeInputLogs() noexcept {
    //Check validity of the input logs

    MemoryLogItemFollower *_p_previous = nullptr;
    const auto &_replicate_entity = this->m_request.replicate_entity();
    for (auto iter = _replicate_entity.cbegin(); iter != _replicate_entity.cend(); ++iter) {

        //Note: This is where memory copy overhead occurs!
        MemoryLogItemFollower *_p_log_item = new MemoryLogItemFollower(*iter);

        this->m_log_list.emplace_back(_p_log_item);

        //Ensure the log entries are continuous
        if (!_p_previous) {
            _p_previous = _p_log_item;
            continue;
        }

        if (!_p_log_item->AfterOf(*_p_previous)) {
            char sz_err[1024] = { 0 };
            std::snprintf(sz_err,sizeof(sz_err),"inputing logs are not continuous,pre:%d|%llu,cur:%d|%llu",
                        _p_previous->GetEntity()->pre_log_id().term(), _p_previous->GetEntity()->pre_log_id().idx(),
                        _p_previous->GetEntity()->entity_id().term(), _p_previous->GetEntity()->entity_id().idx());
            LOG(ERROR) << sz_err;
            return sz_err;
        }

        _p_previous = _p_log_item;
    }

    return "";
}

void AppendEntries::ProcessOverlappedLog() noexcept {

    static const char* _p_err_msg   = "revert log fail.";
    static const char* _p_step_over = "overstep lcl";

    const char* _p_ret_msg = "";

    /*Note : This can be invoked simultaneously , for correctness and simplicity , only one thread could
      successfully reverted the binlog, others will fail, in which case we will return an explicit fail to the
      client .  */
    ErrorCode _error_code = ErrorCode::SUCCESS;
    const auto &_lcl = StorageGlobal::m_instance.GetLastCommitted();

    auto _revert_code = BinLogGlobal::m_instance.RevertLog(this->m_log_list, _lcl);
    if (_revert_code > BinLogOperator::BinlogErrorCode::SUCCEED_MAX) {
        auto _lrl = BinLogGlobal::m_instance.GetLastReplicated();
        LOG(ERROR) << "log conflict detected,but reverting log fail,current ID-LRL:" << _lrl
                   << ",_pre_entity_id:" << this->m_pre_entity_id->DebugString()
                   << ",retCode:" << int(_revert_code);

        _error_code = ErrorCode::FAIL;
        _p_ret_msg = _p_err_msg;

        if (_revert_code == BinLogOperator::BinlogErrorCode::NO_CONSISTENT_ERROR)
            _error_code = ErrorCode::APPEND_ENTRY_CONFLICT;
        else if (_revert_code == BinLogOperator::BinlogErrorCode::OVER_BOUNDARY) {
            _error_code = ErrorCode::OVERSTEP_LCL;
            _p_ret_msg = _p_step_over;
        }

    } else if (_revert_code == BinLogOperator::BinlogErrorCode::SUCCEED_TRUNCATED) {

        /*Note: this elif section may executing simultaneously, but the follower would quickly get
                resolved by a new RESYNC_LOG command issued by the lead.  */

        static std::mutex _m;
        std::unique_lock<std::mutex> _mutex_lock(_m);

        //In case of successfully reverting log, all the pending lists are also become invalid,need to be cleared.
        FollowerView::m_phaseI_pending_list.DeleteAll(); //Cannot use 'Clear' avoiding conflict with 'Insert' operations.
        FollowerView::m_phaseII_pending_list.Clear();

        /*There maybe remaining items in this->m_log_list those already been appended to the binlog after
          reverting,for phaseII correctly committing ,they need to be inserted to phaseII_pending_list.*/
        std::for_each(this->m_log_list.cbegin(), this->m_log_list.cend(), [&](const auto &_one) { FollowerView::m_phaseII_pending_list.Insert(_one); });

    } else if (_revert_code == BinLogOperator::BinlogErrorCode::SUCCEED_MERGED)
        _error_code = ErrorCode::SUCCESS_MERGED;

    this->m_rsp->set_result(_error_code);
    this->m_rsp->set_err_msg(_p_ret_msg);
}

bool AppendEntries::BeforeJudgeOrder() noexcept {

    this->m_tp_start = ::RaftCore::Tools::StartTimeing();

    this->m_rsp = this->m_response.mutable_comm_rsp();
    this->m_rsp->set_result(ErrorCode::SUCCESS);

    /*
    //Testing...
    const auto &_entity = this->m_request.replicate_entity(this->m_request.replicate_entity_size() - 1);
    uint32_t _idx = _entity.entity_id().idx();

    VLOG(89) << " msg received & idx:" << _idx;
    return true;
    */

    auto _err_msg = this->FollowerCheckValidity(this->m_request.base(), &this->m_tp_start, &this->m_last_log);
    if (!_err_msg.empty()) {
        LOG(ERROR) << "check request validity fail :" << _err_msg;
        this->m_rsp->set_result(ErrorCode::FAIL);
        this->m_rsp->set_err_msg(_err_msg);
        return true;
    }

    _err_msg = this->ComposeInputLogs();
    if (!_err_msg.empty()) {
        LOG(ERROR) << "input log invalid,detail";
        this->m_rsp->set_result(ErrorCode::FAIL);
        this->m_rsp->set_err_msg(_err_msg);
        return true;
    }

    //this->m_log_list need to be sorted.
    auto _cmp = [](const std::shared_ptr<MemoryLogItemFollower>& left, const std::shared_ptr<MemoryLogItemFollower>& right) ->bool {
        return ::RaftCore::Common::EntityIDSmaller(left->GetEntity()->entity_id(),right->GetEntity()->entity_id());
    };
    this->m_log_list.sort(_cmp);

    this->m_pre_entity_id  = &(this->m_log_list.front()->GetEntity()->pre_log_id());
    this->m_last_entity_id = &(this->m_log_list.back()->GetEntity()->entity_id());

    this->m_last_log = ::RaftCore::Common::ConvertID(*this->m_last_entity_id);

    ::RaftCore::Tools::EndTiming(this->m_tp_start, "start processing to :", &this->m_last_log);

    //Check if the first log conflict with the written logs
    auto _lrl = BinLogGlobal::m_instance.GetLastReplicated();
    if (::RaftCore::Common::EntityIDSmaller(*this->m_pre_entity_id,_lrl)) {
        ::RaftCore::Tools::EndTiming(this->m_tp_start, "start process overlap log.", &this->m_last_log);
        this->ProcessOverlappedLog();
        auto _lrl = BinLogGlobal::m_instance.GetLastReplicated();
        ::RaftCore::Tools::EndTiming(this->m_tp_start, "overlap log process done, lrl:", &_lrl);
        return true;
    }

    /*Inserting the log entries to the follower's pending list in a reverse order to get rid of the
       'partially inserted' problem.  */
    std::for_each(this->m_log_list.crbegin(), this->m_log_list.crend(), [&](const auto &_one) { FollowerView::m_phaseI_pending_list.Insert(_one); });

    /* If the minimum ID of the log entries is greater than the ID-LRL, means the current thread need to wait..  */
    _lrl = BinLogGlobal::m_instance.GetLastReplicated();

    VLOG(89) << "debug pos1,pre_id:" << ::RaftCore::Common::ConvertID(*this->m_pre_entity_id)
        << ",snapshot:" << _lrl;

    if (!::RaftCore::Common::EntityIDEqual(*this->m_pre_entity_id, _lrl)) {

        this->m_append_entries_stage = AppendEntriesProcessStage::WAITING;

        //Here need to wait on a CV, push it to background threads.
        auto _shp_ctx = std::shared_ptr<DisorderMessageContext>(new DisorderMessageContext());
        _shp_ctx->m_append_request = this->GetOwnership();

        FollowerView::m_disorder_list.Insert(_shp_ctx);

        ::RaftCore::Tools::EndTiming(this->m_tp_start, "insert a disorder msg.", &this->m_last_log);

        return false;
    }

    this->ProcessAdjacentLog();

    ::RaftCore::Tools::EndTiming(this->m_tp_start, "adjacent log process done: ", &this->m_last_log);

    return true;
}

const LogIdentifier& AppendEntries::GetLastLogID() const noexcept {
    return this->m_last_log;
}

void AppendEntries::DisorderLogRoutine() noexcept {

    LOG(INFO) << "follower disorder msg processor thread started.";

    while (true) {

        if (!CommonView::m_running_flag)
            return;

        auto _wait_cond = [&]()->bool { return !FollowerView::m_disorder_list.Empty(); };

        auto _wait_timeo_us = std::chrono::microseconds(::RaftCore::Config::FLAGS_iterating_wait_timeo_us);
        std::unique_lock<std::mutex>  _unique_wrapper(FollowerView::m_cv_mutex);
        bool _waiting_result = FollowerView::m_cv.wait_for(_unique_wrapper, _wait_timeo_us, _wait_cond);

        //There is no shared state among different threads, so it's better to release this lock ASAP.
        _unique_wrapper.unlock();

        if (!_waiting_result)
            continue;

        auto _now = std::chrono::system_clock::now();
        std::shared_ptr<DisorderMessageContext> _shp_last_return;

        auto _lambda = [&](std::shared_ptr<DisorderMessageContext> &one) {
            auto &_shp_req = one->m_append_request;

            auto _upper = BinLogGlobal::m_instance.GetLastReplicated();

            if (_shp_req->ProcessDisorderLog(_now, _upper, one)) {
                _shp_last_return = one;
                return true;
            }

            return false;//No need to go further, stop iterating over the list.
        };

        FollowerView::m_disorder_list.Iterate(_lambda);
        if (!_shp_last_return)
            continue;

        auto* _p_head = FollowerView::m_disorder_list.CutHeadByValue(*_shp_last_return);
        if (_p_head == nullptr)
            continue;

        //_shp_last_return may become invalid here.

        /*For the way of TrivialLockSingleList' work, to get rid of missing elements haven't finish
          inserting during the first iterating of _lambda */
        FollowerView::m_disorder_list.IterateCutHead(_lambda, _p_head);

        FollowerView::m_disorder_garbage.PushFront(_p_head);
    }
}

bool AppendEntries::ProcessDisorderLog(const TypeSysTimePoint &tp, const LogIdentifier &upper_log,
    std::shared_ptr<DisorderMessageContext> &one) noexcept {

    /*Judge whether the current disorder message has already been processed by the current or
      other iterating threads.*/
    if (one->m_processed_flag.load())
        return true;

    /*There is no need to consider the overlapped logs, because :
      1. elements in the pending list can be over written.
      2. overlapped ones can all get a positive result from the follower, you can do nothing
         to prevent this. */

    //Current request's log hasn't been appended to the binlog file.
    bool _not_reach_me = upper_log < this->m_last_log;
    bool _adjacent = ::RaftCore::Common::EntityIDEqual(*this->m_pre_entity_id, upper_log);

    if (_not_reach_me && !_adjacent)
        return false;

    bool _processed = false;
    if (!one->m_processed_flag.compare_exchange_strong(_processed, true)) {
        VLOG(89) << "disorder req processing permission has been taken:" << this->m_last_log;
        return true;
    }

    if (_adjacent) {
        VLOG(89) << "process adjacent in routine:" << this->m_last_log;
        this->ProcessAdjacentLog();
    }

    auto _diff = std::chrono::duration_cast<std::chrono::milliseconds>(tp - one->m_generation_tp);
    if (_diff.count() >= ::RaftCore::Config::FLAGS_disorder_msg_timeo_ms) {

        /*Here we don't need to delete elements from FollowerView::m_phaseI_pending_list where
          encounter failures with it, just leave it here, and they'll get replaced with the ones.*/

        LOG(ERROR) << "Waiting for cv timeout,upper log:" << upper_log << ", last log:"
            << this->m_last_log << ",pre_log_id:" << ::RaftCore::Common::ConvertID(*this->m_pre_entity_id)
            << ", diff:" << _diff.count() << ", wait ms:" << ::RaftCore::Config::FLAGS_disorder_msg_timeo_ms;

        //Return a WAITING_TIMEOUT error indicating the leader that this follower need to be set to `RESYNC_LOG` status.
        this->m_rsp->set_result(ErrorCode::WAITING_TIMEOUT);
        this->m_rsp->set_err_msg("Waiting for cv timeout.");
    }

    this->m_append_entries_stage = AppendEntriesProcessStage::FINISH;
    this->m_responder.Finish(this->m_response, ::grpc::Status::OK, this);

    ::RaftCore::Tools::EndTiming(this->m_tp_start, "process disorder done ,has responded to client.", &this->m_last_log);

    return true;
}

::grpc::Status AppendEntries::Process() noexcept {
    return ::grpc::Status::OK;
}

void AppendEntries::React(bool cq_result) noexcept {
    if (!cq_result) {
        LOG(ERROR) << "AppendEntries got false result from CQ,last log:" << this->m_last_log;
        this->ReleaseOwnership();
        return;
    }

    switch (this->m_append_entries_stage) {
    case AppendEntriesProcessStage::CREATE:
        /* Spawn a new subclass instance to serve new clients while we process
         the one for this . The instance will deallocate itself as
         part of its FINISH state.*/
        new AppendEntries(this->m_async_service,this->m_server_notify_cq,this->m_server_call_cq);

        if (this->BeforeJudgeOrder()) {
            this->m_append_entries_stage = AppendEntriesProcessStage::FINISH;
            this->m_responder.Finish(this->m_response, ::grpc::Status::OK, this);
        }
        break;

    case AppendEntriesProcessStage::WAITING:
        //do nothing.
        break;

    case AppendEntriesProcessStage::FINISH:
        this->ReleaseOwnership();
        break;

    default:
        CHECK(false) << "Unexpected tag " << int(this->m_append_entries_stage);
        break;
    }
}

void AppendEntries::ProcessAdjacentLog() noexcept {

    DoubleListNode<MemoryLogItemFollower> *_p_head = FollowerView::m_phaseI_pending_list.CutHead(CmpMemoryLogFollower);

    CHECK(_p_head) << "cut head empty";
    if (_p_head->m_atomic_next.load() == nullptr) {
        const auto &_pre_log_id = _p_head->m_val->GetEntity()->pre_log_id();
        auto _lrl = BinLogGlobal::m_instance.GetLastReplicated();
        CHECK(::RaftCore::Common::EntityIDEqual(_pre_log_id,_lrl))
            << "cut head got one element but its pre_log_id != ID-LCL :"
            <<  _pre_log_id.ShortDebugString() << "!=" << _lrl;
    }

    std::list<std::shared_ptr<Entity>> _input_list;
    int _cuthead_size = 0;

    auto _push = [&](decltype(_p_head) p_cur)->void{
        _cuthead_size++;

        _input_list.emplace_back(p_cur->m_val->GetEntity());

        /*Insert will take the ownership of p_cur, so no need to release them later.*/
        FollowerView::m_phaseII_pending_list.Insert(p_cur);
    };

    DoubleListNode<MemoryLogItemFollower>::Apply(_p_head, _push);

    CHECK(BinLogGlobal::m_instance.AppendEntry(_input_list)) << "AppendEntry to binlog fail,never should this happen,something terribly wrong.";

    //notify the background thread that the LRL has updated now.
    FollowerView::m_cv.notify_all();
}

CommitEntries::CommitEntries(std::shared_ptr<RaftService::AsyncService> shp_svc,
    std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
    std::shared_ptr<ServerCompletionQueue> &shp_call_cq) noexcept {
    this->Initialize(shp_svc, shp_notify_cq, shp_call_cq);
    this->m_async_service->RequestCommitEntries(&this->m_server_context, &this->m_request,
        &this->m_responder, this->m_server_call_cq.get(), this->m_server_notify_cq.get(), this);
}

::grpc::Status CommitEntries::Process() noexcept {

    //...Test....
    //this->m_response.mutable_comm_rsp()->set_result(ErrorCode::SUCCESS);
    //return ::grpc::Status::OK;

    auto _req_term = this->m_request.entity_id().term();
    auto _req_idx  = this->m_request.entity_id().idx();

    VLOG(89) << "Enter CommitEntries,term:" << _req_term << ",idx:" << _req_idx;

    auto _p_rsp = this->m_response.mutable_comm_rsp();
    _p_rsp->set_result(ErrorCode::SUCCESS);

    auto _err_msg = this->FollowerCheckValidity(this->m_request.base());
    if (!_err_msg.empty()) {
        _p_rsp->set_result(ErrorCode::FAIL);
        _p_rsp->set_err_msg(_err_msg);
        VLOG(89) << "done CommitEntries,pos0:" << _req_term << ",idx:" << _req_idx;
        return ::grpc::Status::OK;
    }

    LogIdentifier req_log;
    req_log.Set(_req_term, _req_idx);
    if (req_log < StorageGlobal::m_instance.GetLastCommitted()) {
        VLOG(89) << "done CommitEntries,pos1:" << _req_term << ",idx:" << _req_idx;
        _p_rsp->set_result(ErrorCode::ALREADY_COMMITTED);
        return ::grpc::Status::OK;
    }

    auto follower_log_item = MemoryLogItemFollower(_req_term, _req_idx);
    DoubleListNode<MemoryLogItemFollower> *_p_head = FollowerView::m_phaseII_pending_list.CutHeadByValue(follower_log_item);
    if (_p_head == nullptr) {
        //In case of (req_log >= ID-LCL &&  cannot get value<follower_log_item) ,means the requested entry has already been committed.
        _p_rsp->set_result(ErrorCode::ALREADY_COMMITTED);
        _p_rsp->set_err_msg("CutHeadByValue got a nullptr");

        VLOG(89) << "done CommitEntries,pos2:" << _req_term << ",idx:" << _req_idx;
        return ::grpc::Status::OK;
    }

    int _cuthead_size = 0;

    //Updating storage.
    auto _store = [&](decltype(_p_head) p_cur)->void{
        auto _entity = p_cur->m_val->GetEntity();

        _cuthead_size++;

        LogIdentifier _log;
        _log.Set(_entity->entity_id().term(),_entity->entity_id().idx());

        /*Since write_op is inside the pb structure rather than in a 'shared_ptr', we have to do a
          memory copy here.*/
        StorageGlobal::m_instance.Set(_log,_entity->write_op().key(), _entity->write_op().value());
    };

    DoubleListNode<MemoryLogItemFollower>::Apply(_p_head, _store);

    FollowerView::m_garbage.PushFront(_p_head);

    VLOG(89) << "done CommitEntries,term:" << _req_term << ",idx:" << _req_idx << ",cuthead size:" << _cuthead_size;

    return ::grpc::Status::OK;
}

SyncData::SyncData(std::shared_ptr<RaftService::AsyncService> shp_svc,
    std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
    std::shared_ptr<ServerCompletionQueue> &shp_call_cq) noexcept {
    this->Initialize(shp_svc, shp_notify_cq, shp_call_cq);
    this->m_async_service->RequestSyncData(&this->m_server_context, &this->m_reader_writer, this->m_server_call_cq.get(), this->m_server_notify_cq.get(), this);
}

::grpc::Status SyncData::Process() noexcept {
    auto _p_comm_rsp = this->m_response.mutable_comm_rsp();

    auto _err_msg = this->FollowerCheckValidity(this->m_request.base());
    if (!_err_msg.empty()) {
        LOG(ERROR) << "[Sync Data Stream] read an invalid input ,error msg:" << _err_msg;
        _p_comm_rsp->set_result(ErrorCode::FAIL);
        _p_comm_rsp->set_err_msg(_err_msg);
        return ::grpc::Status::OK;
    }

    const ::raft::SyncDataMsgType &_msg_type = this->m_request.msg_type();
    switch (_msg_type) {

        case ::raft::SyncDataMsgType::PREPARE: {
            LOG(INFO) << "[Sync Data Stream]receive PREPARE.";
            FollowerView::Clear();

            StorageGlobal::m_instance.Reset();

            if (!BinLogGlobal::m_instance.Clear())
                LOG(ERROR) <<  "SyncData clear storage data fail:";

            _p_comm_rsp->set_result(ErrorCode::PREPARE_CONFRIMED);
            break;
        }

        case ::raft::SyncDataMsgType::SYNC_DATA:
            LOG(INFO) << "[Sync Data Stream]receive SYNC_DATA, size:" << this->m_request.entity_size();
            for (int i = 0; i < this->m_request.entity_size(); ++i) {
                const ::raft::Entity &_entity = this->m_request.entity(i);
                LogIdentifier _log_id = ::RaftCore::Common::ConvertID(_entity.entity_id());
                if (!StorageGlobal::m_instance.Set(_log_id, _entity.write_op().key(), _entity.write_op().key())) {
                    LOG(ERROR) << "SyncData set storage fail,log id:" << _log_id;
                    break;
                }

                /*Along with storing ,the latest log entry should also be appended to the binlog file for
                further uses.*/
                if (i != this->m_request.entity_size() - 1)
                    continue;

                std::shared_ptr<::raft::Entity> _shp_entity(new ::raft::Entity());
                auto _p_entity_id = _shp_entity->mutable_entity_id();
                _p_entity_id->set_term(_entity.entity_id().term());
                _p_entity_id->set_idx(_entity.entity_id().idx());

                _shp_entity->set_allocated_write_op(const_cast<::raft::WriteRequest*>(&_entity.write_op()));
                auto _set_head_error_code = BinLogGlobal::m_instance.SetHead(_shp_entity);
                _shp_entity->release_write_op();
                if (_set_head_error_code != BinLogOperator::BinlogErrorCode::SUCCEED_TRUNCATED) {
                    LOG(ERROR) << "SyncData SetHead fail,log id:" << _log_id;
                    break; // break for loop.
                }
            }
            _p_comm_rsp->set_result(ErrorCode::SYNC_DATA_CONFRIMED);
            break;

        case ::raft::SyncDataMsgType::SYNC_LOG: {
            LOG(INFO) << "[Sync Data Stream]receive SYNC_LOG, size:" << this->m_request.entity_size();
            TypeEntityList _input_list;
            for (int i = 0; i < this->m_request.entity_size(); ++i) {
                const ::raft::Entity &_entity = this->m_request.entity(i);

                /*Note: 1.must specify the deleter for std::shared_ptr<Entity>,otherwise double-free could happen.
                        2. convert Entity* to const Entity*  is safe demonstrated by other tests.  */
                _input_list.emplace_back(const_cast<Entity*>(&_entity), [](auto p) {});

                //Also need to add log entries to pending list II.
                std::shared_ptr<MemoryLogItemFollower> _shp_follower_log(new MemoryLogItemFollower(_entity));
                DoubleListNode<MemoryLogItemFollower> *_p_node = new DoubleListNode<MemoryLogItemFollower>(_shp_follower_log);
                FollowerView::m_phaseII_pending_list.Insert(_p_node);
            }

            CHECK(BinLogGlobal::m_instance.AppendEntry(_input_list)) << "AppendEntry to binlog fail,never should this happen,something terribly wrong.";
            _p_comm_rsp->set_result(ErrorCode::SYNC_LOG_CONFRIMED);
            break;
        }

        default:
            LOG(ERROR) << "SyncData unknown msgType:" << _msg_type;
            break;
    }

    return ::grpc::Status::OK;
}

MemberChangePrepare::MemberChangePrepare(std::shared_ptr<RaftService::AsyncService> shp_svc,
    std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
    std::shared_ptr<ServerCompletionQueue> &shp_call_cq) noexcept {

    this->Initialize(shp_svc, shp_notify_cq, shp_call_cq);
    this->m_async_service->RequestMemberChangePrepare(&this->m_server_context, &this->m_request, &this->m_responder, this->m_server_call_cq.get(), this->m_server_notify_cq.get(), this);
}

::grpc::Status MemberChangePrepare::Process() noexcept {
    LOG(INFO) << "[Membership Change] MemberChangePrepare starts.";

    auto *_p_rsp = this->m_response.mutable_comm_rsp();

    auto _err_msg = this->FollowerCheckValidity(this->m_request.base());
    if (!_err_msg.empty()) {
        LOG(ERROR) << "[Membership Change] cannot do membership change prepare,error message:" << _err_msg;
        _p_rsp->set_result(ErrorCode::FAIL);
        _p_rsp->set_err_msg(_err_msg);
        return ::grpc::Status::OK;
    }

    std::set<std::string>   _new_cluster;
    for (int i = 0; i < this->m_request.node_list_size(); ++i)
        _new_cluster.emplace(this->m_request.node_list(i));

    MemberMgr::JointTopology _joint_topo;
    _joint_topo.Update(&_new_cluster);

    MemberMgr::SwitchToJointConsensus(_joint_topo,this->m_request.version());

    std::string _removed_nodes="",_added_nodes="";
    {
        ReadLock _r_lock(MemberMgr::m_mutex);
        for (const auto& _node : MemberMgr::m_joint_summary.m_joint_topology.m_added_nodes)
            _added_nodes += (_node.first + "|");

        for (const auto& _node : MemberMgr::m_joint_summary.m_joint_topology.m_removed_nodes)
            _removed_nodes += (_node + "|");
    }

    LOG(INFO) << "[Membership Change] switched to JointConsensus status with new nodes:" << _added_nodes
                << " and removed nodes:" << _removed_nodes;

    _p_rsp->set_result(ErrorCode::SUCCESS);

    LOG(INFO) << "[Membership Change] MemberChangePrepare ends.";

    return ::grpc::Status::OK;
}

MemberChangeCommit::MemberChangeCommit(std::shared_ptr<RaftService::AsyncService> shp_svc,
    std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
    std::shared_ptr<ServerCompletionQueue> &shp_call_cq) noexcept {
    this->Initialize(shp_svc, shp_notify_cq, shp_call_cq);
    this->m_async_service->RequestMemberChangeCommit(&this->m_server_context, &this->m_request, &this->m_responder, this->m_server_call_cq.get(), this->m_server_notify_cq.get(), this);
}

::grpc::Status MemberChangeCommit::Process() noexcept {
    LOG(INFO) << "[Membership Change] MemberChangeCommit starts.";

    auto *_p_rsp = this->m_response.mutable_comm_rsp();

    auto _err_msg = this->FollowerCheckValidity(this->m_request.base());
    if (!_err_msg.empty()) {
        LOG(ERROR) << "[Membership Change] cannot do membership change commit,error message:" << _err_msg;
        _p_rsp->set_result(ErrorCode::FAIL);
        _p_rsp->set_err_msg(_err_msg);
        return ::grpc::Status::OK;
    }

    bool _still_in_new_cluster = MemberMgr::SwitchToStable();

    LOG(INFO) << "[Membership Change] switched to Stable status ";

    if(!_still_in_new_cluster){
        LOG(INFO) << "[Membership Change]I'm no longer in the new cluster , shutdown myself in 3 seconds,goodbye and have a good time.";
        //Must start a new thread to shutdown myself.
        auto _shutdown = [&]()->void {
            std::this_thread::sleep_for(std::chrono::seconds(3));
            this->SetServerShuttingDown();
            GlobalEnv::ShutDown();
        };
        std::thread _t(_shutdown);
        _t.detach();
    }

    /*If the old leader is not in the new cluster, the nodes in the new cluster will soon after start
      new rounds of elections, to achieve this, we need to reset the heartbeat clock. */
    if (this->m_request.has_flag()) {
        if (this->m_request.flag() == ::raft::MembershipFlag::NEWBIE) {
            WriteLock _w_lock(FollowerView::m_last_heartbeat_lock);
            FollowerView::m_last_heartbeat = std::chrono::steady_clock::now();
        }
    }

    _p_rsp->set_result(ErrorCode::SUCCESS);

    LOG(INFO) << "[Membership Change] MemberChangeCommit ends.";

    return ::grpc::Status::OK;
}

void MemberChangeCommit::SetServerShuttingDown() noexcept {
    WriteLock   _w_lock(this->m_mutex);
    LeaderView::m_status = LeaderView::ServerStatus::SHUTTING_DOWN;
}

PreVote::PreVote(std::shared_ptr<RaftService::AsyncService> shp_svc,
    std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
    std::shared_ptr<ServerCompletionQueue> &shp_call_cq) noexcept {
    this->Initialize(shp_svc, shp_notify_cq, shp_call_cq);
    this->m_async_service->RequestPreVote(&this->m_server_context, &this->m_request, &this->m_responder, this->m_server_call_cq.get(), this->m_server_notify_cq.get(), this);
}

::grpc::Status PreVote::Process() noexcept {
    auto _p_rsp = this->m_response.mutable_comm_rsp();
    _p_rsp->set_result(ErrorCode::PREVOTE_YES);

    auto _req_term = this->m_request.base().term();
    auto _req_addr = this->m_request.base().addr();

    if (!this->ValidClusterNode(_req_addr)) {
        _p_rsp->set_result(ErrorCode::PREVOTE_NO);
        _p_rsp->set_err_msg("You are not in my cluster config list:" + _req_addr);
        return ::grpc::Status::OK;
    }

    ElectionMgr::AddVotingTerm(_req_term,_req_addr);

    //Only candidate can vote.
    auto _current_role = StateMgr::GetRole();
    if (_current_role != RaftRole::CANDIDATE) {
        _p_rsp->set_result(ErrorCode::PREVOTE_NO);
        _p_rsp->set_err_msg("I'm a " + std::string(StateMgr::GetRoleStr(_current_role)) + " rather than a candidate.");
    }

    return ::grpc::Status::OK;
}

Vote::Vote(std::shared_ptr<RaftService::AsyncService> shp_svc,
    std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
    std::shared_ptr<ServerCompletionQueue> &shp_call_cq) noexcept {
    this->Initialize(shp_svc, shp_notify_cq, shp_call_cq);
    this->m_async_service->RequestVote(&this->m_server_context, &this->m_request, &this->m_responder, this->m_server_call_cq.get(), this->m_server_notify_cq.get(), this);
}

::grpc::Status Vote::Process() noexcept {
    auto _p_rsp = this->m_response.mutable_comm_rsp();
    _p_rsp->set_result(ErrorCode::VOTE_YES);

    auto _req_term = this->m_request.base().term();
    auto _req_addr = this->m_request.base().addr();

    if (!this->ValidClusterNode(_req_addr)) {
        _p_rsp->set_result(ErrorCode::PREVOTE_NO);
        _p_rsp->set_err_msg("You are not in my cluster config list:" + _req_addr);
        return ::grpc::Status::OK;
    }

    ElectionMgr::AddVotingTerm(_req_term,_req_addr);

    auto _current_role = StateMgr::GetRole();
    if (_current_role != RaftRole::CANDIDATE) {
        _p_rsp->set_result(ErrorCode::VOTE_NO);
        _p_rsp->set_err_msg("I'm not a candidate.");
        return ::grpc::Status::OK;
    }

    auto _my_term  = ElectionMgr::m_cur_term.load();
    if ( _req_term < _my_term) {
        _p_rsp->set_result(ErrorCode::VOTE_NO);
        _p_rsp->set_err_msg("I have a greater term:" + std::to_string(_my_term) + " than yours:" + std::to_string(_req_term));
        return ::grpc::Status::OK;
    }

    if (_req_term == _my_term) {
        _p_rsp->set_result(ErrorCode::VOTE_NO);
        _p_rsp->set_err_msg("I already issued an election in your term:" + std::to_string(_req_term));
        return ::grpc::Status::OK;
    }

    //Judge LOG ID.
    const auto & _id_lrl = BinLogGlobal::m_instance.GetLastReplicated();
    if (::RaftCore::Common::EntityIDSmaller(this->m_request.last_log_entity(), _id_lrl)) {
        _p_rsp->set_result(ErrorCode::VOTE_NO);
        _p_rsp->set_err_msg("Your log ID " + this->m_request.last_log_entity().DebugString()
                        + " is smaller than mine:" + _id_lrl.ToString());
        return ::grpc::Status::OK;
    }

    if (::RaftCore::Common::EntityIDEqual(this->m_request.last_log_entity(), _id_lrl)) {
        auto _req_version = this->m_request.member_version();
        auto _my_version  = MemberMgr::GetVersion();
        if (_req_version < _my_version) {
            _p_rsp->set_result(ErrorCode::VOTE_NO);
            _p_rsp->set_err_msg("Your membership version: " + std::to_string(_req_version)
                                + " is smaller than mine:" + std::to_string(_my_version));
            return ::grpc::Status::OK;
        }
    }

    auto _voted_addr = ElectionMgr::TryVote(_req_term, _req_addr);
    if (!_voted_addr.empty()) {
        _p_rsp->set_result(ErrorCode::VOTE_NO);
        _p_rsp->set_err_msg("Try vote fail,I've already voted addr:" + _voted_addr);
        return ::grpc::Status::OK;
    }

    return ::grpc::Status::OK;
}

HeartBeat::HeartBeat(std::shared_ptr<RaftService::AsyncService> shp_svc,
    std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
    std::shared_ptr<ServerCompletionQueue> &shp_call_cq) noexcept {
    this->Initialize(shp_svc, shp_notify_cq, shp_call_cq);
    this->m_async_service->RequestHeartBeat(&this->m_server_context, &this->m_request, &this->m_responder, this->m_server_call_cq.get(), this->m_server_notify_cq.get(), this);
}

::grpc::Status HeartBeat::Process() noexcept {

    VLOG(89) << "receive heartbeat.";

    const auto &_leader_term = this->m_request.base().term();
    const auto &_leader_addr = this->m_request.base().addr();

    if (!this->ValidClusterNode(_leader_addr)) {
        this->m_response.set_result(ErrorCode::PREVOTE_NO);
        this->m_response.set_err_msg("You are not in my cluster config list:" + _leader_addr);
        return ::grpc::Status::OK;
    }

    /*(Read & judge & take action) wrap the three to be an atomic operation. Tiny overhead for
        processing the periodically sent heartbeat messages.  */
    WriteLock _w_lock(ElectionMgr::m_election_mutex);

    uint32_t _cur_term = ElectionMgr::m_cur_term.load();
    if (_leader_term < _cur_term) {
        LOG(ERROR) << "a lower term heartbeat received,detail:" << this->m_request.DebugString();
        this->m_response.set_result(ErrorCode::FAIL);
        this->m_response.set_err_msg("your term " + std::to_string(_leader_term) + " is smaller than mine:" + std::to_string(_cur_term));
        return ::grpc::Status::OK;
    }

    {
        WriteLock _w_lock(FollowerView::m_last_heartbeat_lock);
        FollowerView::m_last_heartbeat = std::chrono::steady_clock::now();
    }

    this->m_response.set_result(::raft::SUCCESS);

    auto _cur_role = StateMgr::GetRole();

    if (_leader_term == _cur_term) {
        CHECK(_cur_role != RaftRole::LEADER) << "I'm a leader,receive heartbeat from the same term,detail:" << this->m_request.DebugString();

        if (_cur_role == RaftRole::FOLLOWER) {
            ::RaftCore::Topology    _topo;
            ::RaftCore::CTopologyMgr::Read(&_topo);
            CHECK(_topo.m_leader == _leader_addr) << "A different leader under term:" << _cur_term << " found"
                   << ",my leader addr:" << _topo.m_leader << ",peer leader addr : " << _leader_addr << ",ignore it.";
        }
        else if (_cur_role == RaftRole::CANDIDATE)
            ElectionMgr::NotifyNewLeaderEvent(_leader_term,_leader_addr);

        return ::grpc::Status::OK;
    }

    //Now :_leader_term > _cur_term ,switch role is needed.
    LOG(INFO) << "higher term found: " << _leader_term << ",current term:" << _cur_term
            <<  ",prepare to switch to follower with respect to the new leader :"  << _leader_addr;

    if (_cur_role == RaftRole::CANDIDATE) {
        //I'm in a electing state.
        ElectionMgr::NotifyNewLeaderEvent(_leader_term,_leader_addr);
    }
    else if (_cur_role == RaftRole::FOLLOWER) {
        ::RaftCore::Topology    _topo;
        ::RaftCore::CTopologyMgr::Read(&_topo);

        //New leader with a higher term found ,move the old leader to follower list.
        _topo.m_followers.emplace(_topo.m_leader);
        _topo.m_followers.erase(_leader_addr);
        _topo.m_candidates.erase(_leader_addr);
        _topo.m_leader = _leader_addr;

        ::RaftCore::CTopologyMgr::Update(_topo);

        ElectionMgr::m_cur_term.store(_leader_term);
    }
    else if (_cur_role == RaftRole::LEADER) {
        //Leader step down.
        ElectionMgr::m_cur_term.store(_leader_term);
        ElectionMgr::SwitchRole(RaftRole::FOLLOWER, _leader_addr);
    }

    this->m_response.set_result(::raft::SUCCESS);
    return ::grpc::Status::OK;
}

}
