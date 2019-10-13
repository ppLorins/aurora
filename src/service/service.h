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

#pragma once

#ifndef __AURORA_SERVICE_EX_H__
#define __AURORA_SERVICE_EX_H__

#include <set>
#include <mutex>

#include "protocol/raft.pb.h"
#include "protocol/raft.grpc.pb.h"

#include "common/log_identifier.h"
#include "leader/leader_request.h"
#include "leader/follower_entity.h"
#include "leader/leader_view.h"
#include "leader/leader_bg_task.h"
#include "follower/follower_request.h"
#include "follower/memory_log_follower.h"
#include "follower/follower_bg_task.h"
#include "candidate/candidate_request.h"
#include "guid/guid_generator.h"
#include "client/client_impl.h"
#include "tools/lock_free_unordered_single_list.h"
#include "service/ownership_delegator.h"

namespace RaftCore::Service {

using ::RaftCore::Guid::GuidGenerator;
using ::RaftCore::Common::LogIdentifier;
using ::RaftCore::DataStructure::AtomicPtrSingleListNode;
using ::RaftCore::Common::FinishStatus;
using ::RaftCore::Service::OwnershipDelegator;
using ::RaftCore::Leader::BackGroundTask::LogReplicationContext;
using ::RaftCore::Leader::BackGroundTask::TwoPhaseCommitContext;
using ::RaftCore::Leader::MemoryLogItemLeader;
using ::RaftCore::Leader::LeaderRequest;
using ::RaftCore::Leader::FollowerEntity;
using ::RaftCore::Leader::LeaderView;
using ::RaftCore::Follower::FollowerUnaryRequest;
using ::RaftCore::Follower::FollowerBidirectionalRequest;
using ::RaftCore::Follower::TypeMemlogFollowerList;
using ::RaftCore::Follower::BackGroundTask::DisorderMessageContext;
using ::RaftCore::Leader::BackGroundTask::CutEmptyContext;
using ::RaftCore::Candidate::CandidateUnaryRequest;
using ::RaftCore::Client::AppendEntriesAsyncClient;
using ::RaftCore::Tools::TypeTimePoint;
using ::RaftCore::Tools::TypeSysTimePoint;

//For the prospective common properties .
class RPCBase {

public:

    RPCBase();

    virtual ~RPCBase();

protected:

    bool LeaderCheckVailidity(::raft::ClientCommonResponse* response) noexcept;

    std::string FollowerCheckValidity(const ::raft::RequestBase &req_base, TypeTimePoint* p_tp = nullptr, LogIdentifier *p_cur_id = nullptr) noexcept;

    bool ValidClusterNode(const std::string &peer_addr) noexcept;

    inline static const char* MacroToString(LeaderView::ServerStatus enum_val) {
        return m_status_macro_names[int(enum_val)];
    }

protected:

    std::shared_timed_mutex    m_mutex;

    static const char*         m_status_macro_names[];

private:

    RPCBase(const RPCBase&) = delete;

    RPCBase& operator=(const RPCBase&) = delete;
};

class Write final : public LeaderRequest<::raft::ClientWriteRequest, ::raft::ClientWriteResponse, Write>,
    public OwnershipDelegator<Write>, public RPCBase {

public:

    Write(std::shared_ptr<RaftService::AsyncService> shp_svc,
        std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
        std::shared_ptr<ServerCompletionQueue> &shp_call_cq)noexcept;

    virtual ~Write();

    /*In Write RPC, the whole 'Process' procedure is divided into several smaller parts,
      just give an empty implementation here.*/
    ::grpc::Status Process() noexcept override;

    void ReplicateDoneCallBack(const ::grpc::Status &status, const ::raft::AppendEntriesResponse& rsp,
        FollowerEntity* ptr_follower, AppendEntriesAsyncClient* ptr_client) noexcept;

    //Return: the to-be entrusted client if any, otherwise a nullptr is returned.
    bool UpdatePhaseIStatistic(const ::grpc::Status &status,
        const ::raft::AppendEntriesResponse& rsp, FollowerEntity* ptr_follower) noexcept;

    void CommitDoneCallBack(const ::grpc::Status &status, const ::raft::CommitEntryResponse& rsp,
        FollowerEntity* ptr_follower) noexcept;

#ifdef _SVC_WRITE_TEST_
    auto GetInnerLog() { return this->m_shp_entity; }
#endif

    const std::shared_ptr<LogReplicationContext>& GetReqCtx() noexcept;

    void AfterAppendBinlog() noexcept;

    static void CutEmptyRoutine() noexcept;

private:

    enum class WriteProcessStage { CREATE, FRONT_FINISH, ABOURTED };

    virtual void React(bool cq_result = true) noexcept override;

    bool BeforeReplicate() noexcept;

    void AfterDetermined(AppendEntriesAsyncClient* ptr_client) noexcept;

    bool PrepareReplicationStatistic(std::list<std::shared_ptr<AppendEntriesAsyncClient>> &entrust_list) noexcept;

    //Return : if get majority entrusted.
    bool PrepareReplicationContext(uint32_t cur_term, uint32_t pre_term) noexcept;

    FinishStatus JudgeReplicationResult() noexcept;

    void ProcessReplicateFailure(const ::raft::CommonResponse& comm_rsp,
        TwoPhaseCommitContext::PhaseState &phaseI_state, FollowerEntity* ptr_follower,
        uint32_t joint_consensus_state) noexcept;

    void AddResyncLogTask(FollowerEntity* ptr_follower, const LogIdentifier &sync_point) noexcept;

    void EntrustCommitRequest(FollowerEntity* ptr_follower, AppendEntriesAsyncClient* ptr_client)noexcept;

    void ReleasePhaseIIReadyList()noexcept;

    void FinishRequest(WriteProcessStage state) noexcept;

    //Return: If successfully CutHead someone off from the pending list.
    bool AppendBinlog(AppendEntriesAsyncClient* ptr_client) noexcept;

    /*The microseconds that the thread should waiting for. After detecting a failure,
        waiting for its previous logs to have a deterministic result(success or implicitly/explicitly fail).
       Both the latest and non-latest logs have to be waited for.  */
    uint32_t GetConservativeTimeoutValue(uint64_t idx,bool last_guid=false) const noexcept;

    //Return : if server status successfully changed .
    bool UpdateServerStatus(uint64_t guid, LeaderView::ServerStatus status) noexcept;

    void LastlogResolve(bool result, uint64_t last_released_guid) noexcept;

    //Return the last released GUID.
    uint64_t WaitForLastGuidReleasing() const noexcept;

    bool ProcessCutEmptyRequest(const TypeSysTimePoint &tp, const LogIdentifier &current_lrl,
        std::shared_ptr<CutEmptyContext> &one, bool recheck) noexcept;

private:

    bool    m_first_of_cur_term = false;

    std::shared_ptr<MemoryLogItemLeader>    m_shp_entity;

    // The latest guid used setting server status.
    uint64_t    m_last_trigger_guid = 0;

    ::raft::EntityID*   m_p_pre_entity_id = nullptr;

    std::shared_ptr<LogReplicationContext>  m_shp_req_ctx;

    TypeTimePoint   m_tp_start;

    std::chrono::time_point<std::chrono::steady_clock>       m_wait_time_point;

    ::raft::ClientCommonResponse*  m_rsp = nullptr;

    GuidGenerator::GUIDPair     m_guid_pair;

    ::raft::ClientWriteRequest*   m_client_request = nullptr;

    std::shared_ptr<::raft::CommitEntryRequest>    m_shp_commit_req;

    //Indicating if the entry point of majority succeed is already taken by other threads.
    std::atomic<bool>     m_phaseI_determined_point;

    AtomicPtrSingleListNode<FollowerEntity>     m_phaseII_ready_list;

    WriteProcessStage     m_write_stage{ WriteProcessStage::CREATE };

#ifdef _SVC_WRITE_TEST_
    std::tm    m_start_tm = { 0, 0, 0, 26, 9 - 1, 2019 - 1900 };

    std::chrono::time_point<std::chrono::system_clock>  m_start_tp;
#endif

private:

    Write(const Write&) = delete;

    Write& operator=(const Write&) = delete;
};

class Read final : public LeaderRequest<::raft::ClientReadRequest,::raft::ClientReadResponse,Read>, public RPCBase {

public:

    Read(std::shared_ptr<RaftService::AsyncService> shp_svc,
        std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
        std::shared_ptr<ServerCompletionQueue> &shp_call_cq)noexcept;

    virtual ::grpc::Status Process() noexcept override;

private:

    Read(const Read&) = delete;

    Read& operator=(const Read&) = delete;
};

class MembershipChange final : public LeaderRequest<::raft::MemberChangeRequest,::raft::MemberChangeResponse,MembershipChange>, public RPCBase {

public:

    MembershipChange(std::shared_ptr<RaftService::AsyncService> shp_svc,
        std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
        std::shared_ptr<ServerCompletionQueue> &shp_call_cq)noexcept;

    virtual ::grpc::Status Process() noexcept override;

private:

    MembershipChange(const MembershipChange&) = delete;

    MembershipChange& operator=(const MembershipChange&) = delete;
};

class AppendEntries final : public FollowerUnaryRequest<::raft::AppendEntriesRequest, ::raft::AppendEntriesResponse, AppendEntries>,
    public OwnershipDelegator<AppendEntries>, public RPCBase {

public:

    AppendEntries(std::shared_ptr<RaftService::AsyncService> shp_svc,
        std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
        std::shared_ptr<ServerCompletionQueue> &shp_call_cq)noexcept;

    virtual ~AppendEntries()noexcept;

    //Return: If current request finished processing.
    bool BeforeJudgeOrder() noexcept ;

    const LogIdentifier& GetLastLogID() const noexcept;

    static void DisorderLogRoutine() noexcept;

protected:

    ::grpc::Status Process() noexcept override;

    virtual void React(bool cq_result = true) noexcept override;

private:

    bool ProcessDisorderLog(const TypeSysTimePoint &tp, const LogIdentifier &upper_log,
        std::shared_ptr<DisorderMessageContext> &one) noexcept;

    void ProcessAdjacentLog() noexcept;

    void ProcessOverlappedLog() noexcept;

    std::string ComposeInputLogs() noexcept;

private:

    TypeTimePoint   m_tp_start;

    ::raft::CommonResponse*   m_rsp = nullptr;

    //std::unique_lock<std::mutex>   *m_mutex_lock = nullptr;

    TypeMemlogFollowerList     m_log_list;

    const ::raft::EntityID     *m_pre_entity_id = nullptr;

    const ::raft::EntityID     *m_last_entity_id = nullptr;

    LogIdentifier   m_last_log;

    enum class AppendEntriesProcessStage { CREATE, WAITING, FINISH };

    AppendEntriesProcessStage     m_append_entries_stage{ AppendEntriesProcessStage::CREATE };

#ifdef _SVC_APPEND_ENTRIES_TEST_
    std::tm    m_start_tm = { 0, 0, 0, 26, 9 - 1, 2019 - 1900 };

    std::chrono::time_point<std::chrono::system_clock>  m_start_tp;
#endif

private:

    AppendEntries(const AppendEntries&) = delete;

    AppendEntries& operator=(const AppendEntries&) = delete;
};

class CommitEntries final : public FollowerUnaryRequest<::raft::CommitEntryRequest,::raft::CommitEntryResponse,CommitEntries>, public RPCBase {

public:

    CommitEntries(std::shared_ptr<RaftService::AsyncService> shp_svc,
        std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
        std::shared_ptr<ServerCompletionQueue> &shp_call_cq)noexcept;

    virtual ::grpc::Status Process() noexcept override;

private:

    CommitEntries(const CommitEntries&) = delete;

    CommitEntries& operator=(const CommitEntries&) = delete;
};

class SyncData final : public FollowerBidirectionalRequest<::raft::SyncDataRequest,::raft::SyncDataResponse,SyncData>, public RPCBase {

public:

    SyncData(std::shared_ptr<RaftService::AsyncService> shp_svc,
        std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
        std::shared_ptr<ServerCompletionQueue> &shp_call_cq)noexcept;

    virtual ::grpc::Status Process() noexcept override;

private:

    SyncData(const SyncData&) = delete;

    SyncData& operator=(const SyncData&) = delete;
};

class MemberChangePrepare final : public FollowerUnaryRequest<::raft::MemberChangeInnerRequest,::raft::MemberChangeInnerResponse,MemberChangePrepare>, public RPCBase {

public:

    MemberChangePrepare(std::shared_ptr<RaftService::AsyncService> shp_svc,
        std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
        std::shared_ptr<ServerCompletionQueue> &shp_call_cq)noexcept;

    virtual ::grpc::Status Process() noexcept override;

private:

    MemberChangePrepare(const MemberChangePrepare&) = delete;

    MemberChangePrepare& operator=(const MemberChangePrepare&) = delete;
};

class MemberChangeCommit final : public FollowerUnaryRequest<::raft::MemberChangeInnerRequest,::raft::MemberChangeInnerResponse,MemberChangeCommit>, public RPCBase {

public:

    MemberChangeCommit(std::shared_ptr<RaftService::AsyncService> shp_svc,
        std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
        std::shared_ptr<ServerCompletionQueue> &shp_call_cq)noexcept;

    virtual ::grpc::Status Process() noexcept override;

private:

    void SetServerShuttingDown() noexcept;

private:

    MemberChangeCommit(const MemberChangeCommit&) = delete;

    MemberChangeCommit& operator=(const MemberChangeCommit&) = delete;
};

class PreVote final : public CandidateUnaryRequest<::raft::VoteRequest,::raft::VoteResponse,PreVote>, public RPCBase {

public:

    PreVote(std::shared_ptr<RaftService::AsyncService> shp_svc,
        std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
        std::shared_ptr<ServerCompletionQueue> &shp_call_cq)noexcept;

    virtual ::grpc::Status Process() noexcept override;

private:

    PreVote(const PreVote&) = delete;

    PreVote& operator=(const PreVote&) = delete;
};

class Vote final : public CandidateUnaryRequest<::raft::VoteRequest,::raft::VoteResponse,Vote>, public RPCBase {

public:

    Vote(std::shared_ptr<RaftService::AsyncService> shp_svc,
        std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
        std::shared_ptr<ServerCompletionQueue> &shp_call_cq)noexcept;

    virtual ::grpc::Status Process() noexcept override;

private:

    Vote(const Vote&) = delete;

    Vote& operator=(const Vote&) = delete;
};

//This RPC making sense to multiple roles.
class HeartBeat final : public UnaryRequest<::raft::HeartBeatRequest,::raft::CommonResponse,HeartBeat>, public RPCBase {

public:

    HeartBeat(std::shared_ptr<RaftService::AsyncService> shp_svc,
        std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
        std::shared_ptr<ServerCompletionQueue> &shp_call_cq)noexcept;

    virtual ::grpc::Status Process() noexcept override;

private:

    HeartBeat(const HeartBeat&) = delete;

    HeartBeat& operator=(const HeartBeat&) = delete;
};

}

#endif
