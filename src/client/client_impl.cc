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

#include "service/service.h"
#include "leader/client_pool.h"
#include "member/member_manager.h"
#include "election/election.h"
#include "client/client_impl.h"

namespace RaftCore::Client {

using ::RaftCore::Common::VoteType;
using ::RaftCore::Service::Write;
using ::RaftCore::Leader::ClientPool;
using ::RaftCore::Member::MemberMgr;
using ::RaftCore::Election::ElectionMgr;

AppendEntriesAsyncClient::AppendEntriesAsyncClient(std::shared_ptr<::grpc::Channel> shp_channel,
    std::shared_ptr<::grpc::CompletionQueue> shp_cq)
    : UnaryAsyncClient<::raft::AppendEntriesRequest, ::raft::AppendEntriesResponse,
    AppendEntriesAsyncClient>(shp_channel, shp_cq) {

    //Give myself a long lived delegator.
    this->OwnershipDelegator<AppendEntriesAsyncClient>::ResetOwnership(this);
}

AppendEntriesAsyncClient::~AppendEntriesAsyncClient() {}

void AppendEntriesAsyncClient::Responder(const ::grpc::Status& status,
    const ::raft::AppendEntriesResponse&  rsp) noexcept {

    auto _shp_write = this->OwnershipDelegator<Write>::GetOwnership();
    auto *_p_conn_pool = (ClientPool<AppendEntriesAsyncClient>*)this->m_callback_args[0];

    _shp_write->ReplicateDoneCallBack(status, rsp, _p_conn_pool->GetParentFollower(), this);
}

void AppendEntriesAsyncClient::Release() noexcept {
    //Reset myself.
    this->Reset();

    //Release associated write request.
    this->OwnershipDelegator<Write>::ReleaseOwnership();

    //Push myself back to the connection pool.
    auto *_p_conn_pool = (ClientPool<AppendEntriesAsyncClient>*)this->m_callback_args[0];
    auto _shp_copied = this->OwnershipDelegator<AppendEntriesAsyncClient>::GetOwnership();
    _p_conn_pool->Back(_shp_copied);

    VLOG(90) << "AppendEntriesAsyncClient returned:" << _p_conn_pool->GetParentFollower()->my_addr;

    //Clear my args.
    this->ClearCallBackArgs();
}

CommitEntriesAsyncClient::CommitEntriesAsyncClient(std::shared_ptr<::grpc::Channel> shp_channel,
    std::shared_ptr<::grpc::CompletionQueue> shp_cq)
    : UnaryAsyncClient<::raft::CommitEntryRequest, ::raft::CommitEntryResponse,
    CommitEntriesAsyncClient> (shp_channel, shp_cq){

    //Give myself a long lived delegator.
    this->OwnershipDelegator<CommitEntriesAsyncClient>::ResetOwnership(this);
}

CommitEntriesAsyncClient::~CommitEntriesAsyncClient() {}

void CommitEntriesAsyncClient::Responder(const ::grpc::Status& status,
    const ::raft::CommitEntryResponse&  rsp) noexcept {

    auto _shp_write = this->OwnershipDelegator<Write>::GetOwnership();
    auto *_p_conn_pool = (ClientPool<CommitEntriesAsyncClient>*)this->m_callback_args[0];

    _shp_write->CommitDoneCallBack(status, rsp, _p_conn_pool->GetParentFollower());
}

void CommitEntriesAsyncClient::Release() noexcept {
    //Reset myself.
    this->Reset();

    //Release associated write request.
    this->OwnershipDelegator<Write>::ReleaseOwnership();

    //Push myself back to the connection pool.
    auto *_p_conn_pool = (ClientPool<CommitEntriesAsyncClient>*)this->m_callback_args[0];

    auto _shp_copied = this->OwnershipDelegator<CommitEntriesAsyncClient>::GetOwnership();
    _p_conn_pool->Back(_shp_copied);

    VLOG(90) << "CommitEntriesAsyncClient returned:" << _p_conn_pool->GetParentFollower()->my_addr;

    //Clear my args.
    this->ClearCallBackArgs();
}

HeartbeatSyncClient::HeartbeatSyncClient(std::shared_ptr<::grpc::Channel> shp_channel):
    UnarySyncClient<::raft::HeartBeatRequest, ::raft::CommonResponse>(shp_channel) {}

HeartbeatSyncClient::~HeartbeatSyncClient() {}

WriteSyncClient::WriteSyncClient(std::shared_ptr<::grpc::Channel> shp_channel):
    UnarySyncClient<::raft::ClientWriteRequest, ::raft::ClientWriteResponse>(shp_channel) {}

WriteSyncClient::~WriteSyncClient() {}

SyncDataSyncClient::SyncDataSyncClient(std::shared_ptr<::grpc::Channel> shp_channel):
    BidirectionalSyncClient<::raft::SyncDataRequest, ::raft::SyncDataResponse>(shp_channel) {
    this->m_sync_rw = this->m_stub->SyncData(this->m_client_context.get());
}

SyncDataSyncClient::~SyncDataSyncClient() {}

auto SyncDataSyncClient::GetInstantiatedReq()noexcept -> decltype(m_shp_request) {
    if (!this->m_shp_request)
        this->m_shp_request.reset(new ::raft::SyncDataRequest());

    return this->m_shp_request;
}

auto SyncDataSyncClient::GetReaderWriter() noexcept -> decltype(m_sync_rw) {
    return this->m_sync_rw;
}

::raft::SyncDataResponse* SyncDataSyncClient::GetResponse() noexcept {
    return &this->m_response;
}

MemberChangePrepareAsyncClient::MemberChangePrepareAsyncClient(std::shared_ptr<::grpc::Channel> shp_channel,
    std::shared_ptr<::grpc::CompletionQueue> shp_cq)
    : UnaryAsyncClient<::raft::MemberChangeInnerRequest, ::raft::MemberChangeInnerResponse,
    MemberChangePrepareAsyncClient,::grpc::CompletionQueue>(shp_channel, shp_cq){}

MemberChangePrepareAsyncClient::~MemberChangePrepareAsyncClient() {}

void MemberChangePrepareAsyncClient::Responder(const ::grpc::Status& status,
    const ::raft::MemberChangeInnerResponse&  rsp) noexcept {
    uint32_t _idx = static_cast<uint32_t>(reinterpret_cast<uintptr_t>(m_callback_args[1]));
    MemberMgr::MemberChangePrepareCallBack(status, rsp, m_callback_args[0], _idx);
}

MemberChangeCommitAsyncClient::MemberChangeCommitAsyncClient(std::shared_ptr<::grpc::Channel> shp_channel,
    std::shared_ptr<::grpc::CompletionQueue> shp_cq)
    : UnaryAsyncClient<::raft::MemberChangeInnerRequest, ::raft::MemberChangeInnerResponse,
    MemberChangeCommitAsyncClient,::grpc::CompletionQueue>(shp_channel, shp_cq){}

MemberChangeCommitAsyncClient::~MemberChangeCommitAsyncClient() {}

void MemberChangeCommitAsyncClient::Responder(const ::grpc::Status& status,
    const ::raft::MemberChangeInnerResponse&  rsp) noexcept {
    uint32_t _idx = static_cast<uint32_t>(reinterpret_cast<uintptr_t>(m_callback_args[1]));
    MemberMgr::MemberChangeCommitCallBack(status, rsp, m_callback_args[0], _idx);
}

PrevoteAsyncClient::PrevoteAsyncClient(std::shared_ptr<::grpc::Channel> shp_channel,
    std::shared_ptr<::grpc::CompletionQueue> shp_cq)
    : UnaryAsyncClient<::raft::VoteRequest, ::raft::VoteResponse,
    PrevoteAsyncClient,::grpc::CompletionQueue>(shp_channel, shp_cq){}

PrevoteAsyncClient::~PrevoteAsyncClient() {}

void PrevoteAsyncClient::Responder(const ::grpc::Status& status,
    const ::raft::VoteResponse&  rsp) noexcept {
    uint32_t _idx = static_cast<uint32_t>(reinterpret_cast<uintptr_t>(m_callback_args[0]));
    ElectionMgr::CallBack(status, rsp, VoteType::PreVote,_idx);
}

VoteAsyncClient::VoteAsyncClient(std::shared_ptr<::grpc::Channel> shp_channel,
    std::shared_ptr<::grpc::CompletionQueue> shp_cq)
    : UnaryAsyncClient<::raft::VoteRequest, ::raft::VoteResponse,
    VoteAsyncClient,::grpc::CompletionQueue>(shp_channel, shp_cq){}

VoteAsyncClient::~VoteAsyncClient() {}

void VoteAsyncClient::Responder(const ::grpc::Status& status,
    const ::raft::VoteResponse& rsp) noexcept {
    uint32_t _idx = static_cast<uint32_t>(reinterpret_cast<uintptr_t>(m_callback_args[0]));
    ElectionMgr::CallBack(status, rsp, VoteType::Vote,_idx);
}

}
