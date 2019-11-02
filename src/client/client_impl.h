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

#ifndef __AURORA_CLIENT_IMPL_H__
#define __AURORA_CLIENT_IMPL_H__

#include "protocol/raft.pb.h"
#include "protocol/raft.grpc.pb.h"

#include "client/client_framework.h"
#include "client/client_base.h"
#include "service/ownership_delegator.h"

namespace RaftCore {
    namespace Service {
        class Write;
    }
}

namespace RaftCore::Client {

using ::RaftCore::Client::UnarySyncClient;
using ::RaftCore::Client::ClientBase;
using ::RaftCore::Service::Write;
using ::RaftCore::Service::OwnershipDelegator;

class AppendEntriesAsyncClient : public UnaryAsyncClient<::raft::AppendEntriesRequest,
    ::raft::AppendEntriesResponse, AppendEntriesAsyncClient>,
    public OwnershipDelegator<AppendEntriesAsyncClient>, public OwnershipDelegator<Write>,
    public ClientBase {

public:

    AppendEntriesAsyncClient(std::shared_ptr<::grpc::Channel> shp_channel,
        std::shared_ptr<::grpc::CompletionQueue> shp_cq, bool delegate_me = true);

    virtual ~AppendEntriesAsyncClient();

protected:

    virtual void Responder(const ::grpc::Status& status, const ::raft::AppendEntriesResponse&  rsp) noexcept override;

    virtual void Release() noexcept override;

private:

    AppendEntriesAsyncClient(const AppendEntriesAsyncClient&) = delete;

    AppendEntriesAsyncClient& operator=(const AppendEntriesAsyncClient&) = delete;
};

class CommitEntriesAsyncClient : public UnaryAsyncClient<::raft::CommitEntryRequest,
    ::raft::CommitEntryResponse, CommitEntriesAsyncClient>,
    public OwnershipDelegator<CommitEntriesAsyncClient>, public OwnershipDelegator<Write>,
    public ClientBase {

public:

    CommitEntriesAsyncClient(std::shared_ptr<::grpc::Channel> shp_channel,
        std::shared_ptr<::grpc::CompletionQueue> shp_cq);

    virtual ~CommitEntriesAsyncClient();

    virtual void Release() noexcept override;

protected:

    virtual void Responder(const ::grpc::Status& status, const ::raft::CommitEntryResponse&  rsp) noexcept override;

private:

    CommitEntriesAsyncClient(const CommitEntriesAsyncClient&) = delete;

    CommitEntriesAsyncClient& operator=(const CommitEntriesAsyncClient&) = delete;
};

typedef std::shared_ptr<CommitEntriesAsyncClient>  TypePtrCommitAC;

class HeartbeatSyncClient : public UnarySyncClient<::raft::HeartBeatRequest, ::raft::CommonResponse>,
    public ClientBase {

public:

    HeartbeatSyncClient(std::shared_ptr<::grpc::Channel> shp_channel);

    virtual ~HeartbeatSyncClient();

private:

    HeartbeatSyncClient(const HeartbeatSyncClient&) = delete;

    HeartbeatSyncClient& operator=(const HeartbeatSyncClient&) = delete;
};

class WriteSyncClient : public UnarySyncClient<::raft::ClientWriteRequest, ::raft::ClientWriteResponse>,
    public ClientBase {

public:

    WriteSyncClient(std::shared_ptr<::grpc::Channel> shp_channel);

    virtual ~WriteSyncClient();

private:

    WriteSyncClient(const WriteSyncClient&) = delete;

    WriteSyncClient& operator=(const WriteSyncClient&) = delete;
};

class SyncDataSyncClient : public BidirectionalSyncClient<::raft::SyncDataRequest, ::raft::SyncDataResponse>,
    public ClientBase {

public:

    SyncDataSyncClient(std::shared_ptr<::grpc::Channel> shp_channel);

    virtual ~SyncDataSyncClient();

    auto GetInstantiatedReq() noexcept-> decltype(m_shp_request);

    auto GetReaderWriter() noexcept -> decltype(m_sync_rw);

    ::raft::SyncDataResponse* GetResponse() noexcept;

private:

    SyncDataSyncClient(const SyncDataSyncClient&) = delete;

    SyncDataSyncClient& operator=(const SyncDataSyncClient&) = delete;
};

class MemberChangePrepareAsyncClient : public UnaryAsyncClient<::raft::MemberChangeInnerRequest,
    ::raft::MemberChangeInnerResponse, MemberChangePrepareAsyncClient,::grpc::CompletionQueue>, public ClientBase {

public:

    MemberChangePrepareAsyncClient(std::shared_ptr<::grpc::Channel> shp_channel,
        std::shared_ptr<::grpc::CompletionQueue> shp_cq);

    virtual ~MemberChangePrepareAsyncClient();

protected:

    virtual void Responder(const ::grpc::Status& status, const ::raft::MemberChangeInnerResponse&  rsp) noexcept override;

private:

    MemberChangePrepareAsyncClient(const MemberChangePrepareAsyncClient&) = delete;

    MemberChangePrepareAsyncClient& operator=(const MemberChangePrepareAsyncClient&) = delete;
};

class MemberChangeCommitAsyncClient : public UnaryAsyncClient<::raft::MemberChangeInnerRequest,
    ::raft::MemberChangeInnerResponse, MemberChangeCommitAsyncClient,::grpc::CompletionQueue>, public ClientBase {

public:

    MemberChangeCommitAsyncClient(std::shared_ptr<::grpc::Channel> shp_channel,
        std::shared_ptr<::grpc::CompletionQueue> shp_cq);

    virtual ~MemberChangeCommitAsyncClient();

protected:

    virtual void Responder(const ::grpc::Status& status, const ::raft::MemberChangeInnerResponse&  rsp) noexcept override;

private:

    MemberChangeCommitAsyncClient(const MemberChangeCommitAsyncClient&) = delete;

    MemberChangeCommitAsyncClient& operator=(const MemberChangeCommitAsyncClient&) = delete;

};

class PrevoteAsyncClient : public UnaryAsyncClient<::raft::VoteRequest,
    ::raft::VoteResponse, PrevoteAsyncClient,::grpc::CompletionQueue>, public ClientBase {

public:

    PrevoteAsyncClient(std::shared_ptr<::grpc::Channel> shp_channel,
        std::shared_ptr<::grpc::CompletionQueue> shp_cq);

    virtual ~PrevoteAsyncClient();

protected:

    virtual void Responder(const ::grpc::Status& status, const ::raft::VoteResponse&  rsp) noexcept override;

private:

    PrevoteAsyncClient(const PrevoteAsyncClient&) = delete;

    PrevoteAsyncClient& operator=(const PrevoteAsyncClient&) = delete;

};

class VoteAsyncClient : public UnaryAsyncClient<::raft::VoteRequest,
    ::raft::VoteResponse, VoteAsyncClient,::grpc::CompletionQueue>, public ClientBase {

public:

    VoteAsyncClient(std::shared_ptr<::grpc::Channel> shp_channel,
        std::shared_ptr<::grpc::CompletionQueue> shp_cq);

    virtual ~VoteAsyncClient();

protected:

    virtual void Responder(const ::grpc::Status& status, const ::raft::VoteResponse&  rsp) noexcept override;

private:

    VoteAsyncClient(const VoteAsyncClient&) = delete;

    VoteAsyncClient& operator=(const VoteAsyncClient&) = delete;

};

}

#endif
