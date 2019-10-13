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

#ifndef __AURORA_CLIENT_FRAMEWORK_H__
#define __AURORA_CLIENT_FRAMEWORK_H__

#include <memory>

#include "grpc/grpc.h"
#include "grpc++/grpc++.h"

#include "protocol/raft.grpc.pb.h"
#include "common/react_base.h"

namespace RaftCore::Client {

using ::grpc::CompletionQueue;
//using ::grpc::ServerCompletionQueue;
using ::grpc::Channel;
using ::RaftCore::Common::ReactBase;

template<typename T,typename R>
using FPrepareAsync = std::function<std::unique_ptr< ::grpc::ClientAsyncResponseReader<R>>(
    ::grpc::ClientContext*,const T&, ::grpc::CompletionQueue*)>;

template<typename T=void>
class ClientFramework {

public:

    ClientFramework(std::shared_ptr<Channel> shp_channel)noexcept;

    virtual ~ClientFramework()noexcept;

    //Reset a client object for reusing purpose.
    virtual void Reset() noexcept;

    virtual std::shared_ptr<::raft::RaftService::Stub> GetStub() noexcept final;

protected:

    std::shared_ptr<::grpc::ClientContext>    m_client_context;

    std::shared_ptr<::raft::RaftService::Stub>    m_stub;

    ::grpc::Status  m_final_status;

private:

    ClientFramework(const ClientFramework&) = delete;

    ClientFramework& operator=(const ClientFramework&) = delete;
};

template<typename T,typename R>
class ClientTpl : public ClientFramework<void> {

public:

    typedef std::function<void(const ::grpc::Status &, const R&)>  TypeResponder;

    ClientTpl(std::shared_ptr<Channel> shp_channel)noexcept;

    virtual ~ClientTpl()noexcept;

protected:

    std::shared_ptr<T>   m_shp_request;

    R   m_response;

private:

    ClientTpl(const ClientTpl&) = delete;

    ClientTpl& operator=(const ClientTpl&) = delete;
};

template<typename T,typename R>
class SyncClient : public ClientTpl<T,R> {

public:

    SyncClient(std::shared_ptr<Channel> shp_channel)noexcept;

    virtual ~SyncClient()noexcept;

private:

    SyncClient(const SyncClient&) = delete;

    SyncClient& operator=(const SyncClient&) = delete;
};

template<typename T,typename R>
class UnarySyncClient : public SyncClient<T,R> {

public:

    UnarySyncClient(std::shared_ptr<Channel> shp_channel)noexcept;

    virtual ~UnarySyncClient()noexcept;

    const R&  DoRPC(std::function<void(std::shared_ptr<T>&)> req_setter,
        std::function<::grpc::Status(::grpc::ClientContext*,const T&,R*)> rpc, uint32_t timeo_ms,
        ::grpc::Status &ret_status)noexcept;

private:

    UnarySyncClient(const UnarySyncClient&) = delete;

    UnarySyncClient& operator=(const UnarySyncClient&) = delete;
};

template<typename T,typename R>
class BidirectionalSyncClient : public SyncClient<T,R> {

public:

    BidirectionalSyncClient(std::shared_ptr<Channel> shp_channel)noexcept;

    virtual ~BidirectionalSyncClient()noexcept;

protected:

    std::shared_ptr<::grpc::ClientReaderWriter<T, R>>     m_sync_rw;

private:

    BidirectionalSyncClient(const BidirectionalSyncClient&) = delete;

    BidirectionalSyncClient& operator=(const BidirectionalSyncClient&) = delete;
};

template<typename T,typename R,typename CQ=CompletionQueue>
class AsyncClient : public ClientTpl<T,R>, public ReactBase {

public:

    AsyncClient(std::shared_ptr<Channel> shp_channel,std::shared_ptr<CQ> shp_cq)noexcept;

    virtual ~AsyncClient()noexcept;

    virtual void Responder(const ::grpc::Status& status, const R&  rsp) noexcept = 0;

protected:

    std::shared_ptr<CQ>     m_server_cq;

private:

    AsyncClient(const AsyncClient&) = delete;

    AsyncClient& operator=(const AsyncClient&) = delete;
};

template<typename T,typename R,typename Q,typename CQ=CompletionQueue>
class UnaryAsyncClient : public AsyncClient<T,R,CQ> {

public:

    UnaryAsyncClient(std::shared_ptr<Channel> shp_channel,std::shared_ptr<CQ> shp_cq)noexcept;

    virtual ~UnaryAsyncClient()noexcept;

    void EntrustRequest(const std::function<void(std::shared_ptr<T>&)> &req_setter,
        const FPrepareAsync<T,R> &f_prepare_async, uint32_t timeo_ms) noexcept;

protected:

    virtual void React(bool cq_result) noexcept override;

    virtual void Release() noexcept;

protected:

    std::unique_ptr<::grpc::ClientAsyncResponseReader<R>>    m_reader;

private:

    UnaryAsyncClient(const UnaryAsyncClient&) = delete;

    UnaryAsyncClient& operator=(const UnaryAsyncClient&) = delete;
};

template<typename T,typename R,typename Q>
class BidirectionalAsyncClient : public AsyncClient<T,R> {

public:

    BidirectionalAsyncClient(std::shared_ptr<Channel> shp_channel, std::shared_ptr<CompletionQueue> shp_cq)noexcept;

    virtual ~BidirectionalAsyncClient()noexcept;

protected:

    virtual void React(bool cq_result) noexcept override;

    void AsyncDo(std::function<void(std::shared_ptr<T>&)> req_setter) noexcept;

    void WriteDone() noexcept;

protected:

    ::grpc::ClientAsyncReaderWriter<T,R>        m_async_rw;

    enum ProcessStage { READ = 1, WRITE = 2, CONNECT = 3, WRITES_DONE = 4, FINISH = 5 };

    ProcessStage   m_status;

private:

    BidirectionalAsyncClient(const BidirectionalAsyncClient&) = delete;

    BidirectionalAsyncClient& operator=(const BidirectionalAsyncClient&) = delete;
};

} //end namespace

#include "client/client_framework.cc"

#endif
