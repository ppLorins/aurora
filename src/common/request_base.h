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

#ifndef __AURORA_REQUEST_BASE_H__
#define __AURORA_REQUEST_BASE_H__

#include <memory>

#include "protocol/raft.grpc.pb.h"
#include "protocol/raft.pb.h"

#include "common/react_base.h"

using ::raft::RaftService;
using ::grpc::ServerCompletionQueue;

namespace RaftCore::Common {

using ::RaftCore::Common::ReactBase;

template<typename T=void>
class RequestBase : public ReactBase {

public:

    RequestBase()noexcept;

    void Initialize(std::shared_ptr<RaftService::AsyncService> shp_svc,
        std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
        std::shared_ptr<ServerCompletionQueue> &shp_call_cq)noexcept;

    virtual ~RequestBase()noexcept;

    virtual ::grpc::Status Process() noexcept = 0;

protected:

    //Server context cannot be reused across rpcs.
    ::grpc::ServerContext    m_server_context;

    std::shared_ptr<RaftService::AsyncService>   m_async_service;

    std::shared_ptr<ServerCompletionQueue>       m_server_notify_cq;

    std::shared_ptr<ServerCompletionQueue>       m_server_call_cq;

private:

    RequestBase(const RequestBase&) = delete;

    RequestBase& operator=(const RequestBase&) = delete;
};

template<typename T,typename R>
class RequestTpl : public RequestBase<void> {

public:

    RequestTpl()noexcept;

    virtual ~RequestTpl()noexcept;

protected:

    T   m_request;

    R   m_response;

private:

    RequestTpl(const RequestTpl&) = delete;

    RequestTpl& operator=(const RequestTpl&) = delete;
};

template<typename T,typename R,typename Q>
class UnaryRequest : public RequestTpl<T,R> {

public:

    UnaryRequest()noexcept;

    virtual ~UnaryRequest()noexcept;

protected:

    virtual void React(bool cq_result) noexcept override;

protected:

    ::grpc::ServerAsyncResponseWriter<R>        m_responder;

    enum class ProcessStage { CREATE = 0, FINISH };

    ProcessStage   m_stage;

private:

    UnaryRequest(const UnaryRequest&) = delete;

    UnaryRequest& operator=(const UnaryRequest&) = delete;
};

template<typename T,typename R,typename Q>
class BidirectionalRequest : public RequestTpl<T,R> {

public:

    BidirectionalRequest()noexcept;

    virtual ~BidirectionalRequest()noexcept;

protected:

    virtual void React(bool cq_result) noexcept override;

    const char* GetStageName()const noexcept;

protected:

    ::grpc::ServerAsyncReaderWriter<R,T>        m_reader_writer;

    enum class ProcessStage { READ = 0, WRITE, CONNECT, DONE, FINISH };

    static const char*  m_status_macro_names[];

    ProcessStage   m_stage;

private:

    std::mutex  m_mutex;

private:

    BidirectionalRequest(const BidirectionalRequest&) = delete;

    BidirectionalRequest& operator=(const BidirectionalRequest&) = delete;
};

} //end namespace

#include "common/request_base.cc"

#endif
