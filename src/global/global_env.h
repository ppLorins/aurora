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

#ifndef __AURORA_GLOBAL_ENV_H__
#define __AURORA_GLOBAL_ENV_H__

#include <thread>

#include "grpc++/server_builder.h"
#include "grpc++/server.h"
#include "grpc++/completion_queue.h"

#include "protocol/raft.grpc.pb.h"

#include "state/state_mgr.h"
#include "common/react_base.h"
#include "common/react_group.h"

namespace RaftCore::Global {

using ::grpc::CompletionQueue;
using ::RaftCore::Common::TypePtrCQ;
using ::RaftCore::Common::TypeReactorFunc;
using ::RaftCore::Common::ReactWorkGroup;

/*Note: This is the class for representing follower's state in follower's own view. */
class GlobalEnv final {

public:

    static void InitialEnv(bool switching_role=false) noexcept;

    static void RunServer() noexcept;

    static void StopServer() noexcept;

    static void UnInitialEnv(::RaftCore::State::RaftRole state=::RaftCore::State::RaftRole::UNKNOWN) noexcept;

    //Note : Must called in other threads , not in any gRPC threads.
    static void ShutDown() noexcept;

    static bool IsRunning() noexcept;

    static TypePtrCQ<CompletionQueue> GetClientCQInstance(uint32_t idx) noexcept;

    static std::vector<ReactWorkGroup<>>    m_vec_notify_cq_workgroup;

private:

    static void InitGrpcEnv() noexcept;

    static void StartGrpcService() noexcept;

    static void SpawnFamilyBucket(std::shared_ptr<::raft::RaftService::AsyncService> shp_svc, std::size_t cq_idx) noexcept;

    static void StopGrpcService() noexcept;

private:

    static std::unique_ptr<::grpc::Server>     m_pserver;

    static volatile bool    m_running;

    static volatile bool    m_cq_fully_shutdown;

    static std::vector<ReactWorkGroup<>>    m_vec_call_cq_workgroup;

    static std::vector<ReactWorkGroup<CompletionQueue>>    m_vec_client_cq_workgroup;

    static std::shared_ptr<::raft::RaftService::AsyncService>   m_async_service;

    static std::string      m_server_addr;

    static ::grpc::ServerBuilder    m_builder;

private:

    GlobalEnv() = delete;

    virtual ~GlobalEnv() noexcept = delete;

    GlobalEnv(const GlobalEnv&) = delete;

    GlobalEnv& operator=(const GlobalEnv&) = delete;

};

} //end namespace

#endif
