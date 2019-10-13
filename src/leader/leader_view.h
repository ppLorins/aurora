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

#ifndef __AURORA_LEADER_VIEW_H__
#define __AURORA_LEADER_VIEW_H__

#include <memory>
#include <string>
#include <set>

#include "grpc/grpc.h"
#include "grpc++/grpc++.h"

#include "protocol/raft.pb.h"

#include "common/comm_defs.h"
#include "common/comm_view.h"
#include "topology/topology_mgr.h"
#include "tools/lock_free_queue.h"
#include "tools/trivial_lock_double_list.h"
#include "tools/trivial_lock_single_list.h"
#include "leader/follower_entity.h"
#include "leader/leader_bg_task.h"
#include "leader/memory_log_leader.h"

namespace RaftCore::Leader {

using grpc::CompletionQueue;
using ::RaftCore::Common::CommonView;
using ::RaftCore::Leader::MemoryLogItemLeader;
using ::RaftCore::Leader::BackGroundTask::CutEmptyContext;
using ::RaftCore::Common::LogIdentifier;
using ::RaftCore::Common::ReactInfo;
using ::RaftCore::DataStructure::DoubleListNode;
using ::RaftCore::DataStructure::TrivialLockDoubleList;
using ::RaftCore::DataStructure::SingleListNode;
using ::RaftCore::DataStructure::TrivialLockSingleList;
using ::RaftCore::DataStructure::LockFreeUnorderedSingleList;

class LeaderView :public CommonView{

public:

    enum class ServerStatus {
        NORMAL=0,
        HALTED,
        SHUTTING_DOWN,
    };

public:

    static void Initialize(const ::RaftCore::Topology& _topo) noexcept;

    static void UnInitialize() noexcept;

//Set the following member functions to protected is to facilitate gtest.
#ifdef _LEADER_VIEW_TEST_
public:
#else
private:
#endif

    static bool ReSyncLogCB(std::shared_ptr<BackGroundTask::ReSyncLogContext> &shp_context)noexcept;

    static bool SyncDataCB(std::shared_ptr<BackGroundTask::SyncDataContenxt> &shp_context)noexcept;

    static bool ClientReactCB(std::shared_ptr<BackGroundTask::ClientReactContext> &shp_context) noexcept;

    static void ClientThreadReacting(const ReactInfo &info) noexcept;

    static void BroadcastHeatBeat() noexcept;

public:

    static  std::string                 my_addr;

    static std::unordered_map<std::string,TypePtrFollowerEntity>    m_hash_followers;

    static std::shared_timed_mutex    m_hash_followers_mutex;

    static TrivialLockDoubleList<MemoryLogItemLeader>      m_entity_pending_list;

    static LockFreeUnorderedSingleList<DoubleListNode<MemoryLogItemLeader>>     m_garbage;

    //Used for write requests which cannot get finished after it CutHead.
    static TrivialLockSingleList<CutEmptyContext>   m_cut_empty_list;

    static LockFreeUnorderedSingleList<SingleListNode<CutEmptyContext>>     m_cut_empty_garbage;

    //CV used for multiple threads cooperating on append binlog operations.
    static std::condition_variable         m_cv;

    static std::mutex                      m_cv_mutex;

    static std::atomic<LogIdentifier>      m_last_cut_log;

    static ServerStatus     m_status;

    static std::atomic<uint32_t>      m_last_log_waiting_num;

private:

    static void AddRescynDataTask(std::shared_ptr<BackGroundTask::ReSyncLogContext> &shp_context) noexcept;

    static auto PrepareAppendEntriesRequest(std::shared_ptr<BackGroundTask::ReSyncLogContext> &shp_context);

    static bool SyncLogAfterLCL(std::shared_ptr<BackGroundTask::SyncDataContenxt> &shp_context);

private:

    static const char*  m_invoker_macro_names[];

private:

    LeaderView() = delete;

    virtual ~LeaderView() = delete;

    LeaderView(const LeaderView &) = delete;

    LeaderView& operator=(const LeaderView &) = delete;
};

}


#endif
