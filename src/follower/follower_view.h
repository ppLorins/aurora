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

#ifndef __AURORA_FOLLOWER_VIEW_H__
#define __AURORA_FOLLOWER_VIEW_H__

#include <memory>
#include <shared_mutex>

#include "common/comm_defs.h"
#include "common/comm_view.h"
#include "follower/memory_log_follower.h"
#include "follower/follower_bg_task.h"
#include "tools/trivial_lock_double_list.h"
#include "tools/trivial_lock_single_list.h"

namespace RaftCore::Follower {

using ::RaftCore::Common::CommonView;
using ::RaftCore::Follower::MemoryLogItemFollower;
using ::RaftCore::Follower::BackGroundTask::DisorderMessageContext;
using ::RaftCore::DataStructure::LockFreeUnorderedSingleList;
using ::RaftCore::DataStructure::SingleListNode;
using ::RaftCore::DataStructure::TrivialLockSingleList;
using ::RaftCore::DataStructure::DoubleListNode;
using ::RaftCore::DataStructure::TrivialLockDoubleList;

/*Note: This is the class for representing follower's state in follower's own view. */
class FollowerView final: public CommonView{

public:

    //Pending log entries waiting to be written into binlog file.
    static TrivialLockDoubleList<MemoryLogItemFollower>   m_phaseI_pending_list;

    //Pending log entries waiting to be stored.
    static TrivialLockDoubleList<MemoryLogItemFollower>   m_phaseII_pending_list;

    static LockFreeUnorderedSingleList<DoubleListNode<MemoryLogItemFollower>>     m_garbage;

    //Pending log entries waiting to be returned.
    static TrivialLockSingleList<DisorderMessageContext>   m_disorder_list;

    static LockFreeUnorderedSingleList<SingleListNode<DisorderMessageContext>>     m_disorder_garbage;

    //Threads whose log entries haven't been written are waiting on this CV
    static std::condition_variable m_cv;

    //Used together with the above CV
    static std::mutex       m_cv_mutex;

    static std::chrono::time_point<std::chrono::steady_clock>     m_last_heartbeat;

    static std::shared_timed_mutex      m_last_heartbeat_lock;

public:

    static void Initialize(bool switching_role=false) noexcept;

    static void UnInitialize() noexcept;

    static void Clear() noexcept;

private:

    FollowerView() = delete;

    virtual ~FollowerView() = delete;

    FollowerView(const FollowerView&) = delete;

    FollowerView& operator=(const FollowerView&) = delete;

};

} //end namespace

#endif
