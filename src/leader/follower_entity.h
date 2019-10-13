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

#ifndef __AURORA_FOLLOWER_STATE_H__
#define __AURORA_FOLLOWER_STATE_H__

#include <memory>
#include <atomic>

#include "common/comm_view.h"
#include "common/comm_defs.h"
#include "client/client_impl.h"
#include "leader/client_pool.h"

namespace RaftCore::Leader {

using ::RaftCore::Common::LogIdentifier;
using ::RaftCore::Member::JointConsensusMask;
using ::RaftCore::Client::AppendEntriesAsyncClient;
using ::RaftCore::Client::CommitEntriesAsyncClient;
using ::RaftCore::Leader::ChannelPool;
using ::RaftCore::Leader::ClientPool;

enum class FollowerStatus {
    NORMAL = 0,
    RESYNC_LOG,
    RESYNC_DATA,
};

/* This is the class for representing follower's state in leader's view */
class FollowerEntity final{

public:

    inline static const char* MacroToString(FollowerStatus enum_val) {
        return m_status_macro_names[int(enum_val)];
    }

public:

    FollowerEntity(const std::string &follower_addr,FollowerStatus status=FollowerStatus::NORMAL,
                    uint32_t joint_consensus_flag=uint32_t(JointConsensusMask::IN_OLD_CLUSTER)) noexcept;

    virtual ~FollowerEntity() noexcept;

    //If ever successfully updated the last sent committed for this follower.
    bool UpdateLastSentCommitted(const LogIdentifier &to) noexcept;

    std::string     my_addr;

    //A simple type , can be read & blind write simultaneously by multiple thread.
    FollowerStatus  m_status;

    uint32_t        m_joint_consensus_flag;

    //Record #(timeout entries) since the latest successfully replicated log.
    int32_t         m_timeout_counter;

    std::shared_ptr<ChannelPool>    m_shp_channel_pool;

    //Note: only high frequency used client need to be pooled.
    std::unique_ptr<ClientPool<AppendEntriesAsyncClient>>     m_append_client_pool;

    std::unique_ptr<ClientPool<CommitEntriesAsyncClient>>     m_commit_client_pool;

    std::atomic<LogIdentifier>      m_last_sent_committed;

private:

    static const char*  m_status_macro_names[];

private:

    FollowerEntity(const FollowerEntity&) = delete;

    FollowerEntity& operator=(const FollowerEntity&) = delete;

};

typedef std::shared_ptr<::RaftCore::Leader::FollowerEntity> TypePtrFollowerEntity;

} //end namespace

#endif
