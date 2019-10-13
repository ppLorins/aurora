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

#ifndef __AURORA_CONNECTION_POOL_EX_H__
#define __AURORA_CONNECTION_POOL_EX_H__

#include <memory>
#include <unordered_map>

#include "grpc/grpc.h"
#include "grpc++/grpc++.h"

#include "protocol/raft.pb.h"
#include "protocol/raft.grpc.pb.h"

#include "tools/lock_free_deque.h"
#include "leader/channel_pool.h"

namespace RaftCore::Leader {

using ::RaftCore::DataStructure::LockFreeDeque;
using ::RaftCore::Leader::ChannelPool;

class FollowerEntity;

template<typename T>
class ClientPool final{

public:

    ClientPool(FollowerEntity* p_follower = nullptr) noexcept;

    virtual ~ClientPool() noexcept;

    std::shared_ptr<T> Fetch() noexcept;

    void Back(std::shared_ptr<T> &client) noexcept;

    FollowerEntity* GetParentFollower() noexcept;

private:

    LockFreeDeque<T>     m_pool;

    /*Cannot contain a shared_ptr<FollowerEntity> since it will cause two shared_ptr points to the same
      the FollowerEntity object resulting in a recursively destructing problem. */
    FollowerEntity*     m_p_parent_follower;

private:

    ClientPool(const ClientPool&) = delete;

    ClientPool& operator=(const ClientPool&) = delete;
};

} //end namespace

#include "leader/client_pool.cc"

#endif
