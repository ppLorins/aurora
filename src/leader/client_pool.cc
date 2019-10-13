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

#include <typeinfo>

#include "grpc++/create_channel.h"

#include "config/config.h"
#include "common/comm_defs.h"
#include "global/global_env.h"
#include "client/client_impl.h"
#include "leader/client_pool.h"

namespace RaftCore::Leader {

using ::RaftCore::Global::GlobalEnv;
using ::RaftCore::Client::HeartbeatSyncClient;

template<typename T>
ClientPool<T>::ClientPool(FollowerEntity* p_follower) noexcept {
    this->m_p_parent_follower = p_follower;
}

template<typename T>
ClientPool<T>::~ClientPool() noexcept{}

template<typename T>
FollowerEntity* ClientPool<T>::GetParentFollower() noexcept {
    return this->m_p_parent_follower;
}

template<typename T>
std::shared_ptr<T> ClientPool<T>::Fetch() noexcept {
    return m_pool.Pop();
}

template<typename T>
void ClientPool<T>::Back(std::shared_ptr<T> &client) noexcept {
    return m_pool.Push(client);
}

}

