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

#ifndef __AURORA_REACT_GROUP_H__
#define __AURORA_REACT_GROUP_H__

#include <memory>
#include <thread>
#include <functional>

#include "grpc++/completion_queue.h"

#include "common/react_base.h"

namespace RaftCore::Common {

using ::grpc::CompletionQueue;
using ::grpc::ServerCompletionQueue;

template<typename T=ServerCompletionQueue>
using TypePtrCQ = std::shared_ptr<T>;

//On a one CQ <---> multiple threads basis.
template<typename T=ServerCompletionQueue>
class ReactWorkGroup {

public:

    enum class CQType { ServerCQ = 2, GENERAL_CQ };

public:

    ReactWorkGroup(TypePtrCQ<T> shp_cq, TypeReactorFunc reactor, int therad_num)noexcept;

    virtual ~ReactWorkGroup();

    void StartPolling() noexcept;

    void WaitPolling() noexcept;

    TypePtrCQ<T> GetCQ() noexcept;

    void ShutDownCQ() noexcept;

    void GetThreadId(std::vector<std::thread::id> &ids) noexcept;

private:

    void GrpcPollingThread() noexcept;

    TypePtrCQ<T>    m_shp_cq;

    std::vector<std::thread*>   m_vec_threads;

    TypeReactorFunc     m_reactor;

    int     m_polling_threads_num = 0;
};

} //end namespace

#include "common/react_group.cc"

#endif
