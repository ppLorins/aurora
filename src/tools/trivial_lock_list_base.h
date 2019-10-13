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

#ifndef __AURORA_TRIVIAL_LOCK_LIST_BASE_H__
#define __AURORA_TRIVIAL_LOCK_LIST_BASE_H__

#include <thread>
#include <atomic>

#include "tools/data_structure_base.h"
#include "tools/lock_free_hash_specific.h"

namespace RaftCore::DataStructure {

using ::RaftCore::DataStructure::HashTypeBase;
using ::RaftCore::DataStructure::HashNode;
using ::RaftCore::DataStructure::LockFreeHashAtomic;

//The template is an wrapper for compile compatibility.
template<typename T=void>
class ThreadIDWrapper final : public HashTypeBase<ThreadIDWrapper<T>> {

public:

    ThreadIDWrapper(std::thread::id tid)noexcept;

    virtual ~ThreadIDWrapper()noexcept;

    virtual bool operator<(const ThreadIDWrapper&)const noexcept override;

    virtual bool operator==(const ThreadIDWrapper&)const noexcept override;

    virtual const ThreadIDWrapper& operator=(const ThreadIDWrapper&other)noexcept override
    {
        this->m_tid = other.m_tid;
        return *this;
    }

    virtual std::size_t Hash() const noexcept override;

    std::thread::id GetTid() const noexcept;

private:

    std::thread::id    m_tid;

private:

    ThreadIDWrapper(const ThreadIDWrapper&) = delete;
};

template<typename T>
class OperationTracker {

public:

    OperationTracker()noexcept;

    virtual ~OperationTracker()noexcept;

protected:

    void WaitForListClean(T* output_head) noexcept;

protected:

    LockFreeHashAtomic<ThreadIDWrapper<void>, T>    *m_p_insert_footprint = nullptr;

private:

    OperationTracker(const OperationTracker&) = delete;

    OperationTracker& operator=(const OperationTracker&) = delete;
};

} //end namespace

#include "tools/trivial_lock_list_base.cc"

#endif
