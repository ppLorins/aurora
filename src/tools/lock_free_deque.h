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

#ifndef __AURORA_LOCK_FREE_DEQUE_H__
#define __AURORA_LOCK_FREE_DEQUE_H__

#include <memory>
#include <atomic>
#include <shared_mutex>

#include "common/comm_defs.h"
#include "tools/lock_free_unordered_single_list.h"

namespace RaftCore::DataStructure {

using ::RaftCore::DataStructure::AtomicPtrSingleListNode;
using ::RaftCore::DataStructure::UnorderedSingleListNode;
using ::RaftCore::DataStructure::LockFreeUnorderedSingleList;

template <typename T>
class DequeNode final{

public:

    DequeNode() noexcept; //For dumb nodes

    DequeNode(const std::shared_ptr<T> &p_val) noexcept;

    virtual ~DequeNode() noexcept;

    std::atomic<DequeNode<T>*>   m_atomic_next;

    std::shared_ptr<T>    m_val;

    /* 0: normal node.
       1: fake node.  */
    uint32_t   m_flag = 0;
};

template <typename T>
class LockFreeDeque final{

public:

    LockFreeDeque() noexcept;

    virtual ~LockFreeDeque() noexcept;

    void Push(const std::shared_ptr<T> &p_one, uint32_t flag = 0) noexcept;

    std::shared_ptr<T> Pop() noexcept;

#ifdef _DEQUE_TEST_
    std::size_t GetSizeByIterating() const noexcept;

    std::size_t GetLogicalSize() const noexcept;

    std::size_t GetPhysicalSize() const noexcept;

    std::size_t Size() const noexcept;
#endif

    static void GC() noexcept;

private:

    DequeNode<T>* PopNode() noexcept;

private:

    std::atomic<DequeNode<T>*>   m_head;

    std::atomic<DequeNode<T>*>   m_tail;

    DequeNode<T>*   m_dummy = nullptr;

#ifdef _DEQUE_TEST_
    std::atomic<uint32_t>   m_logical_size;

    std::atomic<uint32_t>   m_physical_size;
#endif

    static LockFreeUnorderedSingleList<DequeNode<T>>  m_garbage;

private:

    LockFreeDeque& operator=(const LockFreeDeque&) = delete;

};

} //end namespace

#include "tools/lock_free_deque.cc"

#endif
