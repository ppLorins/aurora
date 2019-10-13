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

#ifndef __AURORA_LOCK_FREE_UNORDERED_SINGLE_LIST_H__
#define __AURORA_LOCK_FREE_UNORDERED_SINGLE_LIST_H__

#include <atomic>
#include <functional>

#include "common/macro_manager.h"

namespace RaftCore::DataStructure {

template <typename T>
class UnorderedSingleListNode final{

public:

    template<typename ...Args>
    UnorderedSingleListNode(Args&&... args) noexcept;

    virtual ~UnorderedSingleListNode()noexcept;

    explicit UnorderedSingleListNode(T* p_src)noexcept;

    T*   m_data;
    std::atomic<UnorderedSingleListNode<T>*>    m_next;
};


template<typename T>
using AtomicPtrSingleListNode = std::atomic<UnorderedSingleListNode<T>*>;

template <typename T>
class LockFreeUnorderedSingleList final{

public:

    LockFreeUnorderedSingleList() noexcept;

    virtual ~LockFreeUnorderedSingleList() noexcept;

    void SetDeleter(std::function<void(T*)> deleter)noexcept;

    //Will take the ownership of 'src'.
    void PushFront(T* src) noexcept;

    void PurgeSingleList(uint32_t retain_num) noexcept;

#ifdef _UNORDERED_SINGLE_LIST_TEST_
    uint32_t Size() noexcept;

    void Iterate(std::function<void(T*)> func) noexcept;
#endif

private:

    std::atomic<UnorderedSingleListNode<T>*>     m_head;

    std::function<void(T*)>     m_deleter = [](T* data) { delete data; };

private:

    LockFreeUnorderedSingleList(const LockFreeUnorderedSingleList&) = delete;

    LockFreeUnorderedSingleList& operator=(const LockFreeUnorderedSingleList&) = delete;

};

} //end namespace

#include "tools/lock_free_unordered_single_list.cc"

#endif
