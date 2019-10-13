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

#ifndef __AURORA_TRIVIAL_LOCK_LIST_H__
#define __AURORA_TRIVIAL_LOCK_LIST_H__

#include <memory>
#include <atomic>
#include <type_traits>
#include <mutex>
#include <functional>

#include "glog/logging.h"
#include "tools/data_structure_base.h"
#include "tools/trivial_lock_list_base.h"

namespace RaftCore::DataStructure {

template <typename T>
class DoubleListNode final : public OrderedTypeBase<DoubleListNode<T>>, public LogicalDelete<void> {

public:

    DoubleListNode(const std::shared_ptr<T> &p_val) noexcept;

    virtual ~DoubleListNode() noexcept;

    virtual bool operator<(const DoubleListNode& other)const noexcept override;

    virtual bool operator>(const DoubleListNode& other)const noexcept override;

    virtual bool operator==(const DoubleListNode& other)const noexcept override;

    //There are several lock-free operations base on std::atomic::CAS
    std::atomic<DoubleListNode<T>*>   m_atomic_pre;

    std::atomic<DoubleListNode<T>*>   m_atomic_next;

    std::shared_ptr<T>    m_val;

    static void Apply(DoubleListNode<T>* phead, std::function<void(DoubleListNode<T>*)> unary) noexcept;
};

template <typename T>
class TrivialLockDoubleList final : OperationTracker<DoubleListNode<T>> {

public:

    TrivialLockDoubleList(const std::shared_ptr<T> &p_min, const std::shared_ptr<T> &p_max) noexcept;

    virtual ~TrivialLockDoubleList() noexcept;

    void Insert(const std::shared_ptr<T> &p_one) noexcept;

    void Insert(DoubleListNode<T>* new_node) noexcept;

    /*Note : Delete & CutHead are not intended to be invoked simultaneously.  */
    bool Delete(const std::shared_ptr<T> &p_one) noexcept;

    void DeleteAll() noexcept;

    //1. Each pair of the adjacent elements satisfy criteria: cut them all.
    //2. otherwise, cut the satisfied elements.
    DoubleListNode<T>* CutHead(std::function<bool(const T &left, const T &right)> criteria) noexcept;

    DoubleListNode<T>* CutHead(std::function<bool(const T &one)> criteria) noexcept;

    DoubleListNode<T>* CutHeadByValue(const T &val) noexcept;

    static void ReleaseCutHead(DoubleListNode<T>* output_head) noexcept;

    //This method is not thread safe , but no way to call it simultaneously.
    void Clear() noexcept;

    void IterateCutHead(std::function<bool(T &)> accessor, DoubleListNode<T>* output_head) const noexcept;

    void Iterate(std::function<bool(T &)> accessor) const noexcept;

    bool Empty() const noexcept;

#ifdef _TRIIAL_DOUBLE_LIST_TEST_
    int GetSize() const noexcept;

    DoubleListNode<T>* GetHead() const noexcept;
#endif

private:

    void InsertTracker(DoubleListNode<T>* new_node) noexcept;

    /* Self-purging redundant */
    bool InsertRaw(DoubleListNode<T>* new_node) noexcept;

    DoubleListNode<T>* FindNextNonDelete(DoubleListNode<T>* p_cur) noexcept;

    DoubleListNode<T>* ExpandForward(DoubleListNode<T>* p_cur) noexcept;

    DoubleListNode<T>* ExpandBackward(DoubleListNode<T>* p_cur) noexcept;

    bool MoveForward(DoubleListNode<T>* &p_pre,DoubleListNode<T>* &p_next) noexcept;

    void SiftOutDeleted(DoubleListNode<T>* &output_head) noexcept;

    DoubleListNode<T>*   m_head = nullptr;

    DoubleListNode<T>*   m_tail = nullptr;

    std::recursive_mutex     m_recursive_mutex;

private:

    TrivialLockDoubleList& operator=(const TrivialLockDoubleList&) = delete;

};

} //end namespace

#include "tools/trivial_lock_double_list.cc"

#endif
