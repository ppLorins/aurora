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

#ifndef __AURORA_TRIVIAL_LOCK_ORDERED_SINGLE_LIST_H__
#define __AURORA_TRIVIAL_LOCK_ORDERED_SINGLE_LIST_H__

#include <memory>
#include <atomic>
#include <type_traits>
#include <mutex>
#include <functional>

#include "glog/logging.h"

#include "common/macro_manager.h"
#include "tools/data_structure_base.h"
#include "tools/trivial_lock_list_base.h"

namespace RaftCore::DataStructure {

template <typename T>
class SingleListNode final : public OrderedTypeBase<SingleListNode<T>>, public LogicalDelete<void> {

public:

    SingleListNode(const std::shared_ptr<T> &shp_val) noexcept;

    virtual ~SingleListNode() noexcept;

    virtual bool operator<(const SingleListNode& other)const noexcept override;

    virtual bool operator>(const SingleListNode& other)const noexcept override;

    virtual bool operator==(const SingleListNode& other)const noexcept override;

    static void Apply(SingleListNode<T>* phead, std::function<void(SingleListNode<T>*)> unary) noexcept;

    std::shared_ptr<T>    m_val;

    std::atomic<SingleListNode<T>*>    m_atomic_next;
};

template <typename T>
class TrivialLockSingleList final : public OperationTracker<SingleListNode<T>> {

public:

    TrivialLockSingleList(const std::shared_ptr<T> &p_min, const std::shared_ptr<T> &p_max) noexcept;

    virtual ~TrivialLockSingleList() noexcept;

    void Insert(const std::shared_ptr<T> &p_one) noexcept;

    void Insert(SingleListNode<T>* new_node) noexcept;

    /*Note : Delete & CutHead are not intended to be invoked simultaneously.  */
    bool Delete(const std::shared_ptr<T> &p_one) noexcept;

    SingleListNode<T>* CutHead(std::function<bool(const T &one)> criteria) noexcept;

    SingleListNode<T>* CutHeadByValue(const T &val) noexcept;

    static void ReleaseCutHead(SingleListNode<T>* output_head) noexcept;

    //This method is not thread safe , but no way to call it simultaneously.
    void Clear() noexcept;

    SingleListNode<T>* SetEmpty() noexcept;

    void IterateCutHead(std::function<bool(std::shared_ptr<T> &)> accessor, SingleListNode<T>* output_head) const noexcept;

    void Iterate(std::function<bool(std::shared_ptr<T> &)> accessor) const noexcept;

    bool Empty() const noexcept;

#ifdef _SINGLE_LIST_TEST_
    int GetSize() const noexcept;

    SingleListNode<T>* GetHead() const noexcept;
#endif

private:

    void InsertTracker(SingleListNode<T>* new_node) noexcept;

    bool InsertRaw(SingleListNode<T>* new_node) noexcept;

    void SiftOutDeleted(SingleListNode<T>* &output_head) noexcept;

    SingleListNode<T>*     m_head;

    //Used for indicating a cut head list.
    SingleListNode<T>*     m_tail;

    std::recursive_mutex    m_recursive_mutex;

private:

    TrivialLockSingleList& operator=(const TrivialLockSingleList&) = delete;

};

} //end namespace

#include "tools/trivial_lock_single_list.cc"

#endif
