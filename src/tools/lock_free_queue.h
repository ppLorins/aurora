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

#ifndef __AURORA_TRIVIAL_LOCK_QUEUE_H__
#define __AURORA_TRIVIAL_LOCK_QUEUE_H__

#include <memory>
#include <atomic>
#include <functional>

namespace RaftCore::DataStructure {

enum class SlotState {
    /*--------------Node State--------------*/
    SLOT_EMPTY,
    SLOT_PRODUCING,
    SLOT_PRODUCED,
    SLOT_CONSUMING
};


template <typename T>
class QueueNode final{

public:

    QueueNode() noexcept;

    virtual ~QueueNode() noexcept;

    std::shared_ptr<T>    m_val;

    std::atomic<SlotState>     m_state;

    inline static const char* MacroToString(SlotState enum_val) {
        return m_status_macro_names[int(enum_val)];
    }

private:

    static const char*  m_status_macro_names[];

};

/*Note : LockFreeQueueBase is a wrapper aimed at eliminating specifying the template parameters needed by the invokers
when they call LockFreeQueue methods.  */
class LockFreeQueueBase  {

public:

    LockFreeQueueBase(){}

    virtual ~LockFreeQueueBase(){}

    virtual int Push(void* ptr_shp_element) noexcept = 0;

    virtual int PopConsume() noexcept = 0;

    virtual uint32_t GetSize() const noexcept = 0;

    virtual uint32_t GetCapacity() const noexcept = 0;

    virtual bool Empty() const noexcept = 0;
};


//The following is a ring-buf supported multi-thread producing and multi-thread consuming
template <typename T>
class LockFreeQueue final : public LockFreeQueueBase {

public:

    typedef std::function<bool(std::shared_ptr<T> &ptr_element)> TypeCallBackFunc;

    LockFreeQueue() noexcept;

    void Initilize(TypeCallBackFunc  fn_cb,int queue_size) noexcept;

    virtual ~LockFreeQueue() noexcept;

    virtual int Push(void* ptr_shp_element) noexcept override;

    virtual int PopConsume() noexcept override;

    //Get a snapshot size.
    virtual uint32_t GetSize() const noexcept override;

    //For gtest usage.
    virtual uint32_t GetCapacity() const noexcept override;

    virtual bool Empty() const noexcept override;

private:

    int Pop(std::shared_ptr<T> &ptr_element) noexcept;

private:

    //Position where holds the latest produced element.
    std::atomic<uint32_t>   m_head;

    //Position which just before the earliest produced element.If empty (m_head == m_tail).
    std::atomic<uint32_t>   m_tail;

    /*Note :In the current design, there will always be at least one slot empty , to simplify
      the implementation.  */
    QueueNode<T>   *m_data = nullptr; //Data ring buffer.

    TypeCallBackFunc   m_fn_cb;

    uint32_t m_element_size = 0;

    uint32_t m_element_mask = 0;

private:

    LockFreeQueue& operator=(const LockFreeQueue&) = delete;

    LockFreeQueue(const LockFreeQueue&) = delete;

};

} //end namespace

#include "tools/lock_free_queue.cc"

#endif
