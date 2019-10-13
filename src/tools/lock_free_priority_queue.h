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

#ifndef __AURORA_LOCK_FREE_PRIORITY_QUEUE_H__
#define __AURORA_LOCK_FREE_PRIORITY_QUEUE_H__

#include <memory>
#include <map>
#include <thread>
#include <chrono>
#include <set>

#include "tools/lock_free_queue.h"

namespace RaftCore::DataStructure {

class LockFreePriotityQueue final{

public:

    enum class TaskType {
        /*The sequence of declarations also defines the priority from highest to lowest.*/
        CLIENT_REACTING = 0, //Enum value also indicated the index in the task  array
        RESYNC_DATA,
        RESYNC_LOG,
    };

    struct Task {

        Task(TaskType x, LockFreeQueueBase *y) noexcept;

        Task(const Task& one) noexcept;

        void operator=(Task& one) noexcept;

        virtual ~Task();

        bool operator<(const Task& _other);

        TaskType                              m_task_type;

        std::unique_ptr<LockFreeQueueBase>    m_pc_queue;
    };

public:

    LockFreePriotityQueue() noexcept;

    virtual ~LockFreePriotityQueue() noexcept;

    void Initialize(int consumer_threads_num) noexcept;

    void UnInitialize() noexcept;

    /*Note: 1.AddTask is not thread safe, only invoke this method during server initialization.
            2.Order of calling AddTask should be the same with the order of _task_type parameter defined
              in the 'TaskType'.This constrain is not reasonable and should be optimized off in the future.  */
    void AddTask(TaskType _task_type, LockFreeQueueBase* _queue) noexcept;

    /*_shp_element: pointer of std::shared_ptr<> pointing to the element to be inserted.
      The shared_ptr object's ownership is guaranteed to be increased.  */
    int Push(TaskType _task_type,void* _shp_element) noexcept;

    void Launch() noexcept;

    uint32_t GetSize() const noexcept;

private:

    void Stop() noexcept;

    void ThreadEntrance() noexcept;

private:

    std::condition_variable     m_cv;

    std::mutex                  m_cv_mutex;

    int                         m_consumer_thread_num=0;

    std::atomic<int>            m_running_thread_num;

    volatile bool               m_stop = false;

    std::map<uint32_t,Task>     m_task_queue;

private:

    LockFreePriotityQueue& operator=(const LockFreePriotityQueue&) = delete;

    LockFreePriotityQueue(const LockFreePriotityQueue&) = delete;

};

} //end namespace

#endif
