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

#include "common/comm_defs.h"
#include "common/error_code.h"
#include "config/config.h"
#include "tools/lock_free_priority_queue.h"

namespace RaftCore::DataStructure {

LockFreePriotityQueue::Task::Task(TaskType x,LockFreeQueueBase *y)noexcept{
    this->m_task_type = x;
    this->m_pc_queue.reset(y);
}

LockFreePriotityQueue::Task::Task(const Task& one) noexcept{
    this->m_task_type = one.m_task_type;
    /*To avoid compile errors under darwin clang,parameter of the copy-constructor must be
       const-qualified.  */
    Task &_real_one = const_cast<Task&>(one);
    this->m_pc_queue.swap(_real_one.m_pc_queue);
}

void LockFreePriotityQueue::Task::operator=(Task& one) noexcept{
    this->m_task_type = one.m_task_type;
    this->m_pc_queue.swap(one.m_pc_queue);
}

LockFreePriotityQueue::Task::~Task() {
    m_pc_queue.reset();
}

bool LockFreePriotityQueue::Task::operator<(const Task& _other) {
    return this->m_task_type < _other.m_task_type;
}

LockFreePriotityQueue::LockFreePriotityQueue() noexcept{}

LockFreePriotityQueue::~LockFreePriotityQueue() noexcept{}

void LockFreePriotityQueue::Initialize(int consumer_threads_num) noexcept {
    this->m_consumer_thread_num = consumer_threads_num;
    this->m_stop = false;
    this->m_running_thread_num.store(0);
}

void LockFreePriotityQueue::UnInitialize() noexcept {
    this->Stop();
    this->m_task_queue.clear();
}

void LockFreePriotityQueue::AddTask(TaskType _task_type, LockFreeQueueBase* _queue) noexcept {
    this->m_task_queue.emplace(std::piecewise_construct,
        std::forward_as_tuple((uint32_t)_task_type),
        std::forward_as_tuple(_task_type, _queue));
    //auto _iter = this->m_task_queue.begin();
}

int LockFreePriotityQueue::Push(TaskType _task_type,void* _shp_element) noexcept {
    uint32_t _task_type_uint = uint32_t(_task_type);
    auto _iter = this->m_task_queue.find(_task_type_uint);
    CHECK(_iter != this->m_task_queue.cend()) << ",task type:" << _task_type_uint;
    int _ret = _iter->second.m_pc_queue->Push(_shp_element);
    if (_ret == QUEUE_SUCC)
        this->m_cv.notify_one(); //It's not mandatory to hold the corresponding lock.

    return _ret;
}

void LockFreePriotityQueue::Launch() noexcept {
    for (int i = 0; i < this->m_consumer_thread_num; ++i) {
        std::thread* _p_thread = new std::thread(&LockFreePriotityQueue::ThreadEntrance,this);
        LOG(INFO) << "MCMP queue background thread :" << _p_thread->get_id() << " started";
        _p_thread->detach();
    }
}

void LockFreePriotityQueue::Stop() noexcept {
    this->m_stop = true;
    while (this->m_running_thread_num.load() != 0)
        std::this_thread::sleep_for(std::chrono::microseconds(::RaftCore::Config::FLAGS_thread_stop_waiting_us));
}

uint32_t LockFreePriotityQueue::GetSize() const noexcept {
    uint32_t _sum = 0;
    for (const auto &_item : this->m_task_queue)
        _sum += _item.second.m_pc_queue->GetSize();

    return _sum;
}

void LockFreePriotityQueue::ThreadEntrance() noexcept {

    CHECK(this->m_task_queue.size() > 0);

    this->m_running_thread_num.fetch_add(1);

    auto _wait_us = std::chrono::milliseconds(::RaftCore::Config::FLAGS_lockfree_queue_consumer_wait_ms);

    auto _cond_data_arrived = [&]()->bool{
        for (auto _it = this->m_task_queue.cbegin(); _it != this->m_task_queue.cend(); _it++)
            if (!_it->second.m_pc_queue->Empty())
                return true;

        return false;
    };

    std::unique_lock<std::mutex>  _unique_wrapper(this->m_cv_mutex, std::defer_lock);

    while (true) {
        //To detect somewhere else want the consuming threads to end.
        if (this->m_stop)
            break;

        /*Trade-off: there is a small windows during which we would lose messages, in that case,
           we'll wait until timeout reach.  */
        _unique_wrapper.lock();
        bool _job_comes = this->m_cv.wait_for(_unique_wrapper, _wait_us, _cond_data_arrived);
        _unique_wrapper.unlock();

        if (!_job_comes)
            continue;

        //Start from begin.
        auto _iter = this->m_task_queue.begin();

        //Drain all queues.
        while(true){
            if (_iter->second.m_pc_queue->PopConsume() == QUEUE_SUCC)
                continue;

            // Queue is empty or PopConsume fail.
            _iter++;

            //If there are no more tasks, quit current loop.
            if (_iter == this->m_task_queue.end())
                break;
        }
    }

    this->m_running_thread_num.fetch_sub(1);
}

}
