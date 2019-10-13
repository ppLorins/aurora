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

#ifndef __GTEST_LOCK_FREE_PRIORITY_QUEUE_H__
#define __GTEST_LOCK_FREE_PRIORITY_QUEUE_H__

#include <list>
#include <memory>
#include <chrono>

#include "tools/lock_free_priority_queue.h"

using ::RaftCore::DataStructure::LockFreeQueue;
using ::RaftCore::DataStructure::LockFreeQueueBase;
using ::RaftCore::DataStructure::LockFreePriotityQueue;

class TestLockFreePriorityQueue : public DataStructureBase<LockFreeQueue,int>  {

    public:

        struct STask1 {
            STask1(int i):m_i(i) {}

            static bool Func1(std::shared_ptr<STask1> ptr_element) {
                //std::cout << "Task1 got:" << ptr_element->m_i << std::endl;
                return true;
            }

            int m_i;
        };

        struct STask2 {
            STask2(int i):m_i(i) {}

            static bool Func2(std::shared_ptr<STask2> ptr_element) {
                //std::cout << "Task2 got:" << ptr_element->m_i << std::endl;
                return true;
            }

            int m_i;
        };

        struct STask3 {
            STask3(int i):m_i(i) {}

            static bool Func3(std::shared_ptr<STask3> ptr_element) {
                //std::cout << "Task3 got:" << ptr_element->m_i << std::endl;
                return true;
            }

            int m_i;
        };

    public:

        TestLockFreePriorityQueue() {}
        virtual ~TestLockFreePriorityQueue() {}

        virtual void SetUp() override {

            this->m_pri_queue.Initialize(this->m_cpu_cores * 2);
            //this->m_pri_queue.Initialize(1);

            auto _p_queue_2 = new LockFreeQueue<STask2>();
            _p_queue_2->Initilize(STask2::Func2,::RaftCore::Config::FLAGS_lockfree_queue_resync_data_elements);
            this->m_pri_queue.AddTask(LockFreePriotityQueue::TaskType::RESYNC_DATA,(LockFreeQueueBase*)_p_queue_2);

            auto _p_queue_3 = new LockFreeQueue<STask3>();
            _p_queue_3->Initilize(STask3::Func3,::RaftCore::Config::FLAGS_lockfree_queue_resync_log_elements);
            this->m_pri_queue.AddTask(LockFreePriotityQueue::TaskType::RESYNC_LOG,(LockFreeQueueBase*)_p_queue_3);

        }

        virtual void TearDown() override {}

        virtual void Dump() override {}

    protected:

        LockFreePriotityQueue   m_pri_queue;
};

TEST_F(TestLockFreePriorityQueue, GeneralOperation) {

    this->m_pri_queue.Launch();

    std::this_thread::sleep_for(std::chrono::milliseconds(600));

    std::shared_ptr<STask3> _shp_t3_1(new STask3(1));
    int _rst_val = this->m_pri_queue.Push(LockFreePriotityQueue::TaskType::RESYNC_LOG,&_shp_t3_1);
    if (_rst_val==QUEUE_SUCC)
        std::cout << "T3 Push fail ,result:" << _rst_val << std::endl;

    std::shared_ptr<STask3> _shp_t3_2(new STask3(2));
    _rst_val = this->m_pri_queue.Push(LockFreePriotityQueue::TaskType::RESYNC_LOG,&_shp_t3_2);
    if (_rst_val==QUEUE_SUCC)
        std::cout << "T3 Push fail ,result:" << _rst_val << std::endl;

    std::shared_ptr<STask2> _shp_t2_1(new STask2(10));
    _rst_val = this->m_pri_queue.Push(LockFreePriotityQueue::TaskType::RESYNC_LOG,&_shp_t2_1);
    if (_rst_val==QUEUE_SUCC)
        std::cout << "T2 Push fail ,result:" << _rst_val << std::endl;

    std::shared_ptr<STask2> _shp_t2_2(new STask2(11));
    _rst_val = this->m_pri_queue.Push(LockFreePriotityQueue::TaskType::RESYNC_LOG,&_shp_t2_2);
    if (_rst_val==QUEUE_SUCC)
        std::cout << "T2 Push fail ,result:" << _rst_val << std::endl;

    std::cout << "wait a little while";
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    this->m_pri_queue.UnInitialize();
}

TEST_F(TestLockFreePriorityQueue, ConcurrentOperation) {

    this->m_pri_queue.Launch();

    //Pushing.
    auto _push = [&](int idx) {
        auto _tp = this->StartTimeing();

        int i = 0;
        bool _process_result = true;
        int _counter = 0, _run_times = 50000;
        int _rst_val = 0;
        while (_process_result && _run_times>=0) {

            _counter++;
            _run_times--;

            if (_counter>=30) {
                _counter = 0;
                continue;
            }

            std::shared_ptr<STask3> _shp_t3(new STask3(i++));
            _rst_val = this->m_pri_queue.Push(LockFreePriotityQueue::TaskType::RESYNC_LOG,&_shp_t3);
            _process_result = _rst_val==QUEUE_SUCC;
            if (!_process_result) {
                std::cout << "T3 Push fail ,result:" << _rst_val << std::endl;
                continue;
            }

            if (_counter>=20)
                continue;

            std::shared_ptr<STask2> _shp_t2(new STask2(i++));
            _rst_val = this->m_pri_queue.Push(LockFreePriotityQueue::TaskType::RESYNC_DATA,&_shp_t2);
            _process_result = _rst_val==QUEUE_SUCC;
            if (!_process_result) {
                std::cout << "T2 Push fail ,result:" << _rst_val << std::endl;
                continue;
            }

            this->m_pri_queue.GetSize();

            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }

        this->EndTiming(_tp,"one thread inserting");
    };

    this->LaunchMultipleThread(_push);

    this->m_pri_queue.UnInitialize();
}




#endif
