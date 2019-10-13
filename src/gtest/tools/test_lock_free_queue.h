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

#ifndef __GTEST_LOCK_FREE_QUEUE_H__
#define __GTEST_LOCK_FREE_QUEUE_H__

#include <list>
#include <memory>
#include <chrono>
#include <random>

#include "gtest/tools/test_data_structure_base.h"
#include "tools/lock_free_queue.h"
#include "common/error_code.h"

using ::RaftCore::DataStructure::QueueNode;
using ::RaftCore::DataStructure::LockFreeQueue;

class TestLockFreeQueue : public DataStructureBase<LockFreeQueue,int> {

    public:

        TestLockFreeQueue(): DataStructureBase() {}

        virtual void SetUp() override {
            this->m_fn_cb = [](std::shared_ptr<int> ptr_element) ->bool{
                //std::cout << *ptr_element << " ";
                return true;
            };
            this->m_ds.Initilize(this->m_fn_cb,2 * 1024);
        }

        virtual void TearDown() override {
        }

    protected:

        virtual void Dump() override {
        }

        std::function<bool(std::shared_ptr<int> ptr_element)> m_fn_cb;

};

TEST_F(TestLockFreeQueue, GeneralOperation) {

    int _num_processed = 0;
    int  i = 0;
    bool _process_result = true;
    int _rst_val = 0;
    while (_process_result) {
        std::shared_ptr<int> _shp(new int(i++));
        _rst_val = this->m_ds.Push(&_shp);
        _process_result = _rst_val==QUEUE_SUCC;
        if (!_process_result)
            std::cout << "Push fail ,result:" << _rst_val << std::endl;
        _num_processed++;
    }

    int _original_size = this->m_ds.GetCapacity();
    ASSERT_EQ(_num_processed, _original_size);

    _num_processed = 0;
    _process_result = true;
    while (_process_result) {
        _rst_val = this->m_ds.PopConsume();
        _process_result = _rst_val==QUEUE_SUCC;
        if (!_process_result)
            std::cout << "Pop fail ,result:" << _rst_val << std::endl;
        _num_processed++;
    }

    ASSERT_EQ(_num_processed, _original_size);
    ASSERT_EQ(this->m_ds.GetSize(), 0);
    ASSERT_TRUE(this->m_ds.Empty());
}

TEST_F(TestLockFreeQueue, ConcurrentPush) {

    auto _insert = [&](int idx) {
        auto _tp = this->StartTimeing();

        int i = 0;
        bool _process_result = true;
        while (_process_result) {
            std::shared_ptr<int> _shp(new int(i++));
            int _rst_val = this->m_ds.Push(&_shp);
            _process_result = _rst_val==QUEUE_SUCC;
            if (!_process_result)
                std::cout << "Push fail ,result:" << _rst_val << std::endl;
        }

        this->EndTiming(_tp, "one thread inserting");
    };

    this->LaunchMultipleThread(_insert);

    std::cout << this->m_ds.GetSize() << "|" << this->m_ds.GetCapacity() - 1;

    ASSERT_EQ(this->m_ds.GetSize(), this->m_ds.GetCapacity()-1);
}

TEST_F(TestLockFreeQueue, ConcurrentPopConsume) {

    int i = 0;
    bool _process_result = true;
    while (_process_result) {
        std::shared_ptr<int> _shp(new int(i++));
        int _rst_val = this->m_ds.Push(&_shp);
        _process_result = _rst_val==QUEUE_SUCC;
        if (!_process_result)
            std::cout << "Push fail ,result:" << _rst_val << std::endl;
    }

    ASSERT_EQ(this->m_ds.GetSize(), this->m_ds.GetCapacity() -1);


    auto _pop = [&](int idx) {
        auto _tp = this->StartTimeing();

        bool _pop_rst = true;
        while (_pop_rst) {
            int _rst_val = this->m_ds.PopConsume();
            _pop_rst = _rst_val==QUEUE_SUCC;
            if (!_pop_rst)
                std::cout << "PopConsume fail ,result:" << _rst_val << std::endl;
        }

        this->EndTiming(_tp, "one thread inserting");
    };

    this->LaunchMultipleThread(_pop);
    ASSERT_EQ(this->m_ds.GetSize(), 0);
}

TEST_F(TestLockFreeQueue, ConcurrentPushPopConsume) {

    auto _push_pop = [&](int idx) {
        auto _tp = this->StartTimeing();

        int i = 0;
        bool _process_result = true;
        int _counter = 0;
        while (_process_result) {
            bool _only_push = false;

            _counter++;
            if (_counter > 10) {
                _counter = 0;
                _only_push = true;
            }

            std::shared_ptr<int> _shp(new int(i++));

            int _rst_val = this->m_ds.Push(&_shp);
            _process_result = _rst_val==QUEUE_SUCC;
            if (!_process_result) {
                std::cout << "Push fail ,result:" << _rst_val << std::endl;
                continue;
            }

            if (_only_push)
                continue;

            _rst_val = this->m_ds.PopConsume();
            _process_result = _rst_val==QUEUE_SUCC;
            if (!_process_result) {
                std::cout << "PopConsume fail ,result:" << _rst_val << std::endl;
                continue;
            }

            //std::cout << "------current size:" << this->m_ds.GetSize() << std::endl;
        }

        this->EndTiming(_tp, "one thread inserting");
    };

    this->LaunchMultipleThread(_push_pop);

    ASSERT_EQ(this->m_ds.GetSize(), this->m_ds.GetCapacity()-1);

}

TEST_F(TestLockFreeQueue, Cmp1) {
    std::shared_ptr<int> _shp(new int(7));

    for (int i = 0; i < 1000000; ++i) {
        this->m_ds.Push(&_shp);
        int _rst_val = this->m_ds.PopConsume();
        CHECK(_rst_val == QUEUE_SUCC);
    }
}

TEST_F(TestLockFreeQueue, Cmp2) {
    std::shared_ptr<int> _shp(new int(7));

    for (int i = 0; i < 1000000; ++i) {
        int* p = new int(7);
        delete p;
    }
}

#endif
