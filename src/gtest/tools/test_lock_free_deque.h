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

#ifndef __GTEST_LOCK_FREE_DEQUE_H__
#define __GTEST_LOCK_FREE_DEQUE_H__


#include "gtest/tools/test_data_structure_base.h"
#include "tools/lock_free_deque.h"

using ::RaftCore::DataStructure::LockFreeDeque;

class TestLockFreeDeque : public DataStructureBase<LockFreeDeque,int> {

    protected:

        virtual void SetUp() override {

            //Install GC.
            std::thread _t([&]() {
                while (true) {
                    if (!this->m_running)
                        break;

                    LockFreeDeque<int>::GC();
                }
            });
            _t.detach();
        }

        virtual void TearDown() override {
            this->m_running = false;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        virtual void Dump() override {
            while (auto shp = this->m_ds.Pop()) {
                std::cout << *shp << " ";
            }
        }

        bool m_running = true;
};

TEST_F(TestLockFreeDeque, GeneralOperation) {

    int val = 7;
    std::shared_ptr<int> _shp(new int(val));
    this->m_ds.Push(_shp);

    ASSERT_EQ(this->m_ds.Size(), 1);

    decltype(_shp) _out = this->m_ds.Pop();
    ASSERT_EQ(*_out, val);

    _out = this->m_ds.Pop();
    ASSERT_TRUE(!_out);

    //simulate a bug scenario.
    int _count = 2;
    for (int i = 0; i < _count;++i)
        this->m_ds.Push(std::make_shared<int>(i));

    //simulate a bug scenario.
    while (auto shp = this->m_ds.Pop());
    ASSERT_EQ(this->m_ds.Size(),0);

    for (int i = 0; i < _count;++i)
        this->m_ds.Push(std::make_shared<int>(i));

    while (auto shp = this->m_ds.Pop());
    ASSERT_EQ(this->m_ds.Size(),0);

}

TEST_F(TestLockFreeDeque, ConcurrentPop) {

    int _count = ::RaftCore::Config::FLAGS_deque_push_count;

    for (int i = 0; i < _count;++i)
        this->m_ds.Push(std::make_shared<int>(i));

    //std::cout << "push done,sleeping..." << std::endl;;

    //std::this_thread::sleep_for(std::chrono::seconds(5));

    auto _pop_it = [&](int idx){
        while (auto shp = this->m_ds.Pop());
            //std::cout <<  "got ";
        std::cout << "thread pop end " << std::this_thread::get_id() << std::endl;
    };

    this->LaunchMultipleThread(_pop_it);

    std::cout << "pop done,sleeping --debug size:" << this->m_ds.GetSizeByIterating() << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(10));

    ASSERT_EQ(this->m_ds.GetLogicalSize(),0);
}

TEST_F(TestLockFreeDeque, ConcurrentPush) {

    int _count = 10000;
    std::shared_ptr<int> _shp(new int(7));
    auto _push_it = [&](int idx){
        for (int i = 0; i < _count;++i) {
            //std::cout << "thread : " << std::this_thread::get_id() << " is pushing" << std::endl;
            this->m_ds.Push(_shp);
        }
    };

    this->LaunchMultipleThread(_push_it);

    ASSERT_EQ(this->m_ds.Size(),this->m_cpu_cores * _count);
}

TEST_F(TestLockFreeDeque, ConcurrentPushPop) {

    int _count = 100;
    for (int i = 0; i < _count;++i)
        this->m_ds.Push(std::make_shared<int>(i));

    auto _do_it = [&](int idx){
        int _round = 100000;
        for (int i = 0; i < _round;++i) {
            auto shp = this->m_ds.Pop();
            if (!shp) {
                std::cout << "thread:" << std::this_thread::get_id() << " pop empty"
                    << ",size:" << this->m_ds.Size() << ",i:" << i << std::endl;
                continue;
            }

            auto x = *shp;
            *shp = _count + x + 1;
            this->m_ds.Push(shp);
        }
    };

    this->LaunchMultipleThread(_do_it);

    std::cout << "--debug size:" << this->m_ds.GetSizeByIterating() << std::endl;

    ASSERT_EQ(this->m_ds.Size(),_count);
}


#endif
