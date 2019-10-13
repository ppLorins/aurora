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

#ifndef __GTEST_LOCK_FREE_UNORDERED_jSINGLE_LIST_H__
#define __GTEST_LOCK_FREE_UNORDERED_jSINGLE_LIST_H__


#include "gtest/tools/test_data_structure_base.h"
#include "tools/lock_free_unordered_single_list.h"

using ::RaftCore::DataStructure::UnorderedSingleListNode;
using ::RaftCore::DataStructure::LockFreeUnorderedSingleList;

class TestLockFreeUnorderedSingList : public DataStructureBase<LockFreeUnorderedSingleList,int> {

    protected:

        virtual void SetUp() override {

            this->m_remain = ::RaftCore::Config::FLAGS_retain_num_unordered_single_list;

            //Install GC.
            std::thread _t([&]() {
                //auto _f = std::bind(&LockFreeUnorderedSingleList<int>::PurgeSingleList, &(this->m_ds));
                while (true) {
                    if (!this->m_running)
                        break;

                    this->m_ds.PurgeSingleList(this->m_remain);
                }
            });
            _t.detach();
        }

        virtual void TearDown() override {
            this->m_running = false;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        virtual void Dump() override {
            auto _printer = [](int *p) {
                std::cout << *p << " ";
            };
            this->m_ds.Iterate(_printer);
        }

        int m_remain = 10000;

        bool m_running = true;
};

TEST_F(TestLockFreeUnorderedSingList, GeneralOperation) {

    auto _deleter = [](int* p_data) {
        std::cout << "customized deleter called" << std::endl;
        delete p_data;
    };

    this->m_ds.SetDeleter(_deleter);

    uint32_t _push_num = 5;
    for (std::size_t i = 0; i < _push_num; ++i) {
        int *_p_i = new int(i);
        this->m_ds.PushFront(_p_i);
    }

    ASSERT_TRUE(this->m_ds.Size() <= _push_num);

    int _retain_num = 2;
    this->m_ds.PurgeSingleList(_retain_num);
    ASSERT_EQ(this->m_ds.Size(), _retain_num);

    std::cout << "after testing...:" << std::endl;

    this->Dump();
}


TEST_F(TestLockFreeUnorderedSingList, ConcurrentOperation) {

    int _count = 10000;

    auto _push_it = [&](int idx){
        for (int i = 0; i < _count; ++i) {
            int *_p_i = new int(i);
            this->m_ds.PushFront(_p_i);
        }
    };

    this->LaunchMultipleThread(_push_it);

    //Waiting for purging done.
    std::this_thread::sleep_for(std::chrono::seconds(1));

    //ASSERT_EQ(this->m_ds.Size() , this->m_remain) << "actual size:" << this->m_ds.Size();

    std::cout << "done.";
}


#endif
