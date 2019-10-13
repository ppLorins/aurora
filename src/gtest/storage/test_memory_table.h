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

#ifndef __GTEST_MEMORY_TABLE_H__
#define __GTEST_MEMORY_TABLE_H__

#include <list>
#include <memory>

#include "gtest/test_base.h"
#include "storage/memory_table.h"

using ::RaftCore::Storage::MemoryTable;
using ::RaftCore::Storage::TypePtrHashableString;
using ::RaftCore::Storage::TypePtrHashValue;

class TestMemoryTable : public TestBase {

    public:

        TestMemoryTable() {}

        virtual void SetUp() override {}

        virtual void TearDown() override {}

    protected:

        void GeneralOperation() {

            this->m_obj.Insert("k1", "v1", 1, 1);
            this->m_obj.Insert("k2", "v2", 1, 2);
            this->m_obj.Insert("k3", "v3", 1, 3);

            auto _iterator = [](const TypePtrHashableString &k,const TypePtrHashValue &v) {
                //std::cout << "k:" << k->GetStr() << ",v:" << v->m_term << "|" << v->m_index << "|" << v->m_val << std::endl;;
                return true;
            };

            this->m_obj.IterateByKey(_iterator);

            std::string _val = "";
            ASSERT_TRUE(this->m_obj.GetData("k1", _val));

            ASSERT_TRUE(_val == "v1");

            ASSERT_TRUE(this->m_obj.Size() == 3) << ",actual size:" << this->m_obj.Size();
        }

    protected:

        MemoryTable m_obj;
};

TEST_F(TestMemoryTable, GeneralOperation) {
    this->GeneralOperation();
}

TEST_F(TestMemoryTable, ConcurrentOperation) {

    auto _op = [&](int idx) {
        int _run_times = 1000;
        for (int i = 0; i < _run_times; ++i) {
            this->GeneralOperation();
            std::cout << "thread:" << std::this_thread::get_id() << " finish round:" << i << std::endl;
        }
    };

    this->LaunchMultipleThread(_op);
}


#endif
