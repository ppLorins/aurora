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

#ifndef __GTEST_SSTABLE_H__
#define __GTEST_SSTABLE_H__

#include <list>
#include <memory>

#include "gtest/test_base.h"
#include "storage/sstable.h"

using ::RaftCore::Storage::MemoryTable;
using ::RaftCore::Storage::SSTAble;

class TestSSTable : public TestBase {

    public:

        TestSSTable() {}

        virtual void SetUp() override {}

        virtual void TearDown() override {}

    protected:

        void GeneralOperation() {

        }

};

TEST_F(TestSSTable, GeneralOperation) {
    MemoryTable _memtable_1;
    _memtable_1.Insert("k1", "v1", 1, 1);
    _memtable_1.Insert("k2", "v2", 1, 2);
    _memtable_1.Insert("k3", "v3", 1, 3);

    SSTAble _sstable_1(_memtable_1);

    MemoryTable _memtable_2;
    _memtable_2.Insert("k2", "v2", 1, 2);
    _memtable_2.Insert("k3", "v3", 1, 3);
    _memtable_2.Insert("k4", "v4", 1, 4);

    SSTAble _sstable_2(_memtable_2);

    SSTAble _sstable_3(_sstable_2.GetFilename().c_str());

    //Older file merged into newer file.
    SSTAble _sstable_merged(_sstable_1,_sstable_2);

    std::string _val = "";
    ASSERT_TRUE(_sstable_merged.Read("k4", _val));
    ASSERT_TRUE(_val == "v4");

    auto _max_id = _sstable_merged.GetMaxLogID();
    ASSERT_TRUE(_max_id.m_term == 1 && _max_id.m_index == 4);

    ASSERT_TRUE(_sstable_merged.GetFilename() == (_sstable_2.GetFilename() + _AURORA_SSTABLE_MERGE_SUFFIX_));

    auto _traverse = [](const SSTAble::Meta &meta,const HashableString &key) ->bool{
        std::cout << "traverse term:" << meta.m_term << ",index:" << meta.m_index << std::endl;;
        return true;
    };

    _sstable_merged.IterateByVal(_traverse);
}

TEST_F(TestSSTable, Performance) {

    MemoryTable _memtable;

    std::string _key = "", _val="";

    for (int i = 0; i < 20000; ++i) {
        _key = std::to_string(i);
        _val = std::to_string(i);

        _memtable.Insert(_key, _val, 1, 1);
    }

    std::cout << "........." << std::endl;

    SSTAble _sstable(_memtable);
}

TEST_F(TestSSTable, ConcurrentOperation) {

    MemoryTable _memtable_1;
    _memtable_1.Insert("k1", "v1", 1, 1);
    _memtable_1.Insert("k2", "v2", 1, 2);
    _memtable_1.Insert("k3", "v3", 1, 3);
    _memtable_1.Insert("k4", "v4", 1, 4);

    SSTAble _sstable_1(_memtable_1);

    auto _op = [&](int idx) {
        for (int i = 1; i <= 4; ++i) {
            std::string  _key = "k" + std::to_string(i);
            std::string  _expected_val = "v" + std::to_string(i);

            std::string _val = "";
            ASSERT_TRUE(_sstable_1.Read(_key, _val)) << "key:" << _key;
            ASSERT_EQ(_val, _expected_val);
        }

    };

    this->LaunchMultipleThread(_op);
}


#endif
