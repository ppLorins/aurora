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

#ifndef __GTEST_GUID_H__
#define __GTEST_GUID_H__

#include <list>
#include <memory>
#include <chrono>

#include "boost/filesystem.hpp"

#include "gtest/test_base.h"
#include "guid/guid_generator.h"

using ::RaftCore::Guid::GuidGenerator;
namespace  fs = ::boost::filesystem;

class TestGuid : public TestBase {

    public:

        TestGuid() {}

        virtual void SetUp() override {
            m_i.store(0);
        }

        virtual void TearDown() override {
        }

    protected:

        std::vector<uint64_t>    *m_vec_output = new std::vector<uint64_t>[this->m_cpu_cores];

        std::atomic<int> m_i;
};

TEST_F(TestGuid, GeneralOperation) {

    uint64_t _base = 100;
    GuidGenerator::Initialize(_base);

    uint64_t _pre = _base;
    uint64_t _last_release = _base;

    for (int i = 1; i <= 50 ; ++i) {
        GuidGenerator::GUIDPair _pair = GuidGenerator::GenerateGuid();

        uint64_t _cur = _last_release + 1;

        ASSERT_EQ(_pair.m_pre_guid,_pre);
        ASSERT_EQ(_pair.m_cur_guid,_cur);

        _pre = _cur;
        _last_release = _cur;
    }

    _base = 300;
    _last_release = _base;
    GuidGenerator::SetNextBasePoint(_base);
    GuidGenerator::GUIDPair _pair = GuidGenerator::GenerateGuid();

    ASSERT_EQ(_pair.m_pre_guid, _base);
    ASSERT_EQ(_pair.m_cur_guid, _last_release + 1);

    ASSERT_EQ(GuidGenerator::GetLastReleasedGuid(), _last_release + 1);

    GuidGenerator::UnInitialize();
}

TEST_F(TestGuid, ConcurrentOperation) {

    GuidGenerator::Initialize();

    auto _op = [&](int idx) {

        int _counter = 0;

        for (int i = 1; i <= 100 ; ++i) {
            GuidGenerator::GUIDPair _pair = GuidGenerator::GenerateGuid();

            std::cout << std::this_thread::get_id() << " generate: " << _pair.m_pre_guid << "|"
                << _pair.m_cur_guid << std::endl;

            _counter++;

            if (_counter > 20) {
                std::cout << std::this_thread::get_id() << " last guid: " << GuidGenerator::GetLastReleasedGuid() << std::endl;
                GuidGenerator::SetNextBasePoint(_pair.m_cur_guid - 5);
                _counter = 0;
            }

            m_vec_output[idx].push_back(_pair.m_cur_guid);

        }
    };

    this->LaunchMultipleThread(_op);

    //merge vectors
    std::vector<uint64_t> _total_vec;
    for (int i = 0; i < this->m_cpu_cores; ++i)
        _total_vec.insert(_total_vec.cend(),m_vec_output[i].cbegin(),m_vec_output[i].cend());

    std::sort(_total_vec.begin(), _total_vec.end());

    //for (const auto & _item : _total_vec)
    //    std::cout << "final :" << _item << std::endl;

    GuidGenerator::UnInitialize();
}



#endif
