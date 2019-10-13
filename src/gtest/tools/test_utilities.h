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

#ifndef __GTEST_UTILITIES_H__
#define __GTEST_UTILITIES_H__

#include <list>
#include <memory>
#include <chrono>
#include <random>

#include "gtest/test_base.h"
#include "tools/utilities.h"

using ::RaftCore::Tools::TypeSysTimePoint;

class TestUtilities : public TestBase {

    public:

        TestUtilities() {}

        virtual void SetUp() override {
        }

        virtual void TearDown() override {
        }

    protected:


};

TEST_F(TestUtilities, GeneralOperation) {

    uint32_t uTest  = 0x12345678;
    uint32_t uTest2 = 0x78563412;
    unsigned char* pTest = (unsigned char*)&uTest;
    bool _big_endian = (*pTest) == 0x12;

    ASSERT_EQ(::RaftCore::Tools::LocalBigEndian(), _big_endian);

    uint32_t uTmp = 0x0;
    ::RaftCore::Tools::ConvertToBigEndian<uint32_t>(uTest,&uTmp);
    if (_big_endian)
        ASSERT_EQ(uTmp,uTest);
    else
        ASSERT_EQ(uTmp,uTest2);

    uint32_t uRst1 = 0;
    ::RaftCore::Tools::ConvertBigEndianToLocal<uint32_t>(uTmp,&uRst1);
    ASSERT_EQ(uRst1,uTest);

    std::list<std::string> myself_addr;
    ::RaftCore::Tools::GetLocalIPs(myself_addr);
    for (const auto &_item : myself_addr)
        std::cout << "ip : " << _item << std::endl;

    uint32_t x = 5;
    ASSERT_EQ(::RaftCore::Tools::RoundUp(x),8);

    ASSERT_EQ(::RaftCore::Tools::GetMask(x),3);

    std::string _test_buf = "test_buf";
    uint32_t _crc32_result = ::RaftCore::Tools::CalculateCRC32(_test_buf.data(), _test_buf.length());
    ASSERT_EQ(_crc32_result, 0x3BF65345);

    //Checking the log with VLOG level >= 90.
    auto _tp = ::RaftCore::Tools::StartTimeing();
    ::RaftCore::Tools::EndTiming(_tp,"unit test operations");

    std::list<std::string> _output;
    ::RaftCore::Tools::StringSplit("||def||abc||xyz|||",'|',_output);

    ASSERT_EQ(_output.size(),3);

    auto _iter = _output.cbegin();
    ASSERT_EQ(*_iter++,"def");
    ASSERT_EQ(*_iter++,"abc");
    ASSERT_EQ(*_iter++,"xyz");

    ASSERT_TRUE(_iter == _output.cend());


    TypeSysTimePoint _deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(100);

    _deadline += std::chrono::microseconds(2000);

    std::cout << "now:" << ::RaftCore::Tools::TimePointToString(_deadline) << std::endl;

}


#endif
