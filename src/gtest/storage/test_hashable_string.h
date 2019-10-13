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

#ifndef __GTEST_HASHABLE_STRING_H__
#define __GTEST_HASHABLE_STRING_H__

#include <list>
#include <memory>

#include "gtest/test_base.h"
#include "storage/hashable_string.h"

using ::RaftCore::Storage::HashableString;

class TestHashableString : public TestBase {

    public:

        TestHashableString() {}

        virtual void SetUp() override {}

        virtual void TearDown() override {}

    protected:

};

TEST_F(TestHashableString, GeneralOperation) {

    HashableString _obj1("abc1");

    HashableString _obj2("abc2");

    HashableString _obj3("abc2");

    ASSERT_TRUE(_obj2.Hash() == _obj3.Hash());

    ASSERT_TRUE(_obj1 < _obj2);

    ASSERT_TRUE(_obj2 == _obj3);

    ASSERT_TRUE(_obj2 == "abc2");

    _obj3 = _obj1;

    ASSERT_TRUE(_obj3 == "abc1");

    ASSERT_TRUE(_obj3.GetStr() == "abc1");

    std::string _tmp = "on_the_fly";

    HashableString _obj4(_tmp, false);

    ASSERT_TRUE(_obj4.GetStr()==_tmp);

}



#endif
