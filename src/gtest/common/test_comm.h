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

#ifndef __GTEST_COMMON_H__
#define __GTEST_COMMON_H__

#include <list>
#include <memory>
#include <chrono>

#include "gtest/test_base.h"
#include "common/comm_view.h"
#include "common/comm_defs.h"
#include "common/log_identifier.h"
#include "leader/memory_log_leader.h"
#include "follower/memory_log_follower.h"

using ::RaftCore::Common::LogIdentifier;
using ::RaftCore::Leader::MemoryLogItemLeader;
using ::RaftCore::Follower::MemoryLogItemFollower;
using ::RaftCore::Common::CommonView;
using ::raft::EntityID;

class TestComm : public TestBase {

    public:

        TestComm() {}

        virtual void SetUp() override {
        }

        virtual void TearDown() override {
        }

};

TEST_F(TestComm, GeneralOperation) {

    LogIdentifier _obj1;
    _obj1.Set(3, 17);

    LogIdentifier _obj2;
    _obj2.Set(_obj1);
    ASSERT_TRUE(_obj1==_obj2);

    _obj2.m_index = 19;
    ASSERT_TRUE(_obj1!=_obj2);

    _obj2.m_index = 17;
    ASSERT_TRUE(_obj1<=_obj2);

    _obj2.m_index = 13;
    ASSERT_TRUE(_obj1>_obj2);

    _obj2.m_index = 17;
    ASSERT_TRUE(_obj1>=_obj2);

    std::cout << _obj1.ToString() << std::endl;
    std::cout << _obj1  << std::endl;

    EntityID _entity_id;
    _entity_id.set_term(3);
    _entity_id.set_idx(17);

    ASSERT_TRUE(::RaftCore::Common::ConvertID(_entity_id)== _obj2);
    ASSERT_TRUE(::RaftCore::Common::EntityIDEqual(_entity_id, _obj2));

    _obj2.m_index = 19;
     //ASSERT_TRUE(::RaftCore::Common::EntityIDNotEqual(_entity_id, _obj2));

    _obj2.m_index = 13;
    ASSERT_TRUE(::RaftCore::Common::EntityIDLarger(_entity_id, _obj2));

    _obj2.m_index = 19;
    ASSERT_TRUE(::RaftCore::Common::EntityIDSmaller(_entity_id, _obj2));

    _obj2.m_index = 17;
    //ASSERT_TRUE(::RaftCore::Common::EntityIDSmallerEqual(_entity_id, _obj2));

    MemoryLogItemLeader _ldr1(3,17);
    MemoryLogItemLeader _ldr2(3,19);
    MemoryLogItemLeader _ldr3(*_ldr1.GetEntity());

    ASSERT_TRUE(_ldr1<_ldr2);
    ASSERT_TRUE(_ldr2>_ldr1);
    ASSERT_TRUE(_ldr1==_ldr3);

    MemoryLogItemFollower _f1(3,17);

    MemoryLogItemFollower _f2(*_f1.GetEntity());
    MemoryLogItemFollower _f3(3,19);

    ASSERT_TRUE(_f1<=_f2);
    ASSERT_TRUE(_f1<_f3);
    ASSERT_TRUE(_f3>_f1);
    ASSERT_TRUE(_f1==_f2);
    ASSERT_TRUE(_f1!=_f3);

    CommonView::Initialize();

    CommonView::UnInitialize();
}


#endif
