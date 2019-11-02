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

#ifndef __GTEST_FOLLOWER_ENTITY_H__
#define __GTEST_FOLLOWER_ENTITY_H__

#include <list>
#include <memory>
#include <chrono>

#include "gtest/test_base.h"
#include "leader/follower_entity.h"

using ::RaftCore::Leader::FollowerEntity;
using ::RaftCore::Leader::FollowerStatus;
using ::RaftCore::Member::JointConsensusMask;

class TestFollowerEntity : public TestSingleBackendFollower {

    public:

        TestFollowerEntity() {}

    protected:

        virtual void SetUp() override {}

        virtual void TearDown() override {}

};

TEST_F(TestFollowerEntity, GeneralOperation) {

    std::shared_ptr<CompletionQueue> _shp_cq(new CompletionQueue());

    FollowerEntity _obj_entity(this->m_follower_svc_addr, FollowerStatus::NORMAL,
        uint32_t(JointConsensusMask::IN_OLD_CLUSTER), _shp_cq);
}


#endif
