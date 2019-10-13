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

#ifndef __GTEST_STATE_H__
#define __GTEST_STATE_H__

#include <list>
#include <memory>
#include <chrono>

#include "gtest/test_base.h"
#include "state/state_mgr.h"
#include "topology/topology_mgr.h"

using ::RaftCore::State::RaftRole;
using ::RaftCore::State::StateMgr;
using ::RaftCore::Topology;
using ::RaftCore::CTopologyMgr;

class TestState : public TestBase {

    public:

        TestState() {}

        virtual void SetUp() override {
        }

        virtual void TearDown() override {
        }

};

TEST_F(TestState, GeneralOperation) {

    CTopologyMgr::Initialize();

    Topology _topo;
    CTopologyMgr::Read(&_topo);

    StateMgr::Initialize(_topo);

    auto _state = StateMgr::GetRole();
    ASSERT_EQ(_state, RaftRole::LEADER);
    ASSERT_STREQ(StateMgr::GetRoleStr(), "leader");
    ASSERT_STREQ(StateMgr::GetMyAddr().c_str(),this->m_leader_addr.c_str());

    //---------------Leader --> Follower----------------//
#define _NEW_LEADER_ADDRESS_ "192.168.0.100:10077"
    int _old_follower_size = _topo.m_followers.size();
    StateMgr::SwitchTo(RaftRole::FOLLOWER,_NEW_LEADER_ADDRESS_);

    _state = StateMgr::GetRole();
    ASSERT_EQ(_state, RaftRole::FOLLOWER);
    ASSERT_STREQ(StateMgr::GetRoleStr(), "follower");

    CTopologyMgr::Read(&_topo);
    ASSERT_STREQ(_topo.m_leader.c_str(),_NEW_LEADER_ADDRESS_);

    int _new_follower_size = _topo.m_followers.size();
    ASSERT_EQ(_old_follower_size + 1, _new_follower_size );

    ASSERT_TRUE(std::find(_topo.m_followers.cbegin(),_topo.m_followers.cend(),this->m_leader_addr) != _topo.m_followers.cend());

    StateMgr::UnInitialize();
}


#endif
