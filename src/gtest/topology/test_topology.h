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

#ifndef __GTEST_TOPOLOGY_H__
#define __GTEST_TOPOLOGY_H__

#include <list>
#include <memory>
#include <chrono>

#include "gtest/test_base.h"
#include "topology/topology_mgr.h"

using ::RaftCore::Topology;
using ::RaftCore::CTopologyMgr;

class TestTopology : public TestBase {

    public:

        TestTopology() {}

        virtual void SetUp() override {
            CTopologyMgr::Initialize();
        }

        virtual void TearDown() override {
            CTopologyMgr::UnInitialize();
        }

        void GeneralOP() noexcept{
            Topology _topo;
            CTopologyMgr::Read(&_topo);
            std::cout << _topo;

            _topo.m_leader = "some content";
            _topo.Reset();
            ASSERT_EQ(_topo.m_leader,"");

            _topo.m_leader = "";
            _topo.m_followers.emplace("127.0.0.1:3000");
            _topo.m_followers.emplace("127.0.0.1:3001");

            _topo.m_candidates.emplace("127.0.0.1:3002");
            _topo.m_candidates.emplace("127.0.0.1:3003");

            ASSERT_EQ(_topo.GetClusterSize(),5);

            ASSERT_TRUE(_topo.InCurrentCluster("127.0.0.1:3000"));
            ASSERT_FALSE(_topo.InCurrentCluster("127.0.0.1:3005"));

            _topo.m_leader = "new value";
            CTopologyMgr::Update(_topo);
            std::cout << _topo;
        }
};

TEST_F(TestTopology, GeneralOperation) {

    this->GeneralOP();
}

TEST_F(TestTopology, ConcurrentOperation) {

    auto _op = [&](int idx) {
        int _run_times = 100;
        for (int i = 0; i < _run_times; ++i) {
            this->GeneralOP();
        }
    };

    this->LaunchMultipleThread(_op);
}


#endif
