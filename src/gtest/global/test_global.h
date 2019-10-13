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

#ifndef __GTEST_GLOBAL_H__
#define __GTEST_GLOBAL_H__

#include <list>
#include <memory>
#include <chrono>

#include "gtest/test_base.h"
#include "global/global_env.h"
#include "state/state_mgr.h"

using ::RaftCore::Global::GlobalEnv;
using ::RaftCore::State::RaftRole;

class TestGlobalEnv : public TestMultipleBackendFollower {

    public:

        TestGlobalEnv() {}

        virtual void SetUp() override {
            this->StartFollowers();
        }

        virtual void TearDown() override {
            this->EndFollowers();
        }

};

TEST_F(TestGlobalEnv, GeneralOperation) {

    ::RaftCore::Global::GlobalEnv::InitialEnv();

    std::cout << "server is going to run 1st time..." << std::endl;

    std::thread* _th = new std::thread([]() {
        ::RaftCore::Global::GlobalEnv::RunServer();
    });

    std::this_thread::sleep_for(std::chrono::seconds(3));
    ::RaftCore::Global::GlobalEnv::StopServer();
    ::RaftCore::Global::GlobalEnv::UnInitialEnv(RaftRole::LEADER);

    _th->join();

    std::cout << "server is stopped now 1st time..." << std::endl;

    ::RaftCore::Global::GlobalEnv::InitialEnv(true);

    std::cout << "server is going to run 2nd time..." << std::endl;

    _th = new std::thread([]() {
        ::RaftCore::Global::GlobalEnv::RunServer();
    });

    std::this_thread::sleep_for(std::chrono::seconds(3));
    ::RaftCore::Global::GlobalEnv::StopServer();
    ::RaftCore::Global::GlobalEnv::UnInitialEnv(RaftRole::LEADER);
    _th->join();

    std::cout << "server is stopped now 2nd time..." << std::endl;


    ::RaftCore::Global::GlobalEnv::InitialEnv(true);

    std::cout << "server is going to run 3rd time..." << std::endl;

    _th = new std::thread([]() {
        ::RaftCore::Global::GlobalEnv::RunServer();
    });

    ::RaftCore::Global::GlobalEnv::ShutDown();

    std::cout << "server is stopped now 3rd time..." << std::endl;
}


#endif
