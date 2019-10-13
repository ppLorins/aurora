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

#ifndef __GTEST_TIMER_H__
#define __GTEST_TIMER_H__

#include <list>
#include <memory>
#include <chrono>
#include <random>

#include "gtest/test_base.h"
#include "tools/timer.h"

using ::RaftCore::Timer::GlobalTimer;

class TestTimer : public TestBase {

    public:

        TestTimer() {}

        virtual void SetUp() override {
        }

        virtual void TearDown() override {
        }

    protected:


};

TEST_F(TestTimer, GeneralOperation) {

    GlobalTimer::Initialize();

    int _counter = 0;
    auto _print = [&]() ->bool{
        if (_counter++ >= 10) {
            return false;
        }

        std::cout << "thread id: " << std::this_thread::get_id() << " job called." << std::endl;
        return true;
    };

    GlobalTimer::AddTask(1000,_print);

    std::this_thread::sleep_for(std::chrono::seconds(8));

    GlobalTimer::UnInitialize();

    //Manually checking if the thread exist
    std::this_thread::sleep_for(std::chrono::seconds(10));

    std::cout << "done." << std::endl;
}


#endif
