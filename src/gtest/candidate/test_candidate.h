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

#ifndef __GTEST_CANDIDATE_H__
#define __GTEST_CANDIDATE_H__

#include <list>
#include <memory>
#include <chrono>

#include "gtest/test_base.h"
#include "candidate/candidate_view.h"

using ::RaftCore::Candidate::CandidateView;

class TestCandidate : public TestBase {

    public:

        TestCandidate() {}

        ~TestCandidate() {}

    protected:

        virtual void SetUp() override {}

        virtual void TearDown() override {}

};

TEST_F(TestCandidate, GeneralOperation) {

    CandidateView::Initialize();
    CandidateView::UnInitialize();

    std::cout << "test candidate end." << std::endl;

}


#endif
