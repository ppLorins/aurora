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

#ifndef __AURORA_CANDIDATE_VIEW_H__
#define __AURORA_CANDIDATE_VIEW_H__

#include <memory>
#include <shared_mutex>

#include "common/comm_view.h"

namespace RaftCore::Candidate {

using ::RaftCore::Common::CommonView;

//CandidateView  is nothing inside, all logic in candidate is in the `Election` module.
class CandidateView final: public CommonView{

public:

    static void Initialize() noexcept;

    static void UnInitialize() noexcept;

private:

    CandidateView() = delete;

    virtual ~CandidateView() = delete;

    CandidateView(const CandidateView&) = delete;

    CandidateView& operator=(const CandidateView&) = delete;

};

} //end namespace

#endif
