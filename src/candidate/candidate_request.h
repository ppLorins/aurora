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

#ifndef __AURORA_CANDIDATE_REQUEST_H__
#define __AURORA_CANDIDATE_REQUEST_H__

#include <memory>

#include "protocol/raft.grpc.pb.h"
#include "protocol/raft.pb.h"

#include "common/request_base.h"

using ::raft::RaftService;
using ::grpc::ServerCompletionQueue;
using ::RaftCore::Common::UnaryRequest;

namespace RaftCore::Candidate {

template<typename T,typename R,typename Q>
class CandidateUnaryRequest : public UnaryRequest<T, R, Q> {

public:

    CandidateUnaryRequest()noexcept;

    virtual ~CandidateUnaryRequest()noexcept;

private:

    CandidateUnaryRequest(const CandidateUnaryRequest&) = delete;

    CandidateUnaryRequest& operator=(const CandidateUnaryRequest&) = delete;
};

} //end namespace

#include "candidate_request.cc"

#endif
