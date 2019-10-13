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

#ifndef __AURORA_FOLLOWER_REQUEST_H__
#define __AURORA_FOLLOWER_REQUEST_H__

#include <memory>

#include "protocol/raft.grpc.pb.h"
#include "protocol/raft.pb.h"

#include "common/request_base.h"

using ::raft::RaftService;
using ::grpc::ServerCompletionQueue;
using ::RaftCore::Common::UnaryRequest;
using ::RaftCore::Common::BidirectionalRequest;

namespace RaftCore::Follower {

//Just a thin wrapper for differentiate rpcs.
template<typename T,typename R,typename Q>
class FollowerUnaryRequest : public UnaryRequest<T,R,Q>{

public:

    FollowerUnaryRequest()noexcept;

    virtual ~FollowerUnaryRequest()noexcept;

private:

    FollowerUnaryRequest(const FollowerUnaryRequest&) = delete;

    FollowerUnaryRequest& operator=(const FollowerUnaryRequest&) = delete;

};

template<typename T,typename R,typename Q>
class FollowerBidirectionalRequest : public BidirectionalRequest<T,R,Q>{

public:

    FollowerBidirectionalRequest()noexcept;

    virtual ~FollowerBidirectionalRequest()noexcept;

private:

    FollowerBidirectionalRequest(const FollowerBidirectionalRequest&) = delete;

    FollowerBidirectionalRequest& operator=(const FollowerBidirectionalRequest&) = delete;

};

} //end namespace

#include "follower_request.cc"

#endif
