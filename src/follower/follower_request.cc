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

#include "follower/follower_request.h"

namespace RaftCore::Follower {

template<typename T,typename R,typename Q>
FollowerUnaryRequest<T, R, Q>::FollowerUnaryRequest() noexcept {}

template<typename T,typename R,typename Q>
FollowerUnaryRequest<T, R, Q>::~FollowerUnaryRequest() noexcept {}

template<typename T,typename R,typename Q>
FollowerBidirectionalRequest<T, R, Q>::FollowerBidirectionalRequest() noexcept {}

template<typename T,typename R,typename Q>
FollowerBidirectionalRequest<T,R,Q>::~FollowerBidirectionalRequest() noexcept {}

}

