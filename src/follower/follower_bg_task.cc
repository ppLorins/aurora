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

#include "service/service.h"
#include "member/member_manager.h"
#include "follower/follower_bg_task.h"

namespace RaftCore::Follower::BackGroundTask {

DisorderMessageContext::DisorderMessageContext(int value_flag)noexcept{
    this->m_value_flag = value_flag;
    this->m_generation_tp = std::chrono::system_clock::now();

    this->m_processed_flag.store(false);
    //VLOG(89) << "DisorderMessageContext constructed";
}

DisorderMessageContext::~DisorderMessageContext()noexcept{
    //VLOG(89) << "DisorderMessageContext destructed";
}

bool DisorderMessageContext::operator<(const DisorderMessageContext& other)const noexcept {

    if (this->m_value_flag < 0 || other.m_value_flag > 0)
        return true;

    if (this->m_value_flag > 0 || other.m_value_flag < 0)
        return false;

    auto &_shp_req       = this->m_append_request;
    auto &_shp_req_other = other.m_append_request;

    return _shp_req->GetLastLogID() < _shp_req_other->GetLastLogID();
}

bool DisorderMessageContext::operator>(const DisorderMessageContext& other)const noexcept {

    if (this->m_value_flag < 0 || other.m_value_flag > 0)
        return false;

    if (this->m_value_flag > 0 || other.m_value_flag < 0)
        return true;

    auto &_shp_req       = this->m_append_request;
    auto &_shp_req_other = other.m_append_request;

    return _shp_req->GetLastLogID() > _shp_req_other->GetLastLogID();
}

bool DisorderMessageContext::operator==(const DisorderMessageContext& other)const noexcept {

    if (other.m_value_flag != 0 || this->m_value_flag != 0)
        return false;

    auto &_shp_req       = this->m_append_request;
    auto &_shp_req_other = other.m_append_request;

    return _shp_req->GetLastLogID() == _shp_req_other->GetLastLogID();
}

}

