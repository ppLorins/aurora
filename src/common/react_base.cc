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

#include "glog/logging.h"

#include "common/react_base.h"

namespace RaftCore::Common {

ReactInfo::ReactInfo() noexcept {}

void ReactInfo::Set(bool cq_result, void* tag) noexcept {
    this->m_cq_result = cq_result;
    this->m_tag = tag;
}

ReactInfo::ReactInfo(const ReactInfo &other) noexcept {
    this->m_cq_result = other.m_cq_result;
    this->m_tag = other.m_tag;
}

ReactBase::ReactBase() noexcept{}

ReactBase::~ReactBase() noexcept{}

void ReactBase::GeneralReacting(const ReactInfo &info)noexcept {
    ::RaftCore::Common::ReactBase* _p_ins = static_cast<::RaftCore::Common::ReactBase*>(info.m_tag);
    _p_ins->React(info.m_cq_result);
}

}

