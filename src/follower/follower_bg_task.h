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

#ifndef __AURORA_FOLLOWER_BG_TASK_H__
#define __AURORA_FOLLOWER_BG_TASK_H__

#include "tools/utilities.h"
#include "tools/trivial_lock_double_list.h"

namespace RaftCore {
    namespace Service {
        class AppendEntries;
    }
}

namespace RaftCore::Follower::BackGroundTask {

using ::RaftCore::DataStructure::OrderedTypeBase;
using ::RaftCore::Tools::TypeSysTimePoint;
using ::RaftCore::Service::AppendEntries;

class DisorderMessageContext final : public OrderedTypeBase<DisorderMessageContext> {

    public:

        DisorderMessageContext(int value_flag = 0)noexcept;

        virtual ~DisorderMessageContext()noexcept;

        virtual bool operator<(const DisorderMessageContext& other)const noexcept override;

        virtual bool operator>(const DisorderMessageContext& other)const noexcept override;

        virtual bool operator==(const DisorderMessageContext& other)const noexcept override;

        std::shared_ptr<AppendEntries>  m_append_request;

        TypeSysTimePoint    m_generation_tp;

        /*<0: minimal value;
          >0:max value;
          ==0:comparable value.  */
        int     m_value_flag = 0;

        std::atomic<bool>   m_processed_flag;
};

} //end namespace

#endif
