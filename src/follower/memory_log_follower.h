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

#ifndef _AURORA_MEMORY_LOG_FOLLOWER_H_
#define _AURORA_MEMORY_LOG_FOLLOWER_H_

#include <ostream>

#include "protocol/raft.pb.h"

#include "tools/trivial_lock_double_list.h"
#include "common/memory_log_base.h"

using ::RaftCore::Common::MemoryLogItemBase;
using ::RaftCore::Common::EntityIDEqual;
using ::RaftCore::Common::EntityIDSmallerEqual;

namespace RaftCore::Follower {

class MemoryLogItemFollower final : public ::RaftCore::DataStructure::OrderedTypeBase<MemoryLogItemFollower> , public MemoryLogItemBase {

public:

    virtual ~MemoryLogItemFollower() noexcept;

    MemoryLogItemFollower(uint32_t _term, uint64_t _index) noexcept;

    MemoryLogItemFollower(const ::raft::Entity &_entity) noexcept;

    bool operator<=(const MemoryLogItemFollower& _other)const noexcept;

    virtual bool operator<(const MemoryLogItemFollower& _other)const  noexcept;

    virtual bool operator>(const MemoryLogItemFollower& _other)const noexcept;

    virtual bool operator==(const MemoryLogItemFollower& _other)const noexcept;

    virtual bool operator!=(const MemoryLogItemFollower& _other)const noexcept;

protected:

    virtual void NotImplemented() noexcept{}

};

typedef std::list<std::shared_ptr<MemoryLogItemFollower>>   TypeMemlogFollowerList;

bool CmpMemoryLogFollower(const MemoryLogItemFollower& left, const MemoryLogItemFollower& right) noexcept;

}

#endif
