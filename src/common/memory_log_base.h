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

#ifndef _AURORA_MEMORY_LOG_BASE_H_
#define _AURORA_MEMORY_LOG_BASE_H_

#include <ostream>

#include "protocol/raft.pb.h"

namespace RaftCore::Common {

class MemoryLogItemBase {

public:

    MemoryLogItemBase(uint32_t _term, uint64_t _index)noexcept;

    MemoryLogItemBase(const ::raft::Entity &_entity)noexcept;

    virtual ~MemoryLogItemBase()noexcept;

    bool operator<(const MemoryLogItemBase &_other)const noexcept;

    bool operator>(const MemoryLogItemBase &_other)const noexcept;

    virtual bool operator==(const MemoryLogItemBase& _other)const noexcept;

    virtual bool operator!=(const MemoryLogItemBase& _other)const noexcept;

    bool AfterOf(const MemoryLogItemBase& _other)const noexcept;

    std::shared_ptr<::raft::Entity> GetEntity()const noexcept;

protected:

    //Prevent the base class from being instantiated
    virtual void NotImplemented() noexcept = 0 ;

protected:

    //Note: doesn't take the ownership of the original object
    std::shared_ptr<::raft::Entity>     m_entity;
};

bool CmpMemoryLog(const MemoryLogItemBase *left, const MemoryLogItemBase *right) noexcept;

}

#endif
