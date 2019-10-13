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

#include  "leader/memory_log_leader.h"

namespace RaftCore::Leader {

MemoryLogItemLeader::~MemoryLogItemLeader() noexcept{
    /*The write_op of m_entity of LeaderLogItem if from set_allocated_write_op in the leader service,
      so we need to release the ownership of write_op before releasing m_entity.  */
    //this->m_entity->release_write_op();
}

MemoryLogItemLeader::MemoryLogItemLeader(uint32_t _term, uint64_t _index) noexcept:MemoryLogItemBase(_term, _index) {}

MemoryLogItemLeader::MemoryLogItemLeader(const ::raft::Entity &_entity) noexcept:MemoryLogItemBase(_entity) {}

bool MemoryLogItemLeader::operator<(const MemoryLogItemLeader& _other)const noexcept {
    return this->MemoryLogItemBase::operator<(_other);
}

bool MemoryLogItemLeader::operator>(const MemoryLogItemLeader& _other)const noexcept {
    return this->MemoryLogItemBase::operator>(_other);
}

bool MemoryLogItemLeader::operator==(const MemoryLogItemLeader& _other)const noexcept {
    return this->MemoryLogItemBase::operator==(_other);
}

bool MemoryLogItemLeader::operator!=(const MemoryLogItemLeader& _other)const noexcept {
    return !this->MemoryLogItemBase::operator==(_other);
}

bool CmpMemoryLogLeader(const MemoryLogItemLeader& left, const MemoryLogItemLeader& right) noexcept {
    return CmpMemoryLog(&left,&right);
}

}
