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

#include  "common/comm_defs.h"
#include  "common/log_identifier.h"
#include  "follower/memory_log_follower.h"

namespace RaftCore::Follower {

MemoryLogItemFollower::~MemoryLogItemFollower() noexcept{
    //VLOG(89) << "MemoryLogItemFollower destructed " << ::RaftCore::Common::ConvertID(this->m_entity->entity_id());
}

MemoryLogItemFollower::MemoryLogItemFollower(uint32_t _term, uint64_t _index) noexcept:MemoryLogItemBase(_term, _index) {
    //VLOG(89) << "MemoryLogItemFollower constructed pos1";
}

MemoryLogItemFollower::MemoryLogItemFollower(const ::raft::Entity &_entity) noexcept: MemoryLogItemBase(_entity) {
    //VLOG(89) << "MemoryLogItemFollower constructed pos2";
}

bool MemoryLogItemFollower::operator==(const MemoryLogItemFollower& _other)const noexcept {
    if (!MemoryLogItemBase::operator==(_other))
        return false;

    /*MemoryLogItemFollower comparing additional fields because of it is used in TrivialLockDoubleList<T>
      and could possibly been inserted with same log_id and pre_log_id but different <k,v> pairs.  */
    if (!EntityIDEqual(this->m_entity->pre_log_id(), _other.GetEntity()->pre_log_id()))
        return false;

    //Don't compare their contents, it should be able to being rewrite only by term & idx.
    //(not TODO): improve the comparison by comparing their crc32 values rather than the value itself.
    //return (this->m_entity->write_op().key() == _other.m_entity->write_op().key() && this->m_entity->write_op().value() == _other.m_entity->write_op().value());

    return true;
}

bool MemoryLogItemFollower::operator!=(const MemoryLogItemFollower& _other)const noexcept {
    return !this->operator==(_other);
}

bool MemoryLogItemFollower::operator<=(const MemoryLogItemFollower& _other)const noexcept {
    return EntityIDSmallerEqual(this->m_entity->entity_id(), _other.m_entity->entity_id());
}

bool MemoryLogItemFollower::operator<(const MemoryLogItemFollower& _other)const noexcept {
    return this->MemoryLogItemBase::operator<(_other);
}

bool MemoryLogItemFollower::operator>(const MemoryLogItemFollower& _other)const noexcept {
    return this->MemoryLogItemBase::operator>(_other);
}

bool CmpMemoryLogFollower(const MemoryLogItemFollower& left, const MemoryLogItemFollower& right) noexcept {
    return CmpMemoryLog(&left,&right);
}


}
