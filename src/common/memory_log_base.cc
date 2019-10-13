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

#include  "common/memory_log_base.h"

namespace RaftCore::Common {

MemoryLogItemBase::MemoryLogItemBase(uint32_t _term, uint64_t _index) noexcept{
    auto p_obj = new ::raft::Entity();
    auto _p_entity_id = p_obj->mutable_entity_id();
    _p_entity_id->set_term(_term);
    _p_entity_id->set_idx(_index);

    this->m_entity.reset(p_obj);
}

MemoryLogItemBase::MemoryLogItemBase(const ::raft::Entity &_entity)noexcept {
    /*This is where memory copy overhead occurs.Because the content of AppendEntriesRequest
    object need to be retained until next CommitEntries RPC call.  */
    this->m_entity.reset(new ::raft::Entity(_entity));
}

MemoryLogItemBase::~MemoryLogItemBase() noexcept{}

std::shared_ptr<::raft::Entity> MemoryLogItemBase::GetEntity()const noexcept {
    return m_entity;
}

bool MemoryLogItemBase::operator<(const MemoryLogItemBase &_other) const noexcept{

    if (this->m_entity->entity_id().term() < _other.m_entity->entity_id().term())
        return true;

    if (this->m_entity->entity_id().term() > _other.m_entity->entity_id().term())
        return false;

    return this->m_entity->entity_id().idx() < _other.m_entity->entity_id().idx();
}

bool MemoryLogItemBase::operator==(const MemoryLogItemBase& _other)const noexcept {

    return (this->m_entity->entity_id().term() == _other.m_entity->entity_id().term() &&
        this->m_entity->entity_id().idx() == _other.m_entity->entity_id().idx());
}

bool MemoryLogItemBase::operator!=(const MemoryLogItemBase& _other)const noexcept {
    return !this->operator==(_other);
}

bool MemoryLogItemBase::operator>(const MemoryLogItemBase& _other)const noexcept {

    if (this->m_entity->entity_id().term() > _other.m_entity->entity_id().term())
        return true;

    if (this->m_entity->entity_id().term() < _other.m_entity->entity_id().term())
        return false;

    return this->m_entity->entity_id().idx() > _other.m_entity->entity_id().idx();
}

bool MemoryLogItemBase::AfterOf(const MemoryLogItemBase& _other)const noexcept {
    return (this->m_entity->pre_log_id().term() == _other.m_entity->entity_id().term() &&
        this->m_entity->pre_log_id().idx() == _other.m_entity->entity_id().idx());
}

bool CmpMemoryLog(const MemoryLogItemBase *left, const MemoryLogItemBase *right) noexcept {
    return left->GetEntity()->entity_id().term() == right->GetEntity()->pre_log_id().term() &&
            left->GetEntity()->entity_id().idx() == right->GetEntity()->pre_log_id().idx() ;
}

}
