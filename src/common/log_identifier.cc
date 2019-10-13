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

#include  "common/log_identifier.h"

namespace RaftCore::Common {

void LogIdentifier::Set(const LogIdentifier &_other) noexcept{
    this->m_term  = _other.m_term;
    this->m_index = _other.m_index;
}

void LogIdentifier::Set(uint32_t term,uint64_t index) noexcept{
    this->m_term  = term;
    this->m_index = index;
}

uint32_t LogIdentifier::GreaterThan(const LogIdentifier& _other) const noexcept {
    if (this->operator<(_other))
        return -1;

    if (this->m_term > _other.m_term)
        return 0x7FFFFFFF;

    return (uint32_t)(this->m_index - _other.m_index);
}

bool LogIdentifier::operator==(const LogIdentifier& _other) const noexcept{
    return (_other.m_term == this->m_term && _other.m_index == this->m_index);
}

bool LogIdentifier::operator!=(const LogIdentifier& _other) const noexcept{
    return !(_other == *this);
}

bool LogIdentifier::operator< (const LogIdentifier &_other) const noexcept{
    if (this->m_term < _other.m_term)
        return true;

    if (this->m_term > _other.m_term)
        return false;

    return this->m_index < _other.m_index;
}

bool LogIdentifier::operator<= (const LogIdentifier &_other) const noexcept{
    return (this->operator <(_other) || this->operator ==(_other));
}

bool LogIdentifier::operator> (const LogIdentifier &_other) const noexcept{
    if (this->m_term > _other.m_term)
        return true;

    if (this->m_term < _other.m_term)
        return false;

    return this->m_index > _other.m_index;
}

bool LogIdentifier::operator>= (const LogIdentifier &_other) const noexcept{
    return (this->operator >(_other) || this->operator ==(_other));
}

std::string LogIdentifier::ToString() const noexcept{
    return "LogIdentifier term:" + std::to_string(this->m_term) + ",idx:" + std::to_string(this->m_index);
}

LogIdentifier ConvertID(const ::raft::EntityID &entity_id) {
    LogIdentifier _id;
    _id.Set(entity_id.term(),entity_id.idx());
    return _id;
}

std::ostream& operator<<(std::ostream& os, const LogIdentifier& obj) {
    os << "LogIdentifier term:" << obj.m_term << ",idx:" << obj.m_index;
    return os;
}

bool EntityIDEqual(const ::raft::EntityID &left, const LogIdentifier &right) {
    return (left.term() == right.m_term && left.idx() == right.m_index);
}

bool EntityIDLarger(const ::raft::EntityID &left, const LogIdentifier &right) {
    if (left.term() > right.m_term) {
        return true;
    }

    if (left.term() < right.m_term) {
        return false;
    }

    return left.idx() > right.m_index;
}

bool EntityIDLargerEqual(const ::raft::EntityID &left, const LogIdentifier &right) {
    if (left.term() > right.m_term) {
        return true;
    }

    if (left.term() < right.m_term) {
        return false;
    }

    return left.idx() >= right.m_index;
}

bool EntityIDSmaller(const ::raft::EntityID &left, const LogIdentifier &right) {
    if (left.term() < right.m_term) {
        return true;
    }

    if (left.term() > right.m_term) {
        return false;
    }

    return left.idx() < right.m_index;
}

}
