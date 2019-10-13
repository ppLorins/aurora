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

#include "tools/utilities.h"
#include "storage/hashable_string.h"

namespace RaftCore::Storage {

HashableString::HashableString(const std::string &other,bool on_fly) noexcept  {
    //Ignore delete operation.
    if (on_fly)
        this->m_shp_str.reset(const_cast<std::string*>(&other), [](auto *p) {});
    else
        this->m_shp_str = std::make_shared<std::string>(other);
}

HashableString::~HashableString() noexcept {}

bool HashableString::operator<(const HashableString &other)const noexcept {
    return std::strcmp(this->m_shp_str->c_str(), other.m_shp_str->c_str()) < 0;
}

bool HashableString::operator==(const HashableString &other)const noexcept {
    return std::strcmp(this->m_shp_str->c_str(), other.m_shp_str->c_str()) == 0;
}

bool HashableString::operator==(const std::string &other)const noexcept {
    return std::strcmp(this->m_shp_str->c_str(), other.c_str()) == 0;
}

const HashableString& HashableString::operator=(const HashableString &other)noexcept {
    //Deleter will also be transferred.
    this->m_shp_str = other.m_shp_str;
    return *this;
}

std::size_t HashableString::Hash() const noexcept {
    return std::hash<std::string>{}(*this->m_shp_str);
}

const std::string& HashableString::GetStr() const noexcept {
    return *this->m_shp_str;
}

const std::shared_ptr<std::string> HashableString::GetStrPtr() const noexcept {
    return this->m_shp_str;
}

std::size_t PtrHSHasher::operator()(const TypePtrHashableString &shp_hashable_string)const {
    return std::hash<std::string>{}(shp_hashable_string->GetStr());
}

bool PtrHSEqualer::operator()(const TypePtrHashableString &left,const TypePtrHashableString &right)const {
    return left->GetStr() == right->GetStr();
}

}
