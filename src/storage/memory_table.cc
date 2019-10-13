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

#include "config/config.h"
#include "storage/memory_table.h"

namespace RaftCore::Storage {

using ::RaftCore::Storage::TypePtrHashableString;

HashValue::HashValue(uint32_t a, uint64_t b, const std::string &c) {
    this->m_term  = a;
    this->m_index = b;
    this->m_val = std::make_shared<std::string>(c);
}

bool HashValue::operator<(const HashValue &other)const noexcept {
    if (this->m_term < other.m_term)
        return true;

    if (this->m_term > other.m_term)
        return false;

    return this->m_index < other.m_index;
}

MemoryTable::MemoryTable() noexcept {
    this->m_shp_records = std::make_shared<TypeRecords>(::RaftCore::Config::FLAGS_memory_table_hash_slot_num);
}

MemoryTable::~MemoryTable() noexcept {}

void MemoryTable::Insert(const std::string &key,const std::string &val,uint32_t term,uint64_t index) noexcept{

    //Memory copy overhead,can't not get around.
    TypePtrHashableString  _shp_key = std::make_shared<HashableString>(key);
    TypePtrHashValue       _shp_val = std::make_shared<HashValue>(term, index, val);

    this->m_shp_records->Insert(_shp_key,_shp_val);
}

void MemoryTable::IterateByKey(std::function<bool(const TypePtrHashableString&, const TypePtrHashValue&)> op) const noexcept {

    std::list<TypePtrHashableString> _ordered_meta;
    this->m_shp_records->GetOrderedByKey(_ordered_meta);

    for (const auto &_meta : _ordered_meta) {
        TypePtrHashValue _shp_val;
        this->m_shp_records->Read(*_meta, _shp_val);
        if (!op(_meta, _shp_val))
            break;
    }
}

bool MemoryTable::IterateByVal(std::function<bool(const HashValue&, const HashableString&)> op) const noexcept {

    LockFreeHash<HashableString, HashValue>::ValueComparator _cmp = [](const TypePtrHashValue &left, const TypePtrHashValue &right)->bool { return *left < *right; };

    std::map<std::shared_ptr<HashValue>, std::shared_ptr<HashableString>,decltype(_cmp)> _ordered_by_value_map(_cmp);

    this->m_shp_records->GetOrderedByValue(_ordered_by_value_map);

    for (const auto &_item : _ordered_by_value_map)
        if (!op(*_item.first, *_item.second))
            return false;

    return true;
}

bool MemoryTable::GetData(const std::string &key,std::string &val) const noexcept {

    TypePtrHashValue _shp_val;
    if (!this->m_shp_records->Read(HashableString(key,true), _shp_val))
        return false;

    val = *_shp_val->m_val;
    return true;
}

std::size_t MemoryTable::Size() const noexcept {
    return this->m_shp_records->Size();
}

}
