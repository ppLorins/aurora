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

#ifndef __AURORA_MEMORY_TABLE_H__
#define __AURORA_MEMORY_TABLE_H__

#include "common/comm_defs.h"
#include "tools/lock_free_hash.h"
#include "storage/hashable_string.h"

namespace RaftCore::Storage {

using ::RaftCore::DataStructure::LockFreeHash;
using ::RaftCore::Storage::HashableString;

struct HashValue {

    HashValue(uint32_t a, uint64_t b, const std::string &c);

    bool operator<(const HashValue &other)const noexcept;

    uint32_t  m_term;
    uint64_t  m_index;
    std::shared_ptr<std::string>  m_val;
};

typedef std::shared_ptr<HashValue>  TypePtrHashValue;
typedef LockFreeHash<HashableString, HashValue>  TypeRecords;
typedef std::shared_ptr<TypeRecords>  PtrRecords;

class MemoryTable final{

public:

    MemoryTable() noexcept;

    virtual ~MemoryTable() noexcept;

    void Insert(const std::string &key, const std::string &val, uint32_t term, uint64_t index) noexcept;

    void IterateByKey(std::function<bool(const TypePtrHashableString&,const TypePtrHashValue&)> op) const noexcept;

    bool IterateByVal(std::function<bool(const HashValue&, const HashableString&)> op) const noexcept;

    bool GetData(const std::string &key, std::string &val) const noexcept;

    std::size_t Size() const noexcept;

private:

    PtrRecords   m_shp_records;

private:

    MemoryTable(const MemoryTable&) = delete;

    MemoryTable& operator=(const MemoryTable&) = delete;

};

typedef std::shared_ptr<MemoryTable>    TypePtrMemoryTable;

} //end namespace

#endif
