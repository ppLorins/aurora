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

#ifndef __AURORA_SSTABLE_H__
#define __AURORA_SSTABLE_H__

#include "common/comm_defs.h"
#include "common/log_identifier.h"
#include "tools/lock_free_hash.h"
#include "storage/hashable_string.h"
#include "storage/memory_table.h"

#define _AURORA_SSTABLE_PREFIX_ "sstable.data"
#define _AURORA_SSTABLE_MERGE_SUFFIX_ ".merged"
#define _AURORA_DATA_DIR_ "data"

namespace RaftCore::Storage {

using ::RaftCore::DataStructure::LockFreeHash;
using ::RaftCore::Common::LogIdentifier;
using ::RaftCore::Storage::HashableString;

class SSTAble final{

public:

    struct Meta {

        Meta(uint32_t a, uint16_t b, uint32_t c, uint16_t d,uint32_t e,uint64_t f);

        uint32_t  m_key_offset;
        uint16_t  m_key_len;
        uint32_t  m_val_offset;
        uint16_t  m_val_len;

        uint32_t  m_term;
        uint64_t  m_index;

        bool operator<(const Meta &other);
    };

    typedef std::shared_ptr<Meta>  TypePtrMeta;

public:

    SSTAble(const char* file) noexcept;

    SSTAble(const SSTAble &from,const SSTAble &to) noexcept;

    SSTAble(const MemoryTable &src) noexcept;

    virtual ~SSTAble() noexcept;

    bool Read(const std::string &key, std::string &val) const noexcept;

    LogIdentifier GetMaxLogID() const noexcept;

    LogIdentifier GetMinLogID() const noexcept;

    const std::string& GetFilename() const noexcept;

    bool IterateByVal(std::function<bool(const Meta &meta,const HashableString &key)> op) const noexcept;

private:

    void CreateFile(const char* file_name = nullptr) noexcept;

    void ParseFile() noexcept;

    void ParseMeta(unsigned char* &allocated_buf,std::size_t meta_len) noexcept;

    void DumpFrom(const MemoryTable &src) noexcept;

    void AppendKvPair(const TypePtrHashableString &key, const TypePtrHashValue &val, void* buf,
                uint32_t buff_len, uint32_t &buf_offset, uint32_t &file_offset) noexcept;

    void AppendMeta(const TypePtrHashableString &key, const TypePtrMeta &shp_meta, void* buf,
                uint32_t buff_len, uint32_t &buf_offset, uint32_t &file_offset) noexcept;

    void CalculateMetaOffset() noexcept;

    void AppendChecksum(uint32_t checksum) noexcept;

    void AppendMetaOffset() noexcept;

    void AppendFooter() noexcept;

private:

    uint32_t  m_record_crc = 0;

    uint32_t  m_meta_crc = 0;

    long    m_meta_offset = 0;

    typedef LockFreeHash<HashableString, Meta>  TypeOffset;
    typedef std::shared_ptr<TypeOffset>  TypePtrOffset;

    TypePtrOffset      m_shp_meta;

    LogIdentifier   m_max_log_id;

    LogIdentifier   m_min_log_id;

    std::string     m_associated_file = "";

    std::FILE   *m_file_handler = nullptr;

    static const int m_single_meta_len = _FOUR_BYTES_ * 3 + _TWO_BYTES_ * 2 + _EIGHT_BYTES_;

private:

    SSTAble(const SSTAble&) = delete;

    SSTAble& operator=(const SSTAble&) = delete;

};

} //end namespace

#endif
