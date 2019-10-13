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

#ifndef __AURORA_BIN_LOG_META_DATA_H__
#define __AURORA_BIN_LOG_META_DATA_H__

#include <string>
#include <stdint.h>
#include <memory>
#include <list>

#include "protocol/raft.pb.h"

#include "common/comm_defs.h"
#include "common/log_identifier.h"
#include "tools/lock_free_hash.h"

#define _FILE_META_DATA_MAGIC_STR_ "!@#$binlog$#@!"

namespace RaftCore::BinLog {

using ::RaftCore::Common::LogIdentifier;
using ::RaftCore::Common::TypeBufferInfo;
using ::RaftCore::DataStructure::HashTypeBase;

class FileMetaData final{

public:

    struct IdxPair final : LogIdentifier , HashTypeBase<IdxPair>{

        uint32_t    m_offset;
        uint32_t    key_crc32;
        uint32_t    value_crc32;

        IdxPair(uint32_t _a, uint64_t _b, uint32_t _c, uint32_t _d, uint32_t _e);

        virtual bool operator<(const IdxPair& _other)const noexcept override;

        bool operator==(const IdxPair &_other) const noexcept override;

        const IdxPair& operator=(const IdxPair &_other) noexcept override;

        std::string ToString()const noexcept;

        virtual std::size_t Hash() const noexcept override;

        virtual bool operator>(const IdxPair& _other)const noexcept;

        virtual bool operator>(const LogIdentifier& _other)const noexcept;

        virtual bool operator<(const LogIdentifier& _other)const noexcept;

        virtual bool operator<=(const LogIdentifier& _other)const noexcept;

        virtual bool operator>=(const LogIdentifier& _other)const noexcept;

        virtual bool operator<(const ::raft::EntityID &_other)const noexcept;

        virtual bool operator==(const ::raft::EntityID &_other)const noexcept;

        virtual bool operator!=(const ::raft::EntityID &_other)const noexcept;

        virtual bool operator>(const ::raft::EntityID &_other)const noexcept;
    };

    typedef std::list<std::shared_ptr<FileMetaData::IdxPair>>   TypeOffsetList;

public:

    FileMetaData() noexcept;

    virtual ~FileMetaData() noexcept;

    /* This functions will be invoked each time we write a new log entry into the binlog file,
      namely,in a multiple threading access environment.
      - Support Multiple Thread invoking: True
      - Will be invoked simultaneously: False */
    void AddLogOffset(uint32_t term,uint64_t log_index, uint32_t file_offset,uint32_t key_crc32,uint32_t value_crc32) noexcept;

    /*- Support Multiple Thread invoking: True
      - Will be invoked simultaneously: False */
    void AddLogOffset(const TypeOffsetList &_list) noexcept;

    /*- Support Multiple Thread invoking: True
      - Will be invoked simultaneously: False */
    void Delete(const IdxPair &_item) noexcept;

    /*This functions will be invoked only in the rotating file scenario,namely,
     a single thread accessing environment.After return the pointer pointing
     to the generated buffer, this function will no longer be responsible
     for the memory it allocated, the caller must take care of(freeing) it.

    - Support Multiple Thread invoking: True
    - Will be invoked simultaneously: False */
    TypeBufferInfo GenerateBuffer() const noexcept;

    /* - Support Multiple Thread invoking: True
       - Will be invoked simultaneously: False */
    void ConstructMeta(const unsigned char* _buf, std::size_t _size) noexcept;

    /* - Support Multiple Thread invoking: False
       - Will be invoked simultaneously: False */
    /*Return value:
        0 - parsed successfully.
        >0 - parsed bytes.Remaining bytes are not parsable.Need to be truncated.  */
    int ConstructMeta(std::FILE* _handler) noexcept;

    /* - Support Multiple Thread invoking: True
       - Will be invoked simultaneously: False */
    void GetOrderedMeta(TypeOffsetList &_output) const noexcept;

    void BackOffset(int offset) noexcept;

private:

    ::RaftCore::DataStructure::LockFreeHash<IdxPair>    m_meta_hash;

private:

    FileMetaData(const FileMetaData&) = delete;

    FileMetaData& operator=(const FileMetaData&) = delete;

};

std::ostream& operator<<(std::ostream& os, const FileMetaData::IdxPair& obj);

} //end namespace


#endif
