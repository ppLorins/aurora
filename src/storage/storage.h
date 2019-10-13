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

#ifndef __AURORA_STORAGE_H__
#define __AURORA_STORAGE_H__

#include <unordered_map>
#include <mutex>

#include "boost/filesystem.hpp"

#include "common/comm_defs.h"
#include "common/log_identifier.h"
#include "tools/lock_free_unordered_single_list.h"
#include "storage/sstable.h"

namespace RaftCore::Storage {

using ::RaftCore::Common::LogIdentifier;
using ::RaftCore::Common::WriteLock;
using ::RaftCore::DataStructure::AtomicPtrSingleListNode;
using ::RaftCore::DataStructure::UnorderedSingleListNode;
using ::RaftCore::Storage::MemoryTable;;
namespace  fs = ::boost::filesystem;

class StorageMgr final{

public:

    struct StorageItem {

        StorageItem(const LogIdentifier &log_id,const std::shared_ptr<std::string> &key,
                    const std::shared_ptr<std::string> &value) : m_log_id(log_id),m_key(key),m_value(value) {}

        LogIdentifier                   m_log_id;

        //Ownership of the following two std::shared_ptr can be taken over.
        std::shared_ptr<std::string>    m_key;
        std::shared_ptr<std::string>    m_value;
    };

public:

    StorageMgr() noexcept;

    virtual ~StorageMgr() noexcept;

    bool Initialize(const char* role, bool reset = false) noexcept;

    void UnInitialize() noexcept;

    //Delete all data both in memory & disk.
    void Reset() noexcept;

    bool Get(const std::string &key, std::string &val) const noexcept;

    bool Set(const LogIdentifier &log_id, const std::string &key, const std::string &value)  noexcept;

    const LogIdentifier GetLastCommitted() const noexcept;

    /*Note: return the `step_len` number of records greater than start_at.If start_at is earlier than the oldest item in the storage,
        return the earliest step_len records.  */
    void GetSlice(const LogIdentifier& start_at,uint32_t step_len,std::list<StorageItem> &output_list) const noexcept;

    void PurgeGarbage() noexcept;

    static void FindRoleBinlogFiles(const std::string &role, std::list<std::string> &output);

private:

    bool ConstructFromBinlog(const LogIdentifier &from, const std::string &binlog_file_name) noexcept;

    void GetSliceInSSTable(const LogIdentifier& start_at, int step_len, std::list<StorageItem> &output_list) const noexcept;

    void GetSliceInMemory(const LogIdentifier& start_at, int step_len, std::list<StorageItem> &output_list) const noexcept;

    void ClearInMemoryData() noexcept;

    //return : indicating if purging can proceed or not.
    bool PurgeSSTable() noexcept;

    void PurgeMemoryTable() noexcept;

    void DumpMemoryTable(const MemoryTable *src)  noexcept;

    void ConstructMemoryTable(const LogIdentifier &from) noexcept;

    void RecycleLast2SStables() noexcept;

    template<typename T>
    void ReleaseData(AtomicPtrSingleListNode<T> &head) noexcept {
        //Releasing process should be mutual exclusion from purging process.
        WriteLock _w_lock(this->m_mutex);
        auto *_p_cur = head.load();
        while (_p_cur != nullptr) {
            auto _tmp = _p_cur;
            _p_cur = _p_cur->m_next.load();
            //delete _tmp->m_data;
            delete _tmp;
        }
        head.store(nullptr);
    }

private:

    bool        m_initialized = false;

    std::string     m_role_str = "";

    fs::path    m_path;

    std::atomic<LogIdentifier>          m_last_committed;

    std::atomic<LogIdentifier>          m_last_persist;

    /*There are several special operations for the followings, so use the raw version of single list
      instead of the wrapped version 'LockFreeUnorderedSingleList'.  */
    AtomicPtrSingleListNode<MemoryTable>        m_memory_table_head;

    AtomicPtrSingleListNode<MemoryTable>        m_garbage_memory_table;

    AtomicPtrSingleListNode<SSTAble>            m_sstable_table_head;

    AtomicPtrSingleListNode<SSTAble>            m_garbage_sstable;

    std::shared_timed_mutex     m_mutex;

    std::list<std::string>      m_loaded_binlog_files;

private:

    StorageMgr(const StorageMgr&) = delete;

    StorageMgr& operator=(const StorageMgr&) = delete;

};

} //end namespace

#endif
