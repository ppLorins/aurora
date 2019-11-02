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

#ifndef __AURORA_BIN_LOG_OPERATOR_H__
#define __AURORA_BIN_LOG_OPERATOR_H__

#include <string>
#include <stdint.h>
#include <memory>

#include "protocol/raft.pb.h"

#include "common/comm_defs.h"
#include "common/log_identifier.h"
#include "follower/memory_log_follower.h"
#include "binlog/binlog_meta_data.h"

#define _AURORA_BINLOG_NAME_  "raft.binlog"
#define _AURORA_BINLOG_NAME_REG_  "raft\\.binlog"
#define _AURORA_BINLOG_OP_MODE_ "ab+"
#define _AURORA_BINLOG_READ_MODE_ "rb"

namespace RaftCore::BinLog {

using ::raft::Entity;
using ::RaftCore::Common::LogIdentifier;
using ::RaftCore::Follower::TypeMemlogFollowerList;
using ::RaftCore::Common::TypeEntityList;
using ::RaftCore::Common::TypeBufferInfo;
using ::RaftCore::BinLog::FileMetaData;

class BinLogOperator final{

public:

    enum class BinlogErrorCode {
        UNKNOWN = 0,
        SUCCEED_TRUNCATED,
        SUCCEED_MERGED, //Input logs are all existed in the bin log.

        //------Separating line.------//
        SUCCEED_MAX,

        OVER_BOUNDARY,
        NO_CONSISTENT_ERROR,
        OTHER_ERROR,
    };

    enum class BinlogStatus {

        //Normal status , could be written new log entries
        NORMAL,

        //Reverting due a log conflict with (probably new) leader's log.
        REVERTING,

        //Set the first log entry in the binlog file, used in the SyncData scenario.
        SETTING_HEAD
    };

public:

    BinLogOperator();

    virtual ~BinLogOperator()noexcept;

    void Initialize(const char* role, bool file_name = false) noexcept;

    void UnInitialize() noexcept;

    //The '_input' parameter must be ordered by log index in ascending order.
    bool AppendEntry(const TypeEntityList &input_entities,bool force=false) noexcept;

    /*Although it is supported, reverting log actually won't be executed with AppendEntry simultaneously ,
      which is guaranteed by the callers.*/
    BinlogErrorCode RevertLog(TypeMemlogFollowerList &log_list, const LogIdentifier &boundary) noexcept;

    /*Although it is supported, SetHead log actually won't be executed with AppendEntry simultaneously ,
      which is done by the callers.*/
    BinlogErrorCode SetHead(std::shared_ptr<::raft::Entity> _shp_entity) noexcept;

    bool Clear() noexcept;

    LogIdentifier GetLastReplicated() noexcept;

    std::string GetBinlogFileName() noexcept;

    void GetOrderedMeta(FileMetaData::TypeOffsetList &_output) noexcept;

    void AddPreLRLUseCount() noexcept;

    void SubPreLRLUseCount() noexcept;

private:

    void ParseFile() noexcept;

    void DeleteOpenBinlogFile() noexcept;

    void RenameOpenBinlogFile() noexcept;

    void Truncate(uint32_t new_size) noexcept;

    //The following three member functions are helpers of AppendEntry.
    TypeBufferInfo PrepareBuffer(const TypeEntityList &input_entities, FileMetaData::TypeOffsetList &offset_list) noexcept;

    void AppendBufferToBinlog(TypeBufferInfo &buffer_info, const FileMetaData::TypeOffsetList &offset_list) noexcept;

    void RotateFile() noexcept;

private:

    LogIdentifier                m_zero_log_id;

    bool                         m_initialized = false;

    std::atomic<LogIdentifier>   m_last_logged;

    std::string                  m_file_name = "";

    std::FILE                    *m_binlog_handler = nullptr;

    std::mutex                   m_cv_mutex;

    std::condition_variable      m_cv;

    std::atomic<FileMetaData*>   m_p_meta;

    std::atomic<BinlogStatus>    m_binlog_status;

    std::atomic<int>             m_precede_lcl_inuse;

    std::atomic<uint32_t>        m_log_num;

private:

    BinLogOperator(const BinLogOperator&) = delete;

    BinLogOperator& operator=(const BinLogOperator&) = delete;

};



} //end namespace


#endif
