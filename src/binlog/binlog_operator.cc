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

#include "boost/crc.hpp"
#include "boost/filesystem.hpp"

#include "config/config.h"
#include "tools/utilities.h"
#include "leader/leader_view.h"
#include "binlog/binlog_operator.h"
#include "storage/storage_singleton.h"

namespace RaftCore::BinLog {

//To avoid issues caused by including header files mutually.
namespace  fs = ::boost::filesystem;
using ::raft::EntityID;
using ::RaftCore::Leader::LeaderView;
using ::RaftCore::Common::ConvertID;
using ::RaftCore::Storage::StorageGlobal;

BinLogOperator::BinLogOperator() {}

BinLogOperator::~BinLogOperator()noexcept {
    if (this->m_initialized)
        this->UnInitialize();
}

void BinLogOperator::Initialize(const char* role, bool file_name) noexcept {

    CHECK(role != nullptr);

    if (file_name)
        this->m_file_name = role;
    else
        this->m_file_name = _AURORA_BINLOG_NAME_ + std::string(".") + role;

    this->m_binlog_handler = std::fopen(this->m_file_name.c_str(),_AURORA_BINLOG_OP_MODE_);
    CHECK(this->m_binlog_handler != nullptr) << "open binlog file " << this->m_file_name << "fail..,errno:" << errno;

    this->ParseFile();

    std::list<std::shared_ptr<FileMetaData::IdxPair>> _cur_file_meta;
    m_p_meta.load()->GetOrderedMeta(_cur_file_meta);

    LogIdentifier _last;
    _last.Set(0,0);
    if (!_cur_file_meta.empty())
        _last.Set(_cur_file_meta.back()->m_term,_cur_file_meta.back()->m_index);

    this->m_last_logged.store(_last, std::memory_order_release);
    this->m_binlog_status.store(BinlogStatus::NORMAL);
    this->m_log_num = (uint32_t)_cur_file_meta.size();

    this->m_precede_lcl_inuse.store(0);

    this->m_initialized = true;
}

void BinLogOperator::AddPreLRLUseCount() noexcept {
    this->m_precede_lcl_inuse.fetch_add(1);
}

void BinLogOperator::SubPreLRLUseCount() noexcept {
    this->m_precede_lcl_inuse.fetch_sub(1);
}

void BinLogOperator::ParseFile() noexcept{

    //Construct the meta data if not exist, otherwise just load it into RAM
    m_p_meta.store(new FileMetaData());

    CHECK(std::fseek(this->m_binlog_handler, 0, SEEK_END) == 0) << "seek binlog file "
                  << this->m_file_name << "fail..,errno:" << errno;

    long _file_size = std::ftell(this->m_binlog_handler);
    CHECK(_file_size >= 0) << "tell binlog file " << this->m_file_name << "fail..,errno:" << errno;

    if (_file_size == 0) {
        //Empty file,just skip the following parsing meta steps.
        return;
    }

    long _file_tail_len = _FOUR_BYTES_ * 3;

    int _minimal_pb_section_size   = _FOUR_BYTES_; //Only a four-bytes length field .
    int _minimal_meta_section_size = _FOUR_BYTES_ + _file_tail_len; //Only the tailing part and a 0-length tlv field.

    CHECK(_file_size >= _minimal_pb_section_size + _minimal_meta_section_size) << "binlog file size not correct:" << _file_size;

    //First read the last 12 bytes area.
    CHECK(std::fseek(this->m_binlog_handler, (_file_size - _file_tail_len), SEEK_SET) == 0) << "seek binlog file "
               << this->m_file_name << "fail..,errno:" << errno;

    char sz_last_zone[12] = {0};
    CHECK(std::fread(sz_last_zone, 1, 12, this->m_binlog_handler)==12) << "read binlog file " << this->m_file_name << "last zone fail..,errno:" << errno;

    //This uncompleted file ending is probably due to an unexpected crash.
    static std::size_t _footer_len = std::strlen(_FILE_META_DATA_MAGIC_STR_);
    if (strncmp((const char*)&sz_last_zone[8], _FILE_META_DATA_MAGIC_STR_, _footer_len) != 0) {

        CHECK(std::fseek(this->m_binlog_handler,0, SEEK_SET) == 0) << "seek binlog file "
                    << this->m_file_name << "fail..,errno:" << errno;

        //Since the file has no meta data , we have to construct it all over again.
        int _parsed_bytes = m_p_meta.load()->ConstructMeta(this->m_binlog_handler);
        if (_parsed_bytes > 0) {
            LOG(WARNING) << "binlog incomplete data found , file corruption probably happened,now truncating it.";
            this->Truncate(_parsed_bytes);
        }

        return;
    }

    //Meta data exists , just parse it.
    uint32_t _meta_data_len = *((uint32_t*)&sz_last_zone[4]);
    ::RaftCore::Tools::ConvertBigEndianToLocal<uint32_t>(_meta_data_len, &_meta_data_len);

    CHECK(std::fseek(this->m_binlog_handler,(_file_size - _meta_data_len), SEEK_SET) == 0) << "seek binlog file "
                    << this->m_file_name << "fail..,errno:" << errno;

    auto _ptr_meta_buf = (unsigned char*)malloc(_meta_data_len);
    CHECK(std::fread(_ptr_meta_buf, 1,_meta_data_len, this->m_binlog_handler) == _meta_data_len)
                << "read binlog meta data fail...,errno:" << errno;

    uint32_t crc_in_file = *(uint32_t*)&sz_last_zone[0];
    ::RaftCore::Tools::ConvertBigEndianToLocal<uint32_t>(crc_in_file, &crc_in_file);

    //CRC32 only calculate the 1>meta data area.
    uint32_t _crc32_value = ::RaftCore::Tools::CalculateCRC32(_ptr_meta_buf, _meta_data_len - 12);

    CHECK(crc_in_file == _crc32_value) << "binlog file meta check checksum failed";

    m_p_meta.load()->ConstructMeta(_ptr_meta_buf,_meta_data_len);
    free(_ptr_meta_buf);
}

void BinLogOperator::UnInitialize() noexcept{

    auto _p_tmp = m_p_meta.load();
    delete _p_tmp;
    m_p_meta.store(nullptr);

    this->m_initialized = false;

    if (this->m_binlog_handler == nullptr)
        return ;

    if (!std::fclose(this->m_binlog_handler))
        return; //normally return.

    //Something bad happened.
    CHECK(false) << "close binlog file fail...,errno:"<< errno;
}

void BinLogOperator::Truncate(uint32_t new_size) noexcept {

    CHECK(std::fclose(this->m_binlog_handler)==0) << "truncating file fclose failed";
    fs::resize_file(fs::path(this->m_file_name),new_size);

    //Re-open file.
    this->m_binlog_handler = std::fopen(this->m_file_name.c_str(),_AURORA_BINLOG_OP_MODE_);
    CHECK(this->m_binlog_handler != nullptr) << "re-open binlog file after truncate fail..";
}

TypeBufferInfo BinLogOperator::PrepareBuffer(const TypeEntityList &input_entities, FileMetaData::TypeOffsetList &offset_list) noexcept {

    unsigned char* _p_buf = nullptr;
    std::size_t _buf_size = 0;

    auto ms = std::chrono::duration_cast< std::chrono::milliseconds >(
        std::chrono::system_clock::now().time_since_epoch() );

    uint64_t ts = ms.count();

    std::list<std::string> _buf_list;
    uint32_t _cur_offset = 0; //Offset relative to this append operation.
    for (const auto & _shp_entity : input_entities) {

        auto _binlogitem = ::raft::BinlogItem();
        _binlogitem.set_allocated_entity(_shp_entity.get());
        _binlogitem.set_timestamp_ms(ts);

        std::string _output = "";
        CHECK(_binlogitem.SerializeToString(&_output)) <<  "SerializeToString fail...,log entity:"
                << _shp_entity->entity_id().term() << "|" << _shp_entity->entity_id().idx();

        _binlogitem.release_entity();

        const auto & _wop = _shp_entity->write_op();
        uint32_t _key_crc32   = ::RaftCore::Tools::CalculateCRC32((void*)_wop.key().data(),(unsigned int)_wop.key().length());
        uint32_t _value_crc32 = ::RaftCore::Tools::CalculateCRC32((void*)_wop.value().data(),(unsigned int)_wop.value().length());

        uint32_t _this_buf_size = (uint32_t)_output.size() + _FOUR_BYTES_;
        _buf_size += _this_buf_size;
        _buf_list.emplace_back(std::move(_output));

        //Recording offset info.
        offset_list.emplace_back(new FileMetaData::IdxPair(_shp_entity->entity_id().term(),_shp_entity->entity_id().idx(), _cur_offset,_key_crc32,_value_crc32));
        _cur_offset += _this_buf_size;
    }

    _p_buf = (unsigned char*)malloc(_buf_size);
    auto _pcur = _p_buf;

    for (const auto& _buf : _buf_list) {
        uint32_t _buf_len = (uint32_t)_buf.size();
        ::RaftCore::Tools::ConvertToBigEndian<uint32_t>(uint32_t(_buf_len),&_buf_len);
        std::memcpy(_pcur,(unsigned char*)&_buf_len,_FOUR_BYTES_);
        _pcur += _FOUR_BYTES_;
        std::memcpy(_pcur,_buf.data(),_buf.size());
        _pcur += _buf.size();
    }

    return std::make_tuple(_p_buf, _buf_size);
}

void BinLogOperator::AppendBufferToBinlog(TypeBufferInfo &buffer_info, const FileMetaData::TypeOffsetList &offset_list) noexcept {

    //First,update this file's meta data.
    long _offset = std::ftell(this->m_binlog_handler);
    CHECK (_offset >= 0) << "ftell binlog file fail...,errno:"<< errno ;

    for (const auto &_item : offset_list) {
        _item->m_offset += _offset;
    }

    m_p_meta.load()->AddLogOffset(offset_list);

    //Second,append buffer to file.
    unsigned char* _p_buf = nullptr;
    int _buf_size = 0;
    std::tie(_p_buf, _buf_size) = buffer_info;

    CHECK(std::fwrite((void*)_p_buf,1,_buf_size,this->m_binlog_handler) == _buf_size) << "fwrite binlog file fail...,errno:"<< errno ;
    CHECK(std::fflush(this->m_binlog_handler) == 0)  << "fflush binlog file fail...,errno:" << errno;

    free(_p_buf);
}

void BinLogOperator::RotateFile() noexcept {

    /*Since file position indicator is always at the tail ,
        and file is opened in binary mode , ftell is exactly the file size.  */
    long file_len = std::ftell(this->m_binlog_handler);
    CHECK(file_len >= 0) << "ftell binlog file fail...,errno:"<< errno ;

    bool _exceed_limits = ((uint32_t)file_len > ::RaftCore::Config::FLAGS_binlog_max_size) ||
                        (this->m_log_num.load() > ::RaftCore::Config::FLAGS_binlog_max_log_num);
    if (!_exceed_limits)
        return;

    int _use_count = this->m_precede_lcl_inuse.load();
    if (_use_count > 0) {
        LOG(INFO) << "somewhere using the pre-lcl part,use count:" << _use_count;
        return;
    }

    //Get the tail meta data need to move to the new binlog file.
    uint32_t _reserve_counter = 0; //#logs before ID-LCL.
    std::list<std::shared_ptr<FileMetaData::IdxPair>> _cur_file_meta;
    std::list<std::shared_ptr<FileMetaData::IdxPair>> _new_file_meta;
    FileMetaData* _p_new_meta  = nullptr;
    m_p_meta.load()->GetOrderedMeta(_cur_file_meta);
    for (auto _iter = _cur_file_meta.crbegin(); _iter != _cur_file_meta.crend(); ) {

        /*We need to preserve more log entries . There are reasons for this:

          1. Need to preserve the LCL in the new binlog, because it will be used in reverting
             log scenario as the 'pre_log_id'.
          2. In some extreme cases, the resync log procedure may need log entries from leader's
            binlog file that are even earlier than the LCL. E.g., a follower coming back to the
            normal connected status of the cluster after a relatively long status of disconnected.
            At which point , its logs falling a lot behind the leader's and need to resync a lot of entries.
          */
        bool _less_than_LCL  = (*_iter)->operator<(StorageGlobal::m_instance.GetLastCommitted());
        _reserve_counter += (_less_than_LCL) ? 1 : 0 ;
        bool _overflow_reserve_num = _reserve_counter > ::RaftCore::Config::FLAGS_binlog_reserve_log_num;

        if ( _less_than_LCL && _overflow_reserve_num )
            break;

        _new_file_meta.emplace_front(*_iter);

        //Delete meta data that are smaller than (ID-LCL - FLAGS_reserve_log_num).
        m_p_meta.load()->Delete(**_iter);

        //Insert current iterating meta-data item to the new binlog file's meta-data list.
        if (_p_new_meta == nullptr)
            _p_new_meta = new FileMetaData();
        _p_new_meta->AddLogOffset((*_iter)->m_term,(*_iter)->m_index,(*_iter)->m_offset,(*_iter)->key_crc32,(*_iter)->value_crc32);

        _iter++;
    }

    //If the trailing part actually exists , remove it.
    unsigned char* _p_tail_logs_buf  = nullptr;
    long _tail_size = 0;

    //Note: _new_file_meta will contain log start from,i.e.,>= (ID-LCL - FLAGS_reserve_log_num).
    std::shared_ptr<FileMetaData::IdxPair> _shp_front = _new_file_meta.front();

    //Copy tail meta data to the new binlog file.
    CHECK(std::fseek(this->m_binlog_handler,_shp_front->m_offset,SEEK_SET)==0);

    long _front_size = std::ftell(this->m_binlog_handler);
    CHECK(_front_size >= 0) << "tell binlog file " << this->m_file_name << "fail..,errno:" << errno;
    _tail_size = file_len - _front_size;

    _p_tail_logs_buf = (unsigned char *)malloc(_tail_size);

    CHECK(std::fread(_p_tail_logs_buf,1,_tail_size,this->m_binlog_handler)==_tail_size) << "read binlog file "
        << this->m_file_name << " tail fail..,errno:" << errno;

    this->Truncate(_shp_front->m_offset);

    //Writing meta data to the end of binlog file, close it after that.
    uint32_t _buf_size = 0;
    unsigned char * _p_meta_buf = nullptr;
    std::tie(_p_meta_buf, _buf_size) = m_p_meta.load()->GenerateBuffer();

    CHECK(std::fwrite(_p_meta_buf, _buf_size, 1, this->m_binlog_handler) == 1) << "fwrite binlog file fail...,errno:"<< errno ;
    free(_p_meta_buf);
    CHECK (std::fflush(this->m_binlog_handler) == 0 )  << "fflush meta data to end of binlog file fail...";

    //Rename & re-open.
    this->RenameOpenBinlogFile();

    //Write the tail logs to the new binlog file
    if (_p_tail_logs_buf != nullptr) {
        CHECK(fwrite((void*)_p_tail_logs_buf,_tail_size,1,this->m_binlog_handler) ==1)<< "fwrite binlog file fail...,errno:"<< errno ;
        CHECK (std::fflush(this->m_binlog_handler) == 0)  << "fflush binlog file fail...,errno:" << errno;
    }

    //New file meta offset are relative to the old binlog file,adjust it to the new binlog file.
    _p_new_meta->BackOffset(_front_size);

    //Release and re-allocate meta data buf.
    auto *_p_tmp = m_p_meta.load();
    delete _p_tmp;
    m_p_meta.store(_p_new_meta);
}

bool BinLogOperator::AppendEntry(const TypeEntityList &input_entities,bool force) noexcept{

    //TODO: remove test code
    //this->m_last_logged.store(ConvertID(input_entities.back()->entity_id()), std::memory_order_release);
    //return true;

    /* Prerequisite: There is no way for two or more threads calling this method
                      parallel with the same pre_log(guaranteed by the previous generating
                      guid step). Otherwise, terrible things could happen.  */

    if (!force && this->m_binlog_status.load() != BinlogStatus::NORMAL) {
        LOG(ERROR) << "binlog status wrong:" << int(this->m_binlog_status.load());
        return false;
    }

    if (input_entities.empty())
        return true;

    //For some legacy reason , we represent pre_log like this...
    const auto &pre_log = input_entities.front()->pre_log_id();

    //Log offsets to update.
    FileMetaData::TypeOffsetList _offset_list;
    auto _buf_info = this->PrepareBuffer(input_entities,_offset_list);

    {
        std::unique_lock<std::mutex> _mutex_lock(m_cv_mutex);

        /* Having to be sure that the last log entry which has been written to the binlog file
           is exactly the one prior to the log that we're currently going to write .
           Meaning that we have to wait until the last log caught up with the pre_log.

           Figured out two approaches here:
           A. use CAS weak version to wait.
           B. use condition variable to wait.

           Since approach A will consume a lot of CPU times ,I choose approach B.  */
        while (!EntityIDEqual(pre_log, this->m_last_logged.load(std::memory_order_consume))) {

            auto wait_cond = [&]()->bool {return EntityIDEqual(pre_log,this->m_last_logged.load(std::memory_order_consume)); };

            /* During high request load,many threads will be blocked here , and only one could
                go further after some other threads called cv.notify_all */
            bool waiting_result = m_cv.wait_for(_mutex_lock, std::chrono::microseconds(::RaftCore::Config::FLAGS_binlog_append_file_timeo_us), wait_cond);
            if (!waiting_result) {
                LOG(WARNING) << "timeout during append ,current ID-LRL logID: " << this->m_last_logged.load(std::memory_order_consume) << ",waiting on previous id :" <<  ConvertID(pre_log)
                        << ", this shouldn't exist too much, and will resolve quickly.";
                /*Just continuous waiting, no need to return false or something like that . Threads who got here must finish the appending process.*/
                continue;
            }
            break;
        }

        //Note:Only one thread could reading here.

        //The following two steps can be merged into one .
        this->AppendBufferToBinlog(_buf_info, _offset_list);

        //Rotating binlog file...
        this->RotateFile();
    }

    this->m_last_logged.store(ConvertID(input_entities.back()->entity_id()), std::memory_order_release);

    {
        /* Caution: modifying the conditions must be under the protect of mutex,
           whether the conditions can be represented by an atomic object or not.
           Reference:http://en.cppreference.com/w/cpp/thread/condition_variable */
        std::unique_lock<std::mutex> _mutex_lock(m_cv_mutex);
        //Notifying doesn't need to hold the mutex.
        m_cv.notify_all();
    }

    this->m_log_num.fetch_add((uint32_t)input_entities.size());

    return true;
}

LogIdentifier BinLogOperator::GetLastReplicated() noexcept{
    return this->m_last_logged.load(std::memory_order_consume);
}

BinLogOperator::BinlogErrorCode BinLogOperator::RevertLog(TypeMemlogFollowerList &log_list, const LogIdentifier &boundary) noexcept {

    const auto & pre_entity_id = log_list.front()->GetEntity()->pre_log_id();

    //Find the last entry that being consistent with the first element of log_list.
    std::list<std::shared_ptr<FileMetaData::IdxPair>> _ordered_meta;
    m_p_meta.load()->GetOrderedMeta(_ordered_meta);

    auto _criter_meta = _ordered_meta.crbegin();
    for (; _criter_meta != _ordered_meta.crend(); _criter_meta++) {
        if ((*_criter_meta)->operator!=(pre_entity_id)) {
            continue;
        }
        break;
    }

    CHECK(_criter_meta != _ordered_meta.crend()) << "Reverting log : cannot find pre_entity_id,something must be wrong";

    auto _citer_meta = _criter_meta.base();
    bool _found_consistent_log = false;

    for (auto _citer_log = log_list.cbegin(); _citer_log != log_list.cend();) {
        const std::string &_key   = (*_citer_log)->GetEntity()->write_op().key();
        const std::string &_value = (*_citer_log)->GetEntity()->write_op().value();

        uint32_t _key_crc32   = ::RaftCore::Tools::CalculateCRC32((void*)_key.data(), (unsigned int)_key.length());
        uint32_t _value_crc32 = ::RaftCore::Tools::CalculateCRC32((void*)_value.data(), (unsigned int)_value.length());

        bool _item_equal = (*_citer_meta)->key_crc32 == _key_crc32 && (*_citer_meta)->value_crc32 == _value_crc32;
        if ( !_item_equal )
            break;

        _found_consistent_log = true;

        //Advance input log iterator.
        _citer_log = log_list.erase(_citer_log);

        //Advance binlog meta iterator.
        _citer_meta++;
        if (_citer_meta == _ordered_meta.cend())
            break;
    }

    /* There are several scenarios in reverting:
        1> log_list & binlog can find a consistent log entry:
            assuming _log_listX is the sublist of log_list,with its first element is the first inconsistent
            entry with binlog :

            1) _log_listX is empty: entries in log_list are all the same as binlog, nothing to revert.
            2) _log_listX has no intersection with binlog: entries in log_list will be appended to binlog
                without any erasing operations.
            3) _log_listX has intersection with binlog : the conflict entries in the binlog will be replaced
                by _log_listX.

        2> log_list & binlog can't find a consistent consistent log entry:
            return a NO-CONSITENT error.
    */
    if (!_found_consistent_log)
        return BinlogErrorCode::NO_CONSISTENT_ERROR;

    //No need to revert anything.
    if (log_list.empty())
        return BinlogErrorCode::SUCCEED_MERGED;

    const auto &_first_inconsistent_log = log_list.front();
    if (!EntityIDLarger(_first_inconsistent_log->GetEntity()->entity_id(), boundary))
        return BinlogErrorCode::OVER_BOUNDARY;

    BinlogStatus _cur_status = BinlogStatus::NORMAL;
    if (!this->m_binlog_status.compare_exchange_strong(_cur_status,BinlogStatus::REVERTING)) {
        //Other threads may have already modified the status variable.
        return BinlogErrorCode::OTHER_ERROR;
    }

    bool binlog_ended = (_citer_meta == _ordered_meta.cend());
    std::string _revert_point = binlog_ended ? "end of binlog file" : (*_citer_meta)->ToString();
    LOG(INFO) << "log reverting start...,detail:" << _revert_point;

    //Only in the following case should we do reverting.
    if (!binlog_ended) {

        //Remove meta data.
        auto _remove_iter = _citer_meta;
        while (_remove_iter !=_ordered_meta.cend()) {
            m_p_meta.load()->Delete(**_remove_iter);
            _remove_iter++;
        }

        //Update ID-LRL and notify the other threads who are waiting on it.
        std::unique_lock<std::mutex> _mutex_lock(m_cv_mutex);

        this->Truncate((*_citer_meta)->m_offset);

        _citer_meta--;
        LogIdentifier _id_lrl;
        _id_lrl.Set((*_citer_meta)->m_term,(*_citer_meta)->m_index );
        this->m_last_logged.store(_id_lrl, std::memory_order_release);
        m_cv.notify_all();
    }

    //Appending new entries.
    std::list<std::shared_ptr<Entity>> _entities_list;
    for (const auto& _item : log_list)
        _entities_list.emplace_back(_item->GetEntity());

    CHECK(AppendEntry(_entities_list,true)) << "AppendEntry to binlog fail,never should this happen,something terribly wrong.";

    //No exceptions could happened here.
    _cur_status = BinlogStatus::REVERTING;
    CHECK(this->m_binlog_status.compare_exchange_strong(_cur_status, BinlogStatus::NORMAL)) << "Binlog reverting : status CAS failed,something terribly wrong";

    return BinlogErrorCode::SUCCEED_TRUNCATED;
}

BinLogOperator::BinlogErrorCode BinLogOperator::SetHead(std::shared_ptr<::raft::Entity> _shp_entity) noexcept {

    BinlogStatus _cur_status = BinlogStatus::NORMAL;
    if (!this->m_binlog_status.compare_exchange_strong(_cur_status,BinlogStatus::SETTING_HEAD)) {
        //Other threads may have already modified the status variable.
        return BinlogErrorCode::OTHER_ERROR;
    }

    CHECK(std::fclose(this->m_binlog_handler)==0) << "truncating file fclose failed";
    if (fs::exists(fs::path(this->m_file_name)))
        CHECK(std::remove(this->m_file_name.c_str())==0);
    this->m_binlog_handler = std::fopen(this->m_file_name.c_str(), _AURORA_BINLOG_OP_MODE_);
    CHECK(this->m_binlog_handler != nullptr) << "open binlog file " << this->m_file_name << "fail..,errno:" << errno;

    //Clear and set up new meta info.
    auto *_p_tmp = m_p_meta.load();
    delete _p_tmp;
    m_p_meta.store(new FileMetaData());

    m_zero_log_id.Set(0, 0);
    this->m_last_logged.store(m_zero_log_id, std::memory_order_release);

    /*Since the binlog file has already just been reset,pre_log_id should be set to the initial id,only after that
     the data could be written into the binlog file. */
    auto _p_pre_log_id = _shp_entity->mutable_pre_log_id();
    _p_pre_log_id->set_term(0);
    _p_pre_log_id->set_idx(0);

    TypeEntityList _input_list;
    _input_list.emplace_back(_shp_entity);
    CHECK(AppendEntry(_input_list,true)) << "AppendEntry to binlog fail,never should this happen,something terribly wrong.";

    //No exceptions could happened here.
    _cur_status = BinlogStatus::SETTING_HEAD;
    CHECK(this->m_binlog_status.compare_exchange_strong(_cur_status, BinlogStatus::NORMAL)) << "Binlog setting head : status CAS failed,something terribly wrong";

    return BinlogErrorCode::SUCCEED_TRUNCATED;
}

bool BinLogOperator::Clear() noexcept {
    if (this->m_binlog_status.load() != BinlogStatus::NORMAL)
        return false;

    this->m_last_logged.store(m_zero_log_id, std::memory_order_release);

    this->DeleteOpenBinlogFile();

    auto *_p_tmp = m_p_meta.load();
    delete _p_tmp;
    m_p_meta.store(new FileMetaData());

    return true;
}

void BinLogOperator::DeleteOpenBinlogFile() noexcept {

    CHECK(std::fclose(this->m_binlog_handler)==0);

    LOG(INFO) << "deleting current running binlog:" << this->m_file_name;

    int _ret = std::remove(this->m_file_name.c_str());
    CHECK(_ret == 0) << ",delete fail,errno:" << errno;

    //Open & create new binlog file.
    this->m_binlog_handler = std::fopen(this->m_file_name.c_str(),_AURORA_BINLOG_OP_MODE_);
    CHECK(this->m_binlog_handler != nullptr) << "rotating, fopen binlog file fail...,errno:" << errno;
}

void BinLogOperator::RenameOpenBinlogFile() noexcept{
    //Scan binlog files.
    fs::path _path(".");
    CHECK (fs::is_directory(_path))  << "scan current directory fail,cannot save current file";

    int max_suffix = 0;
    for (auto&& x : fs::directory_iterator(_path)) {
        std::string file_name = x.path().filename().string();
        std::string::size_type pos = file_name.find(this->m_file_name);
        if (pos == std::string::npos)
            continue ;

        int suffix = 0;
        if (file_name != this->m_file_name)
            suffix = std::atol(file_name.substr(pos + this->m_file_name.length() + 1).c_str());

        max_suffix = std::max<int>(suffix,max_suffix);
    }

    CHECK(std::fclose(this->m_binlog_handler)==0);

    //Rename current file.
    char sz_new_name[1024] = { 0 };
    std::snprintf(sz_new_name, sizeof(sz_new_name),"%s-%d",this->m_file_name.c_str() ,max_suffix + 1);
    CHECK (std::rename(this->m_file_name.c_str(), sz_new_name) == 0)  << "rename binlog file fail...,errno:" << errno;

    //Open & create new binlog file.
    this->m_binlog_handler = std::fopen(this->m_file_name.c_str(),_AURORA_BINLOG_OP_MODE_);
    CHECK (this->m_binlog_handler != nullptr)  << "rotating, fopen binlog file fail...,errno:" << errno;
}

std::string BinLogOperator::GetBinlogFileName() noexcept {
    return this->m_file_name;
}

void BinLogOperator::GetOrderedMeta(FileMetaData::TypeOffsetList &_output) noexcept {
    m_p_meta.load()->GetOrderedMeta(_output);
}

}