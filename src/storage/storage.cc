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


#include <regex>

#include "boost/filesystem.hpp"

#include "binlog/binlog_meta_data.h"
#include "binlog/binlog_singleton.h"
#include "storage/storage.h"
#include "state/state_mgr.h"
#include "common/comm_view.h"

namespace RaftCore::Storage {

namespace  fs = ::boost::filesystem;

using ::RaftCore::Common::ReadLock;
using ::RaftCore::Common::WriteLock;
using ::RaftCore::Common::CommonView;
using ::RaftCore::BinLog::FileMetaData;
using ::RaftCore::BinLog::BinLogOperator;
using ::RaftCore::Storage::HashableString;
using ::RaftCore::State::StateMgr;

StorageMgr::StorageMgr() noexcept : m_path(_AURORA_DATA_DIR_) {}

StorageMgr::~StorageMgr() noexcept {
    if (this->m_initialized)
        this->UnInitialize();
}

bool StorageMgr::Initialize(const char* role, bool reset) noexcept {

    CHECK(role != nullptr);

    this->m_role_str = role;

    this->m_garbage_sstable.store(nullptr);
    this->m_garbage_memory_table.store(nullptr);

    this->m_last_committed.store(CommonView::m_zero_log_id);
    this->m_last_persist.store(CommonView::m_zero_log_id);

    if (!fs::exists(this->m_path))
        fs::create_directory(this->m_path);

    CHECK(fs::is_directory(this->m_path)) << "scan data directory fail,cannot save current file";

    this->m_memory_table_head.store(new UnorderedSingleListNode<MemoryTable>());

    std::list<std::string>  _all_sstable_files;

    for (auto&& x : fs::directory_iterator(this->m_path)) {
        std::string _file_name = x.path().filename().string();
        std::string::size_type pos = _file_name.find(_AURORA_SSTABLE_PREFIX_);
        if (pos == std::string::npos)
            continue;
        _all_sstable_files.emplace_back(_file_name);
    }

    /*Sort by descending order.File number won't be large,it's acceptable to sorting a std::list
      compared to using a 'std::vector' and 'std::sort' .*/
    _all_sstable_files.sort([](const std::string &left, const std::string &right)->bool {return right < left; });

    this->m_sstable_table_head.store(nullptr);
    UnorderedSingleListNode<SSTAble>* _p_cur_node = this->m_sstable_table_head.load();

    bool _merged_flag = false;
    for (const auto &_item : _all_sstable_files) {
        const std::string &_file_name = _item;
        auto _cur_path = this->m_path / _file_name;

        if (_merged_flag) {
            LOG(INFO) << "Deleting merged files during initializing: " << _file_name;
            fs::remove(fs::path(_cur_path));
            continue;
        }

        LOG(INFO) << "Parsing and loading sstable:" << _cur_path.string();
        auto* _p_new_node = new UnorderedSingleListNode<SSTAble>(_cur_path.string().c_str());

        if (_p_cur_node)
            _p_cur_node->m_next.store(_p_new_node);
        else
            this->m_sstable_table_head.store(_p_new_node);
        _p_cur_node = _p_new_node;

        //File with a merged suffix must be the last one need to be loaded.
        if (_file_name.find(_AURORA_SSTABLE_MERGE_SUFFIX_) != std::string::npos)
            _merged_flag = true; //Delete all following sstable files, since they are already merged.
    }

    //Find latest entry ID that has been stored in SSTAbles.
    LogIdentifier _max_log_id;
    _max_log_id.Set(0, 0);

    _p_cur_node = this->m_sstable_table_head.load();
    if (_p_cur_node != nullptr)
        _max_log_id = _p_cur_node->m_data->GetMaxLogID();
    else
        LOG(WARNING) << "no sstable found,data will remain empty after initialization.";

    this->m_last_committed.store(_max_log_id);
    this->m_last_persist.store(_max_log_id);

    //Construct memory table from binlog by _max_log_id.
    if (!reset)
        this->ConstructMemoryTable(_max_log_id);

    this->m_initialized = true;

    LOG(INFO) << "[Storage] m_last_committed initialized as:" << this->m_last_committed.load();

    return true;
}

bool StorageMgr::ConstructFromBinlog(const LogIdentifier &from, const std::string &binlog_file_name) noexcept {

    LOG(INFO) << "[Storage] parsing binlog file:" << binlog_file_name;

    BinLogOperator _cur_binlog;
    _cur_binlog.Initialize(binlog_file_name.c_str(), true);

    std::list<std::shared_ptr<FileMetaData::IdxPair>> _file_meta;
    _cur_binlog.GetOrderedMeta(_file_meta);

    if (_file_meta.empty())
        return true;

    std::FILE* _f_handler = std::fopen(binlog_file_name.c_str(),_AURORA_BINLOG_READ_MODE_);

    auto _riter = _file_meta.crbegin();
    for (; _riter != _file_meta.crend(); ++_riter) {
        if ((*_riter)->operator<=(from))
            break;
    }

    bool _finished = (_riter != _file_meta.crend());

    unsigned char* _p_buf = nullptr;
    for (auto _iter = _riter.base(); _iter != _file_meta.cend(); _iter++) {

        //Seek to position
        CHECK(std::fseek(_f_handler, (*_iter)->m_offset, SEEK_SET) == 0) << "ConstructMemoryTable seek binlog file "
            << binlog_file_name << "fail..,errno:" << errno;

        //Read protobuf buf length
        uint32_t _buf_len = 0;
        CHECK(std::fread(&_buf_len, 1, _FOUR_BYTES_, _f_handler) == _FOUR_BYTES_) << "ConstructMemoryTable read binlog file "
                        << binlog_file_name << "fail..,errno:" << errno;
        ::RaftCore::Tools::ConvertBigEndianToLocal<uint32_t>(_buf_len, &_buf_len);

        //Read protobuf buf
        _p_buf = (_p_buf) ? (unsigned char*)std::realloc(_p_buf,_buf_len): (unsigned char*)malloc(_buf_len);
        CHECK(std::fread(_p_buf, 1, _buf_len, _f_handler) == _buf_len) << "ConstructMemoryTable read binlog file "
            << binlog_file_name << " fail..,errno:" << errno;

        ::raft::BinlogItem _binlog_item;
        CHECK(_binlog_item.ParseFromArray(_p_buf, _buf_len)) << "ConstructMemoryTable parse protobuf buffer fail " << binlog_file_name;

        //If the first log entry's pre_id matches 'from', also means the parsing process is finished.
        if (_iter == _riter.base()) {
            auto _pre_of_first = ::RaftCore::Common::ConvertID(_binlog_item.entity().pre_log_id());
            _finished |= (_pre_of_first == from);
        }

        auto *_p_wop = _binlog_item.mutable_entity()->mutable_write_op();
        const auto &_entity_id = _binlog_item.entity().entity_id();
        this->m_memory_table_head.load()->m_data->Insert(*_p_wop->mutable_key(), *_p_wop->mutable_value(), _entity_id.term(), _entity_id.idx());

        LogIdentifier _cur_id;
        _cur_id.Set(_entity_id.term(), _entity_id.idx());

        /*Note: m_last_committed may greater than the real LCL of the current server, it's okay  b/c:
          1> if current server is the leader, any log entries in the binlog should have been committed, as the way aurora works.
          2> if current server is a follower, and the 'm_last_committed' > the last consistent
            log entry, a SYNC_DATA would eventually triggered.
          3> if current server is a candidate, no influence on that.
        */
        if (_cur_id > this->m_last_committed.load())
            this->m_last_committed.store(_cur_id);
    }

    CHECK(fclose(_f_handler) == 0) << "ConstructMemoryTable: close binlog file fail.";

    if (_finished)
        LOG(INFO) << "binlog:" << binlog_file_name << " reach end,from:" << from << ", _riter idx:" << (*_riter.base())->m_index;

    return _finished;
}

void StorageMgr::ConstructMemoryTable(const LogIdentifier &from) noexcept {

    FindRoleBinlogFiles(this->m_role_str, this->m_loaded_binlog_files);

    if (this->m_loaded_binlog_files.empty()) {
        LOG(INFO) << "found no binlog available.";
        return;
    }

    /*Sort by descending order.File number won't be large,it's acceptable to sorting a std::list
      compared to using a 'std::vector' and 'std::sort' .*/
    this->m_loaded_binlog_files.sort([](const std::string &left, const std::string &right)->bool {

        auto _get_suffix = [](const std::string &file_name) {
            int _suffix = 0;
            std::string::size_type pos = file_name.find("-");
            if (pos != std::string::npos)
                _suffix = std::atoi(file_name.substr(pos + 1).c_str());
            return _suffix;
        };

        return _get_suffix(left) > _get_suffix(right);
    });

    //list: 5/4/3/2/1/0, but 0 is the latest one, move it to the first place of the list.
    std::string _lastest_file = this->m_loaded_binlog_files.back();
    this->m_loaded_binlog_files.pop_back();
    this->m_loaded_binlog_files.push_front(_lastest_file);

    //VLOG(89) << "debug size:" << this->m_loaded_binlog_files.size() << ",last:" << _lastest_file;

    bool _find_latest = false;
    for (const auto& _file_name : this->m_loaded_binlog_files) {
        if (!this->ConstructFromBinlog(from, _file_name))
            continue;

        LOG(INFO) << "[Storage] binlog file before(not include): " << _file_name << " can be manually deleted";

        _find_latest = true;
        break;
    }

    CHECK(_find_latest) << "binlog content incomplete, last persistent id:" << this->m_last_persist.load();
}

void StorageMgr::UnInitialize() noexcept {
    this->ClearInMemoryData();
    this->m_initialized = false;
}

bool StorageMgr::Get(const std::string &key,std::string &val) const noexcept{

    //Find in memory tables.
    auto _cur_mem_node = this->m_memory_table_head.load();
    while (_cur_mem_node != nullptr) {
        if (_cur_mem_node->m_data->GetData(key, val))
            return true;

        _cur_mem_node = _cur_mem_node->m_next.load();
    }

    //Find in SSTables.
    auto _p_cur_sstable_node = this->m_sstable_table_head.load();
    while (_p_cur_sstable_node != nullptr) {
        if (_p_cur_sstable_node->m_data->Read(key, val))
            return true;

        _p_cur_sstable_node = _p_cur_sstable_node->m_next.load();
    }

    return false;
}

void StorageMgr::DumpMemoryTable(const MemoryTable *src)  noexcept {

    auto *_cur_head = this->m_sstable_table_head.load();

    auto *_new_sstable_head = new UnorderedSingleListNode<SSTAble>(*src);
    _new_sstable_head->m_next.store(_cur_head);

    while (!this->m_sstable_table_head.compare_exchange_strong(_cur_head, _new_sstable_head)) {
        _new_sstable_head->m_next.store(_cur_head);
        LOG(WARNING) << "concurrently dumping memory table CAS conflict ,continue...";
    }

    VLOG(89) << "storage successfully dumped a memory table to sstable:"
        << _new_sstable_head->m_data->GetFilename();
}

void StorageMgr::PurgeGarbage() noexcept {
    //Purging process should be mutual exclusion from releasing process.
    WriteLock _w_lock(this->m_mutex);

    VLOG(89) << "storage purging started.";

    this->PurgeMemoryTable();

#ifdef _STORAGE_TEST_
    while (this->PurgeSSTable());
#else
    this->PurgeSSTable();
#endif
}

void StorageMgr::PurgeMemoryTable() noexcept {

    auto _p_cur  = this->m_garbage_memory_table.load();
    if (_p_cur == nullptr)
        return ;

    while (!this->m_garbage_memory_table.compare_exchange_weak(_p_cur, nullptr))
        continue;

    //Here '_p_cur' is the cut off list and all elements in it should be reclaimed.
    while (_p_cur != nullptr) {
        auto *_p_pre = _p_cur;
        _p_cur = _p_cur->m_next.load();
        delete _p_pre;

        LOG(INFO) << "purged one memory table.";
    }
}

void StorageMgr::RecycleLast2SStables() noexcept {

    auto *_p_cur_garbage = this->m_garbage_sstable.load();
    if (_p_cur_garbage == nullptr)
        return;

    auto *_p_next_garbage = _p_cur_garbage->m_next.load();
    if (_p_next_garbage == nullptr)
        return;

    int _garbage_size = 2;

    auto *_p_pre_garbage = _p_cur_garbage;
    while (_p_next_garbage->m_next.load() != nullptr) {
        if (_p_pre_garbage != _p_cur_garbage)
            _p_cur_garbage = _p_cur_garbage;
        _p_cur_garbage  = _p_next_garbage;
        _p_next_garbage = _p_next_garbage->m_next.load();
        _garbage_size++;
    }

    //Detach the last two garbage nodes.
    if (_garbage_size == 2) {
        if (!this->m_garbage_sstable.compare_exchange_strong(_p_cur_garbage, nullptr)) {
            LOG(INFO) << "recursive RecycleLast2SStables occurred.";
            return this->RecycleLast2SStables();
        }
    } else {
        CHECK(_p_pre_garbage->m_next.compare_exchange_strong(_p_cur_garbage, nullptr))
            << "recycle sstable garbage last2 CAS fail.";
    }

    auto* _release_cur = _p_cur_garbage;
    while (_release_cur != nullptr) {
        std::string  _filename  = _release_cur->m_data->GetFilename();

        auto *_p_tmp = _release_cur->m_next.load();
        //Clear the in-memory data.
        delete _release_cur;

        //Remove the sstable file.
        LOG(INFO) << "Deleting garbage sstable files : " << _filename;
        fs::remove(fs::path(_filename));

        _release_cur = _p_tmp;
    }
}

bool StorageMgr::PurgeSSTable() noexcept {

    this->RecycleLast2SStables();

    auto _p_cur  = this->m_sstable_table_head.load();
    if (_p_cur == nullptr)
        return false;

    auto _p_next = _p_cur->m_next.load();
    if (_p_next == nullptr)
        return false;

    int _garbage_part_size = 2;

    auto _p_pre = _p_cur;
    while (_p_next->m_next.load() != nullptr) {
        if (_p_pre != _p_cur)
            _p_pre = _p_cur;
        _p_cur  = _p_next;
        _p_next = _p_next->m_next.load();
        ++_garbage_part_size;
    }

    UnorderedSingleListNode<SSTAble> *_new_sstable_node = new UnorderedSingleListNode<SSTAble>(*_p_next->m_data,*_p_cur->m_data);

    if (_garbage_part_size == 2)
        this->m_sstable_table_head.store(_new_sstable_node);
    else {
        /*_p_pre always pointing to the element immediately preceding the one to be merged into, and its
         origin value is related to the #sstables.And the following CAS operation should always
         succeed since there is no multiple thread updating scenarios for the 'next' pointer of
         _p_pre.*/
        CHECK(_p_pre->m_next.compare_exchange_strong(_p_cur, _new_sstable_node))
                << "update merged sstable previous next pointer fail.";
    }

    /*Note :We cannot release the purged sstable objects immediately after the moment we've finished purging
      since there may have other threads accessing them. Here we push them into a 'garbage' list and
      releasing them later,aka next round of purging.  */

    LOG(INFO) << "merged sstable of " << _p_cur->m_data->GetFilename() << " and "
        << _p_next->m_data->GetFilename() << " into " << _new_sstable_node->m_data->GetFilename();

    //Insert the new nodes at garbage list's head.
    auto *_p_cur_head = m_garbage_sstable.load();
    _p_next->m_next.store(_p_cur_head);

    while (!m_garbage_sstable.compare_exchange_weak(_p_cur_head, _p_cur))
        _p_next->m_next.store(_p_cur_head);

    return true;
}

bool StorageMgr::Set(const LogIdentifier &log_id ,const std::string &key, const std::string &value)  noexcept{

    auto *_cur_head = this->m_memory_table_head.load();
    _cur_head->m_data->Insert(key, value, log_id.m_term, log_id.m_index);

    LogIdentifier _cur_id;
    _cur_id.Set(this->m_last_committed.load());

    while (log_id > _cur_id) {
        if (this->m_last_committed.compare_exchange_strong(_cur_id, log_id))
            break;
    }

    if (_cur_head->m_data->Size() <= ::RaftCore::Config::FLAGS_memory_table_max_item)
        return true;

    UnorderedSingleListNode<MemoryTable>* _new_head = new UnorderedSingleListNode<MemoryTable>();

    bool _insert_succeed = false;
    while (_cur_head->m_data->Size() > ::RaftCore::Config::FLAGS_memory_table_max_item) {

        _new_head->m_next.store(_cur_head);

        _insert_succeed = this->m_memory_table_head.compare_exchange_strong(_cur_head, _new_head);
        if (!_insert_succeed)
            continue; //Here '_cur_head' already been updated to the latest head of 'm_memory_table_head'.

        this->DumpMemoryTable(_cur_head->m_data);

        /*Note: 1.Since DumpMemoryTable() can be executed simultaneously, we need to ensure that all subsequent
         nodes after '_cur_head' in the single linked list are dumped(aka,their data can be found in the sstable
         link list now) before cutting off _cur_head's ancestor's link to it. Otherwise client cannot query
         data in the node which are cut off too early.
         2. Records among sstables can be not in order because of the reasons:
            1> records can invode 'StorageMgr::Set' simultanously.
            2> memory tables are dummped into stables simultanously.
            3> sstable file names are not in lexicographical order if more one sstables falls into the
               same microsecond time windows.

            But the change of get unordered records among sstables is slim:
             1> & 2> depends on a concurrent dumping which itself is also rare.
             3> can be basically ignored.

             It's still acceptable even if that happened:
             1) No impacts on reading, you can't decide a strict order for records that very close
                to each other, in the first place.
             2) get unordered result in 'GetSlice', but it's okay for the scenario where it applies
                to : sync data to the lag behind followers.
         */
        while (_cur_head->m_next.load() != nullptr);

        _new_head->m_next.store(nullptr);

        auto *_p_garbage_head = this->m_garbage_memory_table.load();
        _cur_head->m_next = _p_garbage_head;
        while (!this->m_garbage_memory_table.compare_exchange_weak(_p_garbage_head, _cur_head))
            _cur_head->m_next = _p_garbage_head;

        return true;
    }

    if (!_insert_succeed)
        delete _new_head;

    return true;
}

const LogIdentifier StorageMgr::GetLastCommitted() const noexcept {
    return this->m_last_committed.load();
}

void StorageMgr::ClearInMemoryData() noexcept {

    //Clear in-memory data themselves.
    this->ReleaseData<MemoryTable>(this->m_garbage_memory_table);
    this->ReleaseData<MemoryTable>(this->m_memory_table_head);
    this->ReleaseData<SSTAble>(this->m_sstable_table_head);
    this->ReleaseData<SSTAble>(this->m_garbage_sstable);
}

void StorageMgr::Reset() noexcept {

    this->ClearInMemoryData();

    //Clear on-disk data.
    CHECK(!this->m_loaded_binlog_files.empty());

    if(this->m_loaded_binlog_files.size() > 1) {
        auto _iter = this->m_loaded_binlog_files.cbegin();

        //Skip the first one since it's the default binlog and shall be delete in the BinlogMgr.
        _iter++;

        for (; _iter != this->m_loaded_binlog_files.cend(); ++_iter) {
            const std::string &_delete_binlog_file = *_iter;
            LOG(INFO) << "Deleting the loaded binlog files for storage Reset:" << _delete_binlog_file;
            CHECK(std::remove(_delete_binlog_file.c_str()) == 0);
        }
    }

    LOG(INFO) << "Deleting the whole data directory for storage Reset.";
    fs::remove(this->m_path);

    //TODO: figure why sometimes this failed under win10.
    CHECK(!fs::exists(this->m_path));
    fs::create_directory(this->m_path);

    //Reinitialization.
    this->Initialize(this->m_role_str.c_str(), true);
}

void StorageMgr::GetSliceInSSTable(const LogIdentifier& start_at, int step_len, std::list<StorageItem> &output_list) const noexcept {

    //Once goes here, the binlog must have been iterated, so we only need to
    auto *_p_cur_sstable = this->m_sstable_table_head.load();
    if (_p_cur_sstable == nullptr)
        return;

    std::vector<decltype(_p_cur_sstable)>   _access_list;

    do {
        auto _cur_max_id = _p_cur_sstable->m_data->GetMaxLogID();
        if (start_at >= _cur_max_id)
            break;

        _access_list.emplace_back(_p_cur_sstable);
        _p_cur_sstable = _p_cur_sstable->m_next.load();
    } while (_p_cur_sstable != nullptr);

    if (_access_list.empty())
        return;

    //Records' log ID in the newer sstable are all larger than those in the older sstable,so just need to start at _p_pre.
    int _counter = 0;

    auto _reading = [&](const SSTAble::Meta &meta,const HashableString &key) ->bool{

        if (_counter >= step_len)
            return false;

        LogIdentifier _cur_id;
        _cur_id.Set(meta.m_term,meta.m_index);
        if (_cur_id <= start_at)
            return true;

        std::string _val = "";
        _p_cur_sstable->m_data->Read(*key.GetStrPtr(), _val);

        output_list.emplace_back(_cur_id, key.GetStrPtr(), std::make_shared<std::string>(std::move(_val)));
        _counter++;

        return true;
    };

    for (auto _iter = _access_list.crbegin(); _iter != _access_list.crend(); ++_iter) {
        _p_cur_sstable = *_iter;
        if (!(*_iter)->m_data->IterateByVal(_reading))
            break;
    }

    return;
}

void StorageMgr::GetSliceInMemory(const LogIdentifier& start_at, int step_len,
    std::list<StorageItem> &output_list) const noexcept {

    //Once goes here, the binlog must have been iterated, so we only need to
    auto *_p_cur_memory_table = this->m_memory_table_head.load();
    if (_p_cur_memory_table == nullptr)
        return;

    /*Caveat : There is an implicit constrain that the dumping memory tables won't be reclaimed
      during the following iterations. Since there are several seconds before next GC, we can just
      rely on it.*/

    std::vector<decltype(_p_cur_memory_table)>   _access_list;

    /*TODO: Prevent from a rare case of losing dumping tables while switch from iterating sstable
        to iterating memory table.*/
    while (_p_cur_memory_table != nullptr) {
        _access_list.push_back(_p_cur_memory_table);
        _p_cur_memory_table = _p_cur_memory_table->m_next.load();
    }

    CHECK(!_access_list.empty());

    //std::function<bool(const TypePtrHashableString&,const TypePtrHashValue&)> op

    int _counter = 0;
    auto _reading = [&](const HashValue &hash_val,const HashableString &key) ->bool{

        if (_counter >= step_len)
            return false;

        LogIdentifier _cur_id;
        _cur_id.Set(hash_val.m_term, hash_val.m_index);
        if (_cur_id <= start_at)
            return true;

        output_list.emplace_back(_cur_id, key.GetStrPtr(), hash_val.m_val);
        _counter++;

        return true;
    };

    for (auto _iter = _access_list.crbegin(); _iter != _access_list.crend(); ++_iter) {
        _p_cur_memory_table = *_iter;
        (*_iter)->m_data->IterateByVal(_reading);
    }

    return;
}

void StorageMgr::GetSlice(const LogIdentifier& start_at,uint32_t step_len,std::list<StorageItem> &output_list) const noexcept {
    output_list.clear();

    this->GetSliceInSSTable(start_at, step_len, output_list);
    std::size_t _got_size = output_list.size();
    if (_got_size >= step_len)
        return;

    uint32_t _remain = (uint32_t)(step_len - _got_size);

    this->GetSliceInMemory(start_at, _remain, output_list);
}

void StorageMgr::FindRoleBinlogFiles(const std::string &role, std::list<std::string> &output) {

    output.clear();

    std::string _filename_reg_pattern = _AURORA_BINLOG_NAME_REG_ + std::string("\\.") + role
        + std::string("(-[0-9]*){0,1}");

    LOG(INFO) << "searching binlog file with pattern:" << _filename_reg_pattern;

    std::regex _pattern(_filename_reg_pattern);
    std::smatch _sm;

    for (auto&& x : fs::directory_iterator(fs::path("."))) {
        std::string _file_name = x.path().filename().string();

        if (!std::regex_match(_file_name, _sm, _pattern))
            continue;

        output.emplace_back(_file_name);
    }
}


}