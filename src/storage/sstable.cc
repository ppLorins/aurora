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

#include <unordered_set>
#include <chrono>

#include "boost/filesystem.hpp"

#include "common/comm_view.h"
#include "config/config.h"
#include "tools/utilities.h"
#include "storage/sstable.h"

#define _AURORA_SSTABLE_OP_MODE_ "ab+"
#define _AURORA_SSTABLE_READ_MODE_ "rb"
#define _AURORA_SSTABLE_FOOTER_ "!@#$sstable$#@!"

namespace RaftCore::Storage {

namespace  fs = ::boost::filesystem;
using ::RaftCore::Common::CommonView;
using ::RaftCore::Tools::ConvertToBigEndian;
using ::RaftCore::Tools::ConvertBigEndianToLocal;
using ::RaftCore::Storage::TypePtrHashableString;
using ::RaftCore::Storage::PtrHSHasher;
using ::RaftCore::Storage::PtrHSEqualer;

SSTAble::Meta::Meta(uint32_t a, uint16_t b, uint32_t c, uint16_t d,uint32_t e,uint64_t f) {
    this->m_key_offset  = a;
    this->m_key_len     = b;
    this->m_val_offset  = c;
    this->m_val_len     = d;
    this->m_term  = e;
    this->m_index = f;
}

bool SSTAble::Meta::operator<(const Meta &other) {

    if (this->m_term < other.m_term)
        return true;

    if (this->m_term > other.m_term)
        return false;

    return this->m_index < other.m_index;
}

SSTAble::SSTAble(const char* file) noexcept {
    this->m_shp_meta = std::make_shared<TypeOffset>(::RaftCore::Config::FLAGS_sstable_table_hash_slot_num);

    this->m_min_log_id.Set(CommonView::m_max_log_id);
    this->m_max_log_id.Set(CommonView::m_zero_log_id);

    this->m_associated_file = file;
    this->ParseFile();
}

SSTAble::SSTAble(const MemoryTable &src) noexcept {
    this->m_shp_meta = std::make_shared<TypeOffset>(::RaftCore::Config::FLAGS_sstable_table_hash_slot_num);

    this->CreateFile();
    this->DumpFrom(src);
}

SSTAble::SSTAble(const SSTAble &from, const SSTAble &to) noexcept {

    this->m_shp_meta = std::make_shared<TypeOffset>(::RaftCore::Config::FLAGS_sstable_table_hash_slot_num);

    std::list<TypePtrHashableString>   _from_keys;
    from.m_shp_meta->GetOrderedByKey(_from_keys);
    CHECK(_from_keys.size() > 0) << "sstable meta size invalid ,file:" << from.GetFilename();

    std::list<TypePtrHashableString>   _to_keys;
    to.m_shp_meta->GetOrderedByKey(_to_keys);
    CHECK(_to_keys.size() > 0) << "sstable meta size invalid ,file:" << to.GetFilename();

    //Merge sort.
    MemoryTable _mem_table;

    std::unordered_set<TypePtrHashableString,PtrHSHasher,PtrHSEqualer> _intersection;
    auto _cmp = [](const TypePtrHashableString &left, const TypePtrHashableString &right) ->bool {
        return left->operator<(*right);
    };

    std::set_intersection(_from_keys.cbegin(), _from_keys.cend(), _to_keys.cbegin(), _to_keys.cend(), std::inserter(_intersection,_intersection.end()),_cmp);

    auto _update_mem_table = [&](const std::list<TypePtrHashableString> &_key_list,const SSTAble &sstable,bool filter=false) {

        for (const auto &_key : _key_list) {

            if (filter && _intersection.find(_key) != _intersection.cend())
                continue;

            std::string _val = "";
            const std::string &_key_str = _key->GetStr();

            CHECK(sstable.Read(_key_str, _val)) << "key:" << _key_str << " doesn't exist in file:" << sstable.GetFilename();

            TypePtrMeta _shp_meta;
            CHECK(sstable.m_shp_meta->Read(*_key, _shp_meta)) << "key:" << _key_str << " doesn't exist in meta:" << sstable.GetFilename();

            _mem_table.Insert(_key_str,_val,_shp_meta->m_term,_shp_meta->m_index);
        }
    };

    _update_mem_table(_to_keys, to);
    _update_mem_table(_from_keys, from, true);

    std::string _new_file_name = to.m_associated_file + _AURORA_SSTABLE_MERGE_SUFFIX_;
    this->CreateFile(_new_file_name.c_str());

    this->DumpFrom(_mem_table);
}

SSTAble::~SSTAble() noexcept {
    if (this->m_file_handler != nullptr)
        CHECK(fclose(this->m_file_handler) == 0) << "close sstable file fail.";
}

const std::string& SSTAble::GetFilename() const noexcept {
    return this->m_associated_file;
}

void SSTAble::CreateFile(const char* file_name) noexcept {

    char sz_file[128] = { 0 };

    if (file_name) {
        std::snprintf(sz_file,sizeof(sz_file),"%s",file_name);
        this->m_associated_file = sz_file;
    } else {
        auto _rand = ::RaftCore::Tools::GenerateRandom(0, 1000);
        auto _now = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch());
        std::snprintf(sz_file,sizeof(sz_file),_AURORA_SSTABLE_PREFIX_".%llu.%03u",_now.count(),_rand);
        fs::path _path(_AURORA_DATA_DIR_);
        if (!fs::exists(_path))
            fs::create_directory(_path);
        _path /= sz_file;
        this->m_associated_file = _path.string();
    }

    this->m_file_handler = std::fopen(this->m_associated_file.c_str(), _AURORA_SSTABLE_OP_MODE_);
    CHECK(this->m_file_handler != nullptr) << "create sstable file fail.";
}

void SSTAble::ParseFile() noexcept {

    this->m_file_handler = std::fopen(this->m_associated_file.c_str(),_AURORA_SSTABLE_READ_MODE_);
    CHECK(this->m_file_handler != nullptr) << "open sstable file fail.";

    CHECK(std::fseek(this->m_file_handler, 0, SEEK_END) == 0) << "seek sstable file fail" << this->m_associated_file;

    std::size_t _footer_len = std::strlen(_AURORA_SSTABLE_FOOTER_);
    std::size_t _mininal_file_size = _FOUR_BYTES_ * 3 + _footer_len;

    uint32_t _file_size = std::ftell(this->m_file_handler);
    CHECK(_file_size >= _mininal_file_size) << "ftell sstable file " << this->m_associated_file << " fail..,errno:" << errno;

    std::size_t _tail_len    = _footer_len + _FOUR_BYTES_ * 2;
    long _tail_offset = (long)(_file_size - _tail_len);
    CHECK(std::fseek(this->m_file_handler, _tail_offset, SEEK_SET) == 0);

    unsigned int _init_size = 1024;
    unsigned char *_p_buf = (unsigned char *)malloc(_init_size);

    std::size_t _read = std::fread(_p_buf, 1, _tail_len, this->m_file_handler);
    CHECK(_read==_tail_len) << "fread fail,need:" << _tail_len << ",actual:" << _read;

    //Check footer.
    CHECK(std::strncmp((char*)&_p_buf[8], _AURORA_SSTABLE_FOOTER_, _footer_len) == 0) << "sstable footer wrong,check it:" << this->m_associated_file;

    //Read meta offset.
    uint32_t _meta_offset = 0;
    std::memcpy(&_meta_offset, &_p_buf[4], _FOUR_BYTES_);
    ConvertBigEndianToLocal<uint32_t>(_meta_offset,&_meta_offset);

    //Read meta checksum.
    uint32_t _meta_crc = 0;
    std::memcpy(&_meta_crc, &_p_buf, _FOUR_BYTES_);
    ConvertBigEndianToLocal<uint32_t>(_meta_crc,&_meta_crc);

    CHECK(std::fseek(this->m_file_handler, _meta_offset, SEEK_SET) == 0);

    std::size_t _meta_area_len = _tail_offset - _meta_offset;
    if (_meta_area_len > _init_size)
        _p_buf = (unsigned char*)std::realloc(_p_buf, _meta_area_len);

    this->ParseMeta(_p_buf,_meta_area_len);

    free(_p_buf);
}

void SSTAble::ParseMeta(unsigned char* &allocated_buf,std::size_t meta_len) noexcept {

    std::size_t _read = std::fread(allocated_buf, 1, meta_len, this->m_file_handler);
    CHECK(_read==meta_len) << "fread fail,need:" << meta_len << ",actual:" << _read;

    auto *_p_cur = allocated_buf;

    while (_p_cur < allocated_buf + meta_len) {
        //Parse key offset.
        uint32_t _key_offset = 0;
        std::memcpy(&_key_offset, _p_cur, _FOUR_BYTES_);
        ConvertBigEndianToLocal<uint32_t>(_key_offset,&_key_offset);
        _p_cur += _FOUR_BYTES_;

        //Parse key len.
        uint16_t _key_len = 0;
        std::memcpy(&_key_len, _p_cur, _TWO_BYTES_);
        ConvertBigEndianToLocal<uint16_t>(_key_len,&_key_len);
        _p_cur += _TWO_BYTES_;

        //Parse val offset.
        uint32_t _val_offset = 0;
        std::memcpy(&_val_offset, _p_cur, _FOUR_BYTES_);
        ConvertBigEndianToLocal<uint32_t>(_val_offset,&_val_offset);
        _p_cur += _FOUR_BYTES_;

        //Parse val len.
        uint16_t _val_len = 0;
        std::memcpy(&_val_len, _p_cur, _TWO_BYTES_);
        ConvertBigEndianToLocal<uint16_t>(_val_len,&_val_len);
        _p_cur += _TWO_BYTES_;

        //Parse term.
        uint32_t _term = 0;
        std::memcpy(&_term, _p_cur, _FOUR_BYTES_);
        ConvertBigEndianToLocal<uint32_t>(_term,&_term);
        _p_cur += _FOUR_BYTES_;

        //Parse index.
        uint64_t _index = 0;
        std::memcpy(&_index, _p_cur, _EIGHT_BYTES_);
        ConvertBigEndianToLocal<uint64_t>(_index,&_index);
        _p_cur += _EIGHT_BYTES_;

        //Read key.
        std::string _key(_key_len,0);
        CHECK(std::fseek(this->m_file_handler, _key_offset, SEEK_SET) == 0);
        std::fread((char*)_key.data(), 1, _key_len, this->m_file_handler);

        TypePtrHashableString _shp_key(new HashableString(_key));
        TypePtrMeta _shp_meta(new Meta(_key_offset,_key_len,_val_offset,_val_len,_term,_index));
        this->m_shp_meta->Insert(_shp_key,_shp_meta);

        //Update max log ID.
        LogIdentifier _cur_id;
        _cur_id.Set(_term, _index);
        if (_cur_id > this->m_max_log_id)
            this->m_max_log_id.Set(_cur_id);
        if (_cur_id < this->m_min_log_id)
            this->m_min_log_id.Set(_cur_id);
    }
}

bool SSTAble::Read(const std::string &key, std::string &val) const noexcept {

    std::shared_ptr<Meta> _shp_meta;

    if (!this->m_shp_meta->Read(HashableString(key,true), _shp_meta))
        return false;

    //Need to open a new fd to support concurrently reading.
    std::FILE* _handler = std::fopen(this->m_associated_file.c_str(),_AURORA_SSTABLE_READ_MODE_);
    CHECK(_handler != nullptr) << "fopen sstable file fail.";

    CHECK(std::fseek(_handler, _shp_meta->m_val_offset, SEEK_SET) == 0);

    val.resize(_shp_meta->m_val_len);
    std::size_t _read = std::fread((char*)val.data(), 1, _shp_meta->m_val_len, _handler);
    CHECK(_read==_shp_meta->m_val_len) << "fread fail,need:" << _shp_meta->m_val_len << ",actual:" << _read;

    CHECK(std::fclose(_handler)==0) << "Read sstable file fclose failed";

    return true;
}

void SSTAble::AppendKvPair(const TypePtrHashableString &key, const TypePtrHashValue &val, void* buf,
    uint32_t buff_len, uint32_t &buf_offset, uint32_t &file_offset) noexcept {

    uint16_t _key_len = (uint16_t)key->GetStr().length();
    uint16_t _val_len = (uint16_t)val->m_val->length();

    int _cur_len = _key_len + _val_len;

    if (buf_offset + _cur_len > buff_len) {
        CHECK(std::fwrite(buf, 1, buf_offset, this->m_file_handler) == buf_offset) << "fwrite KV records fail,error no:" << errno;
        CHECK (std::fflush(this->m_file_handler) == 0 )  << "fflush KV data to end of binlog file fail...";

        //Reset the offset after a successful flush.
        buf_offset = 0;
    }

    unsigned char* _p_start_point = (unsigned char*)buf + buf_offset;
    unsigned char* _p_cur = _p_start_point;

    //Advance the global position identifiers.
    buf_offset  += _cur_len;

    //Field-1 : key content.
    std::memcpy(_p_cur, key->GetStr().data(), _key_len);
    _p_cur += _key_len;

    //Field-2 : val content.
    std::memcpy(_p_cur, val->m_val->data(), _val_len);
    _p_cur += _val_len;

    uint32_t _record_crc = ::RaftCore::Tools::CalculateCRC32(_p_start_point, _cur_len);
    this->m_record_crc += _record_crc;

    uint32_t _val_offset = file_offset + _key_len;
    this->m_shp_meta->Insert(key, std::make_shared<Meta>(file_offset, _key_len, _val_offset,
                                                         _val_len, val->m_term, val->m_index));

    file_offset += _cur_len;

    //Update max log ID.
    LogIdentifier _cur_id;
    _cur_id.Set(val->m_term, val->m_index);
    if (_cur_id > this->m_max_log_id)
        this->m_max_log_id.Set(_cur_id);
    if (_cur_id < this->m_min_log_id)
        this->m_min_log_id.Set(_cur_id);
}

void SSTAble::AppendChecksum(uint32_t checksum) noexcept {
    uint32_t _copy = checksum;
    ConvertToBigEndian<uint32_t>(_copy, &_copy);
    CHECK(std::fwrite(&_copy, 1, _FOUR_BYTES_, this->m_file_handler) == _FOUR_BYTES_) << "fwrite CRC fail,error no:" << errno;
    CHECK (std::fflush(this->m_file_handler) == 0 )  << "fflush checksum to end of binlog file fail...";
}

void SSTAble::CalculateMetaOffset() noexcept {
    this->m_meta_offset = std::ftell(this->m_file_handler);
    CHECK(this->m_meta_offset >= 0) << "ftell sstable file " << this->m_associated_file << "fail..,errno:" << errno;
}

void SSTAble::AppendMetaOffset() noexcept {
    uint32_t _copy = this->m_meta_offset;
    ConvertToBigEndian<uint32_t>(_copy, &_copy);
    CHECK(std::fwrite(&_copy, 1, _FOUR_BYTES_, this->m_file_handler) == _FOUR_BYTES_) << "fwrite CRC fail,error no:" << errno;
    CHECK(std::fflush(this->m_file_handler) == 0) << "fflush meta offset to end of binlog file fail...";
}

void SSTAble::AppendMeta(const TypePtrHashableString &key, const TypePtrMeta &shp_meta, void* buf,
                uint32_t buff_len, uint32_t &buf_offset, uint32_t &file_offset) noexcept {

    if (buf_offset + this->m_single_meta_len > buff_len) {
        CHECK(std::fwrite(buf, 1, buf_offset, this->m_file_handler) == buf_offset) << "fwrite meta fail,error no:" << errno;
        CHECK (std::fflush(this->m_file_handler) == 0 )  << "fflush meta to end of binlog file fail...";

        buf_offset = 0;
    }

    uint32_t _key_offset = shp_meta->m_key_offset;
    uint16_t _key_len    = (uint16_t)key->GetStr().length();
    uint32_t _val_offset = shp_meta->m_val_offset;
    uint16_t _val_len    = shp_meta->m_val_len;
    uint32_t _term       = shp_meta->m_term;
    uint64_t _index      = shp_meta->m_index;

    int _cur_buf_len = this->m_single_meta_len;

    unsigned char* _p_start_point = (unsigned char*)buf + buf_offset;
    auto *_p_cur = _p_start_point;

    buf_offset += this->m_single_meta_len;

    //Field-1 : key offset.
    ConvertToBigEndian<uint32_t>(_key_offset, &_key_offset);
    std::memcpy(_p_cur, (unsigned char*)&_key_offset, _FOUR_BYTES_);
    _p_cur += _FOUR_BYTES_;

    //Field-2 : key len.
    ConvertToBigEndian<uint16_t>(_key_len, &_key_len);
    std::memcpy(_p_cur, (unsigned char*)&_key_len, _TWO_BYTES_);
    _p_cur += _TWO_BYTES_;

    //Field-3 : val offset.
    ConvertToBigEndian<uint32_t>(_val_offset, &_val_offset);
    std::memcpy(_p_cur, (unsigned char*)&_val_offset, _FOUR_BYTES_);
    _p_cur += _FOUR_BYTES_;

    //Field-4 : val len.
    ConvertToBigEndian<uint16_t>(_val_len, &_val_len);
    std::memcpy(_p_cur, (unsigned char*)&_val_len, _TWO_BYTES_);
    _p_cur += _TWO_BYTES_;

    //Field-5 : term.
    ConvertToBigEndian<uint32_t>(_term, &_term);
    std::memcpy(_p_cur, (unsigned char*)&_term, _FOUR_BYTES_);
    _p_cur += _FOUR_BYTES_;

    //Field-6 : index.
    ConvertToBigEndian<uint64_t>(_index, &_index);
    std::memcpy(_p_cur, (unsigned char*)&_index, _EIGHT_BYTES_);
    _p_cur += _EIGHT_BYTES_;

    uint32_t _meta_crc = ::RaftCore::Tools::CalculateCRC32(_p_start_point, _cur_buf_len);
    this->m_meta_crc += _meta_crc;
}

void SSTAble::AppendFooter() noexcept {
    static std::size_t _len = std::strlen(_AURORA_SSTABLE_FOOTER_);
    CHECK(std::fwrite(_AURORA_SSTABLE_FOOTER_, 1, _len, this->m_file_handler) == _len) << "fwrite footer fail,error no:" << errno;
    CHECK (std::fflush(this->m_file_handler) == 0 )  << "fflush footer to end of binlog file fail...";
}

void SSTAble::DumpFrom(const MemoryTable &src) noexcept {

    const uint32_t _estimated_avg_record_bytes = 20;
    uint32_t _buf_size = _estimated_avg_record_bytes * ::RaftCore::Config::FLAGS_memory_table_max_item;
    uint32_t _buf_offset = 0, _file_offset = 0;

    void* _p_buf = malloc(_buf_size);

    //Append KV records.
    auto _append_kv = [&](const TypePtrHashableString &shp_key, const TypePtrHashValue &shp_val)->bool {
        this->AppendKvPair(shp_key, shp_val, _p_buf, _buf_size, _buf_offset, _file_offset);
        return true;
    };
    src.IterateByKey(_append_kv);

    //Check if there are remaining bytes
    if (_buf_offset > 0) {
        CHECK(std::fwrite(_p_buf, 1, _buf_offset, this->m_file_handler) == _buf_offset) << "fwrite KV records fail,error no:" << errno;
        CHECK (std::fflush(this->m_file_handler) == 0 )  << "fflush KV data to end of binlog file fail...";
    }

    //Append checksum of KV records.
    this->AppendChecksum(this->m_record_crc);

    //Get the offset of meta. Must be called immediately  after appending KV checksum.
    this->CalculateMetaOffset();

    //To reuse the buff.
    _buf_offset = 0;

    //Append Meta data.
    auto _append_meta = [&](const TypePtrHashableString &shp_key, const TypePtrMeta &shp_meta)->bool {
        this->AppendMeta(shp_key, shp_meta, _p_buf, _buf_size, _buf_offset, _file_offset);
        return true;
    };
    this->m_shp_meta->Iterate(_append_meta);

    free(_p_buf);
    _p_buf = nullptr;

    //Append checksum of meta.
    this->AppendChecksum(this->m_meta_crc);

    this->AppendMetaOffset();

    //Append footprint.
    this->AppendFooter();
}

LogIdentifier SSTAble::GetMaxLogID() const noexcept {
    return this->m_max_log_id;
}

LogIdentifier SSTAble::GetMinLogID() const noexcept {
    return this->m_min_log_id;
}

bool SSTAble::IterateByVal(std::function<bool(const Meta &meta, const HashableString &key)> op) const noexcept {

    LockFreeHash<HashableString, Meta>::ValueComparator _cmp = [](const TypePtrMeta &left, const TypePtrMeta &right)->bool { return *left < *right; };

    std::map<std::shared_ptr<Meta>, std::shared_ptr<HashableString>,decltype(_cmp)> _ordered_by_value_map(_cmp);

    this->m_shp_meta->GetOrderedByValue(_ordered_by_value_map);

    for (const auto &_item : _ordered_by_value_map)
        if (!op(*_item.first, *_item.second))
            return false;

    return true;
}

}

