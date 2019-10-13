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

#include "config/config.h"
#include "tools/utilities.h"
#include "binlog/binlog_meta_data.h"

#define _TYPE_LOG_IDX_OFFSET_ (0x01)

namespace RaftCore::BinLog {

FileMetaData::IdxPair::IdxPair(uint32_t _a, uint64_t _b, uint32_t _c, uint32_t _d, uint32_t _e) {
    this->m_term        = _a;
    this->m_index       = _b;
    this->m_offset      = _c;
    this->key_crc32     = _d;
    this->value_crc32   = _e;
}

bool FileMetaData::IdxPair::operator<(const IdxPair& _other)const noexcept {
    return this->LogIdentifier::operator<(_other);
}

bool FileMetaData::IdxPair::operator>(const IdxPair& _other)const noexcept {
    return this->LogIdentifier::operator>(_other);
}

bool FileMetaData::IdxPair::operator>(const LogIdentifier& _other)const noexcept {
    return this->LogIdentifier::operator>(_other);
}

bool FileMetaData::IdxPair::operator<(const LogIdentifier& _other)const noexcept {
    return this->LogIdentifier::operator<(_other);
}

bool FileMetaData::IdxPair::operator<=(const LogIdentifier& _other)const noexcept {
    return this->LogIdentifier::operator<=(_other);
}

bool FileMetaData::IdxPair::operator>=(const LogIdentifier& _other)const noexcept {
    return this->LogIdentifier::operator>=(_other);
}

bool FileMetaData::IdxPair::operator<(const ::raft::EntityID &_other)const noexcept {
    if (this->m_term < _other.term())
        return true;

    if (this->m_term > _other.term())
        return false;

    return this->m_index < _other.idx();
}

bool FileMetaData::IdxPair::operator==(const ::raft::EntityID &_other)const noexcept {
    return (this->m_term == _other.term()) && (this->m_index==_other.idx());
}

bool FileMetaData::IdxPair::operator!=(const ::raft::EntityID &_other)const noexcept {
    return !this->operator==(_other);
}

bool FileMetaData::IdxPair::operator>(const ::raft::EntityID &_other)const noexcept {
    if (this->m_term > _other.term())
        return true;

    if (this->m_term < _other.term())
        return false;

    return this->m_index > _other.idx();
}

bool FileMetaData::IdxPair::operator==(const IdxPair &_other) const noexcept{
    return (this->m_term == _other.m_term && this->m_index == _other.m_index);
}

const FileMetaData::IdxPair& FileMetaData::IdxPair::operator=(const IdxPair &_other) noexcept {
    this->m_term      = _other.m_term;
    this->m_index     = _other.m_index;
    this->m_offset    = _other.m_offset;
    this->key_crc32   = _other.key_crc32;
    this->value_crc32 = _other.value_crc32;

    return *this;
}

std::size_t FileMetaData::IdxPair::Hash() const noexcept {
    auto h1 = std::hash<uint64_t>{}(this->m_index);
    auto h2 = std::hash<uint64_t>{}(this->m_offset);
    return h1 ^ (h2 << 8);
}

FileMetaData::FileMetaData() noexcept {}

FileMetaData::~FileMetaData() noexcept {}

void FileMetaData::AddLogOffset(uint32_t term,uint64_t log_index, uint32_t file_offset, uint32_t key_crc32,uint32_t value_crc32) noexcept {
    //The allocated buf will be released in the destructor.
    std::shared_ptr<IdxPair> _shp_new_record(new IdxPair(term,log_index,file_offset,key_crc32,value_crc32));
    this->m_meta_hash.Insert(_shp_new_record);
}

void FileMetaData::AddLogOffset(const TypeOffsetList &_list) noexcept {
    for (const auto &_item : _list)
        this->m_meta_hash.Insert(_item);
}

void FileMetaData::Delete(const IdxPair &_item) noexcept {
    this->m_meta_hash.Delete(_item);
}

TypeBufferInfo FileMetaData::GenerateBuffer() const noexcept {

    std::list<std::shared_ptr<FileMetaData::IdxPair>> _hash_entry_list;
    this->m_meta_hash.GetOrderedByKey(_hash_entry_list);

    /* Meta data on-disk format,from low offset to high offset:
        1> meta data(tlv list)
           1) type :1 bytes
           2) length : 3 bytes
           3) value : vary
        2> CRC32 checksum. (4 bytes)
        3> length of whole meta data area. (4 bytes)
        4> a magic string to identify the end of meta-data area. (variadic bytes) */

    ::raft::LogOffset _log_offset;
    for (const auto &_ptr_entry : _hash_entry_list) {
        if (_ptr_entry == nullptr)
            continue;

        ::raft::LogOffsetItem* _p_item = _log_offset.add_mappings();
        _p_item->set_log_term(_ptr_entry->m_term);
        _p_item->set_log_idx(_ptr_entry->m_index);
        _p_item->set_offset(_ptr_entry->m_offset);
        _p_item->set_key_crc32(_ptr_entry->key_crc32);
        _p_item->set_value_crc32(_ptr_entry->value_crc32);
    }

    std::string _log_offset_buf = "";
    _log_offset.SerializeToString(&_log_offset_buf);

    uint32_t entry_field_length = (uint32_t)_log_offset_buf.size();

    //Value length cannot exceeds the maximum a three byte long integer can represent.
    assert(entry_field_length <= 0xFFFFFF);

    static uint32_t _footer_len = (uint32_t)std::strlen(_FILE_META_DATA_MAGIC_STR_);
    uint32_t _tail_length = 4 * 2 + _footer_len;
    uint32_t _buf_size = 1 + 3 + entry_field_length + _tail_length;
    auto _ptr = (unsigned char*)malloc(_buf_size);
    assert(_ptr!=nullptr);

    //Set meta data 1> 1).
    unsigned char* _p_cur = (unsigned char*)_ptr;
    _p_cur[0] = _TYPE_LOG_IDX_OFFSET_;
    _p_cur++;

    //Set meta data 1> 2).
    uint32_t _tmp = 0;
    ::RaftCore::Tools::ConvertToBigEndian<uint32_t>(entry_field_length, &_tmp);
    unsigned char* px = (unsigned char*)&_tmp;
    CHECK(*px == 0x0); //for it is big endian , the fist byte must be 0x0.
    px++;
    std::memcpy(_p_cur,px,_FOUR_BYTES_ - 1);
    _p_cur += 3;

    //Set meta data 1> 3).
    std::memcpy(_p_cur,_log_offset_buf.data(),entry_field_length);
    _p_cur += entry_field_length;

    //Calculate crc32 checksum.
    uint32_t _crc32_value = ::RaftCore::Tools::CalculateCRC32(_ptr, _buf_size - _tail_length);

    //Set crc32 checksum.
    ::RaftCore::Tools::ConvertToBigEndian<uint32_t>(_crc32_value, &_tmp);
    std::memcpy(_p_cur,&_tmp,_FOUR_BYTES_);
    _p_cur += _FOUR_BYTES_;

    //Set meta data area length.
    ::RaftCore::Tools::ConvertToBigEndian<uint32_t>(_buf_size, &_tmp);
    std::memcpy(_p_cur,&_tmp,_FOUR_BYTES_);
    _p_cur += _FOUR_BYTES_;

    //Set meta data area magic string.
    std::memcpy(_p_cur,_FILE_META_DATA_MAGIC_STR_,_footer_len);
    _p_cur += _footer_len;

    return std::make_tuple(_ptr,_buf_size);
}

void FileMetaData::ConstructMeta(const unsigned char* _buf, std::size_t _size) noexcept {
    auto _cur_ptr = _buf;
    //Examine header.
    CHECK(*_cur_ptr == _TYPE_LOG_IDX_OFFSET_) << "meta header check fail";
    _cur_ptr++;

    //Examine the checksum of metadata buf.
    uint32_t _length = 0x0;
    std::memcpy((unsigned char*)&_length+1,_cur_ptr,_FOUR_BYTES_ - 1);
    ::RaftCore::Tools::ConvertBigEndianToLocal<uint32_t>(_length, &_length);
    _cur_ptr += 3;

    //Examine the checksum of metadata buf.
    ::raft::LogOffset _log_offset;
    CHECK(_log_offset.ParseFromArray(_cur_ptr, _length)) << "parse meta data buf fail..";

    this->m_meta_hash.Clear();
    for (const auto &_item : _log_offset.mappings()) {
        std::shared_ptr<IdxPair> _shp_pair(new IdxPair(_item.log_term(),_item.log_idx(),_item.offset(),_item.key_crc32(),_item.value_crc32()));
        this->m_meta_hash.Insert(_shp_pair);
    }
}

int FileMetaData::ConstructMeta(std::FILE* _handler) noexcept {

    //Offset of _handler should be set to 0 before calling this method.

    assert(_handler != nullptr);

    uint32_t _buf_size = ::RaftCore::Config::FLAGS_binlog_parse_buf_size * 1024 * 1024;
    unsigned char *sz_buf = (unsigned char *)malloc(_buf_size);

    uint32_t _this_read  = 0,_total_read = 0;
    long _total_offset = 0;

    this->m_meta_hash.Clear();
    do{
        _this_read = (uint32_t)std::fread(sz_buf,1,_buf_size,_handler);
        CHECK(_this_read > _FOUR_BYTES_) << "parsing:read raw file fail,file corruption found,actual read:" << _this_read;
        _total_read += _this_read;

        auto _p_cur = sz_buf;

        //Parsing this buffer.
        uint32_t _this_offset = 0;
        do{
            if (_this_read - _this_offset < _FOUR_BYTES_) {
                /*Each of the <length,content> pairs must be read integrally,if current parsing buffer is smaller than 4 bytes,
                    means a partially read happened,need to adjust the reading position .  */
                CHECK(std::fseek(_handler,_total_offset,SEEK_SET)==0) << "seeking binlog fail";
                break;
            }

            uint32_t _cur_offset = _total_offset;

            uint32_t _item_buf_len  = *(uint32_t*)_p_cur;
            ::RaftCore::Tools::ConvertBigEndianToLocal<uint32_t>(_item_buf_len, &_item_buf_len);
            if ( _this_read - (_this_offset+_FOUR_BYTES_) < _item_buf_len ) {
                /*Like the previous if branch which contains the break statement:need position adjusting*/
                CHECK(std::fseek(_handler,_total_offset,SEEK_SET)==0) << "seeking binlog fail";
                break;
            }

            _p_cur        += _FOUR_BYTES_;
            _total_offset += _FOUR_BYTES_;
            _this_offset  += _FOUR_BYTES_;

            ::raft::BinlogItem _binlog_item;

            //Parse binglog_item buffer.
            CHECK(_binlog_item.ParseFromArray(_p_cur, _item_buf_len)) << "parse file content fail..";
            _p_cur        += _item_buf_len;
            _total_offset += _item_buf_len;
            _this_offset  += _item_buf_len;

            const auto & _wop = _binlog_item.entity().write_op();
            uint32_t _key_crc32   = ::RaftCore::Tools::CalculateCRC32((void*)_wop.key().data(),(unsigned int)_wop.key().length());
            uint32_t _value_crc32 = ::RaftCore::Tools::CalculateCRC32((void*)_wop.value().data(),(unsigned int)_wop.value().length());

            std::shared_ptr<IdxPair>  _shp_pair(new IdxPair(_binlog_item.entity().entity_id().term(),
                                                _binlog_item.entity().entity_id().idx(), _cur_offset,_key_crc32,_value_crc32));
            this->m_meta_hash.Insert(_shp_pair);

        //This will always be true, otherwise it would break at previous break statement.
        } while (_this_offset < _this_read);

    //If _this_read < _buf_size , means we've read to the end of file.
    } while (_this_read >= _buf_size);

    free(sz_buf);

    return _total_offset == _total_read ? 0 : _total_offset;
}

void FileMetaData::GetOrderedMeta(TypeOffsetList &_output) const noexcept {
    this->m_meta_hash.GetOrderedByKey(_output);
}

void FileMetaData::BackOffset(int offset) noexcept{
    this->m_meta_hash.Map([&offset](std::shared_ptr<IdxPair> &_one)->void{ _one->m_offset -= offset;});
}

std::string FileMetaData::IdxPair::ToString() const noexcept {
    const static int _buf_size = 512;
    char _sz_buf[_buf_size] = { 0 };
    std::snprintf(_sz_buf,_buf_size,"IdxPair term:%u,idx:%llu,offset:%u",this->m_term,this->m_index,this->m_offset);

    return std::string(_sz_buf);
}

std::ostream& operator<<(std::ostream& os, const FileMetaData::IdxPair& obj) {
    os << obj.ToString();
    return os;
}

}