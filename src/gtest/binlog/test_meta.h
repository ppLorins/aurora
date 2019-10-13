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

#ifndef __GTEST_META_H__
#define __GTEST_META_H__

#include <list>
#include <memory>
#include <cstdio>
#include <chrono>

#include "gtest/test_base.h"
#include "binlog/binlog_meta_data.h"
#include "binlog/binlog_singleton.h"

/*
#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <crtdbg.h>*/

using ::RaftCore::BinLog::FileMetaData;
using ::RaftCore::BinLog::BinLogGlobal;
using ::RaftCore::Common::LogIdentifier;
using ::RaftCore::DataStructure::HashNode;

class TestMeta : public TestBase {

    public:

        TestMeta() {}

        virtual void SetUp() override {
        }

        virtual void TearDown() override {
        }

    protected:

        void GenerateTestMeta(int _sum,FileMetaData &_meta){
            int _counter = 0;
            bool _add_batch = true;
            std::list<std::shared_ptr<FileMetaData::IdxPair>> _list;
            for (int i = 0; i < _sum; ++i) {

                _counter++;
                if (_counter <= 5) {
                    std::shared_ptr<FileMetaData::IdxPair> _shp_pair(new FileMetaData::IdxPair(10, i, i, 79, 79));
                    _list.emplace_back(_shp_pair);
                    if (i == _sum - 1)
                        _meta.AddLogOffset(_list);
                    continue;
                }

                if (_add_batch) {
                    std::shared_ptr<FileMetaData::IdxPair> _shp_pair(new FileMetaData::IdxPair(10, i, i, 79, 79));
                    _list.emplace_back(_shp_pair);
                    _meta.AddLogOffset(_list);
                    _add_batch = false;
                    _list.clear();
                    continue;
                }

                //add single
                _meta.AddLogOffset(10,i,i,79,79);
                _add_batch = true;
                _counter = 0;
            }
        }

        void CheckMetaListEqual(const std::list<std::shared_ptr<FileMetaData::IdxPair>> &_output1,
                                    const std::list<std::shared_ptr<FileMetaData::IdxPair>> &_output2) {
            ASSERT_EQ(_output1.size(),_output2.size());

            auto _iter2 = _output2.cbegin();
            for (auto _iter = _output1.cbegin(); _iter != _output1.cend();++_iter,++_iter2) {
                ASSERT_TRUE(**_iter == **_iter2);
            }
        }

        void CheckMetaEqual(const FileMetaData &meta1,const FileMetaData &meta2) {

            std::list<std::shared_ptr<FileMetaData::IdxPair>> _output1;
            meta1.GetOrderedMeta(_output1);

            std::list<std::shared_ptr<FileMetaData::IdxPair>> _output2;
            meta2.GetOrderedMeta(_output2);

            this->CheckMetaListEqual(_output1, _output2);
        }

};

TEST_F(TestMeta, GeneralOperation) {

    //testing IdxPair.
    FileMetaData::IdxPair _pair_1(10,20,30,40,50);

    FileMetaData::IdxPair _pair_2(10,21,30,40,50);
    ASSERT_TRUE(_pair_1 < _pair_2);

    FileMetaData::IdxPair _pair_3(10,20,30,40,50);
    ASSERT_TRUE(_pair_1 == _pair_3);

    std::cout << _pair_3.Hash() << std::endl;

    FileMetaData::IdxPair _pair_4(10,19,30,40,50);
    ASSERT_TRUE(_pair_1 > _pair_4);

    LogIdentifier _log_id1;
    _log_id1.Set(10,19);
    ASSERT_TRUE(_pair_1 > _log_id1);
    ASSERT_TRUE(_pair_1 >= _log_id1);

    LogIdentifier _log_id2;
    _log_id2.Set(10,21);
    ASSERT_TRUE(_pair_1<_log_id2);
    ASSERT_TRUE(_pair_1<=_log_id2);

    ::raft::EntityID _entity_id;
    _entity_id.set_term(10);
    _entity_id.set_idx(21);
    ASSERT_TRUE(_pair_1<_entity_id);

    _entity_id.set_idx(19);
    ASSERT_TRUE(_pair_1>_entity_id);
    ASSERT_TRUE(_pair_1!=_entity_id);

    _entity_id.set_idx(20);
    ASSERT_TRUE(_pair_1==_entity_id);

    //testing FileMetaData.
    int _sum = 10000;
    FileMetaData _meta;
    this->GenerateTestMeta(_sum,_meta);

    std::list<std::shared_ptr<FileMetaData::IdxPair>> _output;
    _meta.GetOrderedMeta(_output);
    ASSERT_EQ(_sum, _output.size());

    int _delete_point1 = _sum / 3;
    FileMetaData::IdxPair _pair_d1(10,_delete_point1 , _delete_point1, 79, 79);
    _meta.Delete(_pair_d1);

    int _delete_point2 = _sum / 3*2;
    FileMetaData::IdxPair _pair_d2(10, _delete_point2, _delete_point2, 79, 79);
    _meta.Delete(_pair_d2);

    _meta.GetOrderedMeta(_output);

    ASSERT_EQ(_sum - 2, _output.size());

    int _cur_val = 0;
    auto _iter = _output.cbegin();
    for (int i = 0; i < _sum; ++i) {

        if (_cur_val == _delete_point1 || _cur_val == _delete_point2)
            continue;

        ASSERT_TRUE(*(*_iter) == FileMetaData::IdxPair(10, i, 0, 0, 0));

        _iter++;
        _cur_val++;
    }

    //testing buf.
    uint32_t _buf_size = 0;;
    unsigned char* _pbuf = nullptr;
    std::tie(_pbuf,_buf_size) = _meta.GenerateBuffer();

    std::cout << "generated buf size:" << _buf_size << std::endl;

    FileMetaData _meta_2;
    _meta_2.ConstructMeta(_pbuf,_buf_size);

    this->CheckMetaEqual(_meta,_meta_2);

    //Remove existing binlog file first.
    const char *_role = "test";
    ASSERT_EQ(std::remove(std::string(_AURORA_BINLOG_NAME_ + std::string(".") + _role).c_str()),0);

    //testing file
    BinLogGlobal::m_instance.Initialize(_role);

    //Construct binlog file.
    std::list<std::shared_ptr<::raft::Entity> > _input;
    for (int i = 0; i < 10;++i) {
        std::shared_ptr<::raft::Entity> _shp_entity(new ::raft::Entity());

        auto _p_entity_id = _shp_entity->mutable_entity_id();
        _p_entity_id->set_term(0);
        _p_entity_id->set_idx(i);

        auto _p_pre_entity_id = _shp_entity->mutable_pre_log_id();
        _p_pre_entity_id->set_term(0);
        _p_pre_entity_id->set_idx(i==0?0:i-1);

        auto _p_wop = _shp_entity->mutable_write_op();
        _p_wop->set_key("key_" + std::to_string(i));
        _p_wop->set_value("val_" + std::to_string(i));

        _input.emplace_back(_shp_entity);
    }
    ASSERT_TRUE(BinLogGlobal::m_instance.AppendEntry(_input));

    BinLogGlobal::m_instance.GetOrderedMeta(_output);

    BinLogGlobal::m_instance.UnInitialize();

    //starting real test.
    std::FILE* _hfile = std::fopen(BinLogGlobal::m_instance.GetBinlogFileName().c_str(),_AURORA_BINLOG_OP_MODE_);

    ASSERT_EQ(std::fseek(_hfile, 0, SEEK_SET), 0);
    FileMetaData _meta_3;
    _meta_3.ConstructMeta(_hfile);

    std::list<std::shared_ptr<FileMetaData::IdxPair>> _output2;
    _meta_3.GetOrderedMeta(_output2);

    this->CheckMetaListEqual(_output,_output2);

}

TEST_F(TestMeta, MetaAllocate) {

    auto _start = this->StartTimeing();
    LogIdentifier _log_id;
    this->EndTiming(_start, "new id");

    _start = this->StartTimeing();
    ::RaftCore::DataStructure::LockFreeHash<FileMetaData::IdxPair>    m_meta_hash;
    this->EndTiming(_start, "new LockFreeHash");

    _start = this->StartTimeing();
    FileMetaData _meta;
    this->EndTiming(_start, "new meta");

    std::cout << "sizeof meta:" << sizeof(_meta) << std::endl;
}

template <typename T,typename R=void>
class HashNodeTest final {

public:

    HashNodeTest(const std::shared_ptr<T> &key, const std::shared_ptr<R> &val) noexcept {
        this->m_shp_key = key;
        this->m_shp_val = val;
        this->m_next = nullptr;
    }

private:

    std::shared_ptr<T>    m_shp_key;

    std::shared_ptr<R>    m_shp_val;

    HashNodeTest<T,R>*    m_next = nullptr;

    uint32_t  m_iterating_tag = 0;

private:

    HashNodeTest(const HashNodeTest&) = delete;

    HashNodeTest& operator=(const HashNodeTest&) = delete;

};

TEST_F(TestMeta, MetaLeak) {

    std::cout << "sizeof(FileMetaData) :" << sizeof(FileMetaData) << std::endl;;

    std::cout << "sizeof(::RaftCore::DataStructure::LockFreeHash<IdxPair>) :"
        << sizeof(::RaftCore::DataStructure::LockFreeHash<FileMetaData::IdxPair>) << std::endl;

    std::cout << "sizeof(HashNodeTest<IdxPair>) :" << sizeof(HashNodeTest<FileMetaData::IdxPair>) << std::endl;

    std::cout << "sizeof(std::atomic<HashNodeTest<IdxPair>*>) :"
        << sizeof(std::atomic<HashNodeTest<FileMetaData::IdxPair>*>) << std::endl;

    std::cout << "sizeof(FileMetaData::IdxPair) :" << sizeof(FileMetaData::IdxPair) << std::endl;

    {
        uint32_t _count = ::RaftCore::Config::FLAGS_meta_count;

        //FileMetaData is not the cause.
        /* FileMetaData _meta;
        for (std::size_t i = 0; i < _count; ++i)
            _meta.AddLogOffset(0, i, 7, 1, 1); */

        ::RaftCore::DataStructure::LockFreeHash<FileMetaData::IdxPair>    m_meta_hash;
        for (std::size_t i = 0; i < _count; ++i) {
            //std::shared_ptr<FileMetaData::IdxPair> _shp_new_record(new FileMetaData::IdxPair(0, i, 7, 1, 1));
            //m_meta_hash.Insert(_shp_new_record);
            //HashNodeTest<FileMetaData::IdxPair, void>* p_new_node = new HashNodeTest<FileMetaData::IdxPair, void>(_shp_new_record, nullptr);

            //std::shared_ptr<uint64_t> _shp_new_record(new uint64_t(i), [](auto *P) {});

            std::shared_ptr<uint64_t> _shp_new_record(new uint64_t(i));
            HashNodeTest<uint64_t>* p_new_node = new HashNodeTest<uint64_t>(_shp_new_record, nullptr);
        }

        std::cout << "check buf size now." << sizeof(HashNodeTest<uint64_t>) << std::endl;
    }

    std::cout << "clear now." << std::endl;;

    //_CrtSetReportMode(_CRT_ERROR, _CRTDBG_MODE_DEBUG);
    //_CrtDumpMemoryLeaks();
}

#endif
