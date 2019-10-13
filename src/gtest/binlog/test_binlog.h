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

#ifndef __GTEST_BINLOG_H__
#define __GTEST_BINLOG_H__

#include <list>
#include <memory>
#include <chrono>

#include "boost/filesystem.hpp"

#include "gtest/test_base.h"
#include "common/comm_defs.h"
#include "binlog/binlog_singleton.h"
#include "storage/storage_singleton.h"
#include "leader/leader_view.h"

using ::RaftCore::BinLog::BinLogGlobal;
using ::RaftCore::BinLog::BinLogOperator;
using ::RaftCore::Storage::StorageGlobal;
using ::RaftCore::Leader::LeaderView;
using ::RaftCore::DataStructure::HashNode;

namespace  fs = ::boost::filesystem;

class TestBinlog : public TestBase {

    public:

        TestBinlog() {}

    protected:

        virtual void SetUp() override {
            m_zero.Set(0, 0);
        }

        virtual void TearDown() override {
        }

        auto RevertPreviousLogs(int cur_idx,int thread_idx) {

            //test reverting.
            std::list<std::shared_ptr<MemoryLogItemFollower>>  _log_list;

            int _pre_count = 3;

            for (int i = cur_idx - _pre_count; i <= cur_idx; ++i) {
                ::raft::Entity _tmp;
                auto _p_id = _tmp.mutable_entity_id();
                _p_id->set_term(0);
                _p_id->set_idx(i);

                auto _p_pre_id = _tmp.mutable_pre_log_id();
                _p_pre_id->set_term(0);
                _p_pre_id->set_idx(i-1);

                auto _p_wop = _tmp.mutable_write_op();

                char sz_val[1024] = { 0 };
                //std::snprintf(sz_val,sizeof(sz_val),"val_%d tid:%d",i,thread_idx);
                std::snprintf(sz_val,sizeof(sz_val),"val_%d",i);
                _p_wop->set_key("key_" + std::to_string(i));
                _p_wop->set_value(sz_val);

                _log_list.emplace_back(new MemoryLogItemFollower(_tmp));
            }

            return BinLogGlobal::m_instance.RevertLog(_log_list, this->m_zero);
        }

        LogIdentifier   m_zero;
};

TEST_F(TestBinlog, GeneralOperation) {

    //Remove existing binlog file first.
    const char *_role = "test";
    std::string _binlog_file = _AURORA_BINLOG_NAME_ + std::string(".") + _role;
    if (fs::exists(fs::path(_binlog_file)))
        ASSERT_TRUE(std::remove(_binlog_file.c_str())==0);

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

    std::list<std::shared_ptr<FileMetaData::IdxPair>> _output;
    BinLogGlobal::m_instance.GetOrderedMeta(_output);

    ASSERT_TRUE(BinLogGlobal::m_instance.GetLastReplicated() == LogIdentifier(*_output.back()));

    ASSERT_TRUE(BinLogGlobal::m_instance.GetBinlogFileName() == _binlog_file);

    //test reverting.
    std::list<std::shared_ptr<MemoryLogItemFollower>>  _log_list;

    /*This is test case is for 1> 3) of the scenarios mentioned in the implementation
        of BinLogGlobal::m_instance.RevertLog function.Others too less error prone to test.  */
    for (int i = 6; i <= 15; ++i) {
        ::raft::Entity _tmp;
        auto _p_id = _tmp.mutable_entity_id();
        _p_id->set_term(0);
        _p_id->set_idx(i);

        auto _p_pre_id = _tmp.mutable_pre_log_id();
        _p_pre_id->set_term(0);
        _p_pre_id->set_idx(i-1);

        auto _p_wop = _tmp.mutable_write_op();

        int idx = i <= 7 ? i : i+1;
        std::string _cur_idx = std::to_string(idx);
        _p_wop->set_key("key_"  + _cur_idx);
        _p_wop->set_value("val_"  + _cur_idx);

        _log_list.emplace_back(new MemoryLogItemFollower(_tmp));
    }

    BinLogGlobal::m_instance.RevertLog(_log_list, this->m_zero);

    //Setting Head.
    int _head_idx = 13317;
    std::shared_ptr<::raft::Entity> _shp_entity(new ::raft::Entity());
    auto _p_entity_id = _shp_entity->mutable_entity_id();
    _p_entity_id->set_term(0);
    _p_entity_id->set_idx(_head_idx);

    auto _p_wop = _shp_entity->mutable_write_op();

    std::string _cur_idx = std::to_string(_head_idx);
    _p_wop->set_key("key_head_"  + _cur_idx);
    _p_wop->set_value("val_head_"  + _cur_idx);

    BinLogGlobal::m_instance.SetHead(_shp_entity);

    BinLogGlobal::m_instance.UnInitialize();
}

//TODO: figure out why binlog consume so much memory: 8w ~25MB.
TEST_F(TestBinlog, ConcurrentOperation) {

    //Remove existing binlog file first.
    const char *_role = "test";
    std::string _binlog_file = _AURORA_BINLOG_NAME_ + std::string(".") + _role;

    if (fs::exists(fs::path(_binlog_file)))
        ASSERT_TRUE(std::remove(_binlog_file.c_str())==0);

    //testing file
    BinLogGlobal::m_instance.Initialize(_role);

    int _sum = 10000;
    auto _op = [&](int thread_idx) {

        int _revert_each    = 100;
        int _revert_counter = 0;

        for (int i=thread_idx; i < _sum*this->m_cpu_cores;i+=this->m_cpu_cores) {

            std::list<std::shared_ptr<::raft::Entity> > _input;
            std::shared_ptr<::raft::Entity> _shp_entity(new ::raft::Entity());

            auto _p_entity_id = _shp_entity->mutable_entity_id();
            _p_entity_id->set_term(0);
            _p_entity_id->set_idx(i);

            auto _p_pre_entity_id = _shp_entity->mutable_pre_log_id();
            _p_pre_entity_id->set_term(0);
            _p_pre_entity_id->set_idx(i==0?0:i-1);

            auto _p_wop = _shp_entity->mutable_write_op();
            std::string _cur_idx = std::to_string(i);

            char sz_val[1024] = { 0 };
            //std::snprintf(sz_val,sizeof(sz_val),"val_%d tid:%d",i,thread_idx);
            std::snprintf(sz_val,sizeof(sz_val),"val_%d",i);
            _p_wop->set_key("key_" + _cur_idx);
            _p_wop->set_value(sz_val);

            _input.emplace_back(_shp_entity);

            ASSERT_TRUE(BinLogGlobal::m_instance.AppendEntry(_input));

            if (_revert_counter >= _revert_each) {
                auto _revert_code = this->RevertPreviousLogs(i,thread_idx);
                //ASSERT_TRUE(_revert_code == BinLogOperator::BinlogErrorCode::SUCCEED_TRUNCATED || _revert_code == BinLogOperator::BinlogErrorCode::OTHER_ERROR) << "error code:" << _revert_code;
                if (_revert_code == BinLogOperator::BinlogErrorCode::SUCCEED_TRUNCATED) {
                    std::cout << "!!!!!!!! revert succeed,cur_idx:" << i << ",thread_idx:" << thread_idx << std::endl;
                }

                if(_revert_code != BinLogOperator::BinlogErrorCode::SUCCEED_TRUNCATED && _revert_code != BinLogOperator::BinlogErrorCode::OTHER_ERROR)
                    std::cout << "----------error code:" << int(_revert_code) << ",cur_idx:" << i
                            << ",thread_idx:" << thread_idx << std::endl;

                _revert_counter = 0;
            }

            std::cout << BinLogGlobal::m_instance.GetLastReplicated() << ",thread_idx:" << thread_idx << std::endl;

            _revert_counter++;
        }

        ASSERT_TRUE(BinLogGlobal::m_instance.GetBinlogFileName() == _binlog_file);
    };

    this->LaunchMultipleThread(_op);

    LogIdentifier _end_log_id;
    _end_log_id.Set(0,_sum * this->m_cpu_cores - 1);
    ASSERT_TRUE(BinLogGlobal::m_instance.GetLastReplicated()==_end_log_id);

    BinLogGlobal::m_instance.UnInitialize();
}

TEST_F(TestBinlog, SetHead) {

    //Remove existing binlog file first.
    const char *_role = "setHead";
    std::string _binlog_file = _AURORA_BINLOG_NAME_ + std::string(".") + _role;
    if (fs::exists(fs::path(_binlog_file)))
        ASSERT_TRUE(std::remove(_binlog_file.c_str())==0);

    //testing file
    BinLogGlobal::m_instance.Initialize(_role);

    std::shared_ptr<::raft::Entity> _shp_entity(new ::raft::Entity());

    int i = 17;
    auto _p_entity_id = _shp_entity->mutable_entity_id();
    _p_entity_id->set_term(0);
    _p_entity_id->set_idx(i);
    auto _p_wop = _shp_entity->mutable_write_op();
    _p_wop->set_key("key_" + std::to_string(i));
    _p_wop->set_value("val_" + std::to_string(i));
    BinLogGlobal::m_instance.SetHead(_shp_entity);

    i = 19;
    _p_entity_id->set_term(0);
    _p_entity_id->set_idx(i);
    _p_wop = _shp_entity->mutable_write_op();
    _p_wop->set_key("key_" + std::to_string(i));
    _p_wop->set_value("val_" + std::to_string(i));

    BinLogGlobal::m_instance.SetHead(_shp_entity);

    BinLogGlobal::m_instance.UnInitialize();
}

TEST_F(TestBinlog, RotateFile) {

    ::RaftCore::Config::FLAGS_binlog_max_size = 100;
    ::RaftCore::Config::FLAGS_binlog_reserve_log_num = 2;

    //Remove existing binlog file first.
    const char *_role = "test";
    std::string _binlog_file = _AURORA_BINLOG_NAME_ + std::string(".") + _role;
    if (fs::exists(fs::path(_binlog_file)))
        ASSERT_TRUE(std::remove(_binlog_file.c_str())==0);

    //testing file
    BinLogGlobal::m_instance.Initialize(_role);

    //Set storage first.
    ASSERT_TRUE(StorageGlobal::m_instance.Initialize(_ROLE_STR_TEST_));

    LogIdentifier _lcl_id;
    _lcl_id.Set(0, 5);
    ASSERT_TRUE(StorageGlobal::m_instance.Set(_lcl_id,"k","v"));

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

    BinLogGlobal::m_instance.UnInitialize();
}

TEST_F(TestBinlog, Perf) {

    //Remove existing binlog file first.
    const char *_role = "test";
    std::string _binlog_file = _AURORA_BINLOG_NAME_ + std::string(".") + _role;
    if (fs::exists(fs::path(_binlog_file)))
        ASSERT_TRUE(std::remove(_binlog_file.c_str())==0);

    //testing file
    BinLogGlobal::m_instance.Initialize(_role);

    auto _start = std::chrono::steady_clock::now();

    int _write_times = 10000;
    int _count_each_time = 20;

    uint32_t _start_idx = 0;

    for (int i = 0; i < _write_times; ++i) {
        std::list<std::shared_ptr<::raft::Entity> > _input;

        _start_idx = i * _count_each_time;

        for (int j = _start_idx; j < _count_each_time;++j) {
            std::shared_ptr<::raft::Entity> _shp_entity(new ::raft::Entity());

            auto _p_pre_entity_id = _shp_entity->mutable_pre_log_id();
            _p_pre_entity_id->set_term(0);
            _p_pre_entity_id->set_idx(j==0?0:j-1);

            auto _p_entity_id = _shp_entity->mutable_entity_id();
            _p_entity_id->set_term(0);
            _p_entity_id->set_idx(j);

            auto _p_wop = _shp_entity->mutable_write_op();
            _p_wop->set_key("key_" + std::to_string(j));
            _p_wop->set_value("val_" + std::to_string(j));

            _input.emplace_back(_shp_entity);
        }
        ASSERT_TRUE(BinLogGlobal::m_instance.AppendEntry(_input));
    }

    auto _end = std::chrono::steady_clock::now();
    auto _ms = std::chrono::duration_cast<std::chrono::microseconds>(_end - _start).count();

    std::cout << "time cost:" << _ms << " us with write " << _write_times << " times,"
        << _count_each_time << " items for each write, avg " << _ms/float(_write_times) << " us for each write.";

    BinLogGlobal::m_instance.UnInitialize();
}

#endif
