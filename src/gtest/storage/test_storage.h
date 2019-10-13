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

#ifndef __GTEST_STORAGE_H__
#define __GTEST_STORAGE_H__

#include <list>
#include <memory>
#include <chrono>

#include "gtest/test_base.h"
#include "common/log_identifier.h"
#include "binlog/binlog_singleton.h"
#include "storage/storage.h"

using ::RaftCore::Storage::StorageMgr;
using ::RaftCore::BinLog::BinLogGlobal;
using ::RaftCore::Common::LogIdentifier;

class TestStorage : public TestBase {

    public:

        TestStorage() {
            //::RaftCore::Config::FLAGS_memory_table_max_item = 20;
        }

        virtual ~TestStorage() {}

        virtual void SetUp() override {

            //Clear all existing files.
            if (::RaftCore::Config::FLAGS_clear_existing_sstable_files) {
                fs::path _data_path("data");
                fs::remove_all(_data_path);
            }

            //Using the test binlog file.
            BinLogGlobal::m_instance.Initialize(_ROLE_STR_TEST_);

            this->m_lrl = BinLogGlobal::m_instance.GetLastReplicated();
            this->m_const_term = this->m_lrl.m_term;
            this->m_base_index = this->m_lrl.m_index;

            this->m_obj.Initialize(_ROLE_STR_TEST_);
        }

        virtual void TearDown() override {
            this->m_obj.UnInitialize();
        }

    protected:

        void DoRW(int thread_idx = -1) {
            for (int i = 1; i <= this->m_counter;++i) {

                uint64_t _idx = this->m_base_index + i;
                if (thread_idx >= 0)
                    _idx = this->m_base_index + thread_idx*this->m_counter + i;

                //VLOG(89) << "writing idx:" << _idx;

                LogIdentifier _log_id;
                _log_id.Set(this->m_const_term,_idx);
                std::string _i = std::to_string(_idx);
                std::string _key = "key_" + _i;
                std::string _val = "val_" + _i;

                ASSERT_TRUE(this->m_obj.Set(_log_id,_key,_val));

                std::string _val_2 = "val_" + _i;
                ASSERT_TRUE(this->m_obj.Get(_key, _val_2));
                if (_val != _val_2)
                    ASSERT_TRUE(false) << "_key:" << _key << ",expect _val:" << _val << ",actual _val:" << _val_2;
            }
        }

        void GeneralOperation() {

            this->DoRW();

            LogIdentifier _max_log_id;
            _max_log_id.Set(this->m_const_term, this->m_counter + this->m_base_index);
            ASSERT_EQ(this->m_obj.GetLastCommitted(), _max_log_id);

            LogIdentifier _log_id;
            int _start_idx = this->m_counter / 2;
            _log_id.Set(this->m_const_term,_start_idx);
            std::list<StorageMgr::StorageItem> output_list;

            int _get_count = ::RaftCore::Config::FLAGS_storage_get_slice_count;
            this->m_obj.GetSlice(_log_id,_get_count,output_list);

            ASSERT_EQ(output_list.size(), _get_count);

            _start_idx++;
            for (const auto &_item : output_list) {
                LogIdentifier _id;
                _id.Set(this->m_const_term,_start_idx);
                ASSERT_EQ(_item.m_log_id,_id);

                std::string _key = "key_" + std::to_string(_start_idx);
                ASSERT_EQ(*_item.m_key,_key);

                std::string _val = "val_" + std::to_string(_start_idx);
                ASSERT_EQ(*_item.m_value,_val);

                _start_idx++;
            }
        }

        StorageMgr  m_obj;

        LogIdentifier   m_lrl;

        uint32_t    m_const_term;

        uint64_t    m_base_index;

        int m_counter = 100;
};

TEST_F(TestStorage, GeneralOperation) {
    this->GeneralOperation();

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    this->m_obj.PurgeGarbage();

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
}

TEST_F(TestStorage, Reset) {
    for (int i = 0; i < 1000; ++i)
        this->m_obj.Reset();
}

TEST_F(TestStorage, ConcurrentOperation) {

    auto _op = [&](int idx) {

        //Note:More rounds more memory will be consumed.
        this->m_counter = 200;
        this->DoRW(idx);
    };

    this->LaunchMultipleThread(_op);

    std::cout << "start GC ....." << std::endl;

    //Check memory reclaiming status.
    this->m_obj.PurgeGarbage();

    std::cout << "start GC done, check memory usage now." << std::endl;

    std::this_thread::sleep_for(std::chrono::seconds(10));
}

#endif
