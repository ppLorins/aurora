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

#ifndef __GTEST_FOLLOWER_SERVICE_H__
#define __GTEST_FOLLOWER_SERVICE_H__

#include <list>
#include <memory>
#include <chrono>

#include "gtest/service/test_benchmark.h"
#include "follower/follower_view.h"

class TestFollowerServiceBanchmark :  public BenchmarkBase {

public:

    TestFollowerServiceBanchmark() : BenchmarkBase(false) {}

    virtual ~TestFollowerServiceBanchmark() {}

    virtual std::string GetLeaderAddr()const noexcept = 0;

    virtual void EntrustClient2CQ(std::shared_ptr<Channel> shp_channel,
        std::shared_ptr<CompletionQueue> shp_cq, int idx)noexcept override {

        //Shouldn't start with 0 when doing appendEntries.
        idx += 1;

        auto *_p_append_client =  new AppendEntrieBenchmarkClient(shp_channel, shp_cq);

        std::shared_ptr<AppendEntriesRequest> _shp_req(new AppendEntriesRequest());

        std::string _my_addr = this->GetLeaderAddr();
        if (::RaftCore::Config::FLAGS_my_ip != std::string("default_none"))
            _my_addr = ::RaftCore::Config::FLAGS_my_ip;

        _shp_req->mutable_base()->set_addr(_my_addr);
        _shp_req->mutable_base()->set_term(0);

        auto _p_entry = _shp_req->add_replicate_entity();
        auto _p_entity_id = _p_entry->mutable_entity_id();
        _p_entity_id->set_term(0);
        _p_entity_id->set_idx(idx);

        auto _p_pre_entity_id = _p_entry->mutable_pre_log_id();
        _p_pre_entity_id->set_term(0);
        _p_pre_entity_id->set_idx(idx - 1);

        auto _p_wop = _p_entry->mutable_write_op();

        _p_wop->set_key("follower_benchmark_key_" + std::to_string(idx));
        _p_wop->set_value("follower_benchmark_val_" + std::to_string(idx));

        static std::tm    m_start_tm = { 0, 0, 0, 26, 9 - 1, 2019 - 1900 };
        static auto  m_start_tp = std::chrono::system_clock::from_time_t(std::mktime(&m_start_tm));
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - m_start_tp);
        _shp_req->set_debug_info(std::to_string(us.count()));

        auto _req_setter = [&_shp_req](std::shared_ptr<::raft::AppendEntriesRequest>& _target)->void {
            _target = _shp_req;
        };
        auto _f_prepare =  std::bind(&::raft::RaftService::Stub::PrepareAsyncAppendEntries,
                                    _p_append_client->GetStub().get(), std::placeholders::_1,
                                    std::placeholders::_2, std::placeholders::_3);
        _p_append_client->EntrustRequest(_req_setter, _f_prepare,
            ::RaftCore::Config::FLAGS_leader_append_entries_rpc_timeo_ms);
    }

};

class TestFollowerService : public TestSingleBackendFollower, public TestFollowerServiceBanchmark {

    public:

        TestFollowerService() {}

        virtual void SetUp() override {
            //::RaftCore::Config::FLAGS_memory_table_max_item = 10;
            ::RaftCore::Config::FLAGS_checking_heartbeat = false;
        }

        virtual void TearDown() override {}

    protected:

        std::string GetLeaderAddr()const noexcept override {
            return this->m_leader_addr;
        }

        void DoHeartBeat() {

            std::shared_ptr<::grpc::Channel>   _channel = grpc::CreateChannel(this->m_follower_svc_addr, grpc::InsecureChannelCredentials());
            std::unique_ptr<::raft::RaftService::Stub>  _stub = ::raft::RaftService::NewStub(_channel);

            ::grpc::ClientContext    _context;
            std::chrono::system_clock::time_point _deadline = std::chrono::system_clock::now() + std::chrono::seconds(1);
            //_context.set_deadline(_deadline);

            ::raft::HeartBeatRequest  _req;
            ::raft::CommonResponse    _rsp;

            _req.mutable_base()->set_addr(this->m_leader_addr);
            _req.mutable_base()->set_term(0);

            ::grpc::Status _status = _stub->HeartBeat(&_context, _req, &_rsp);

            ASSERT_TRUE(_status.ok());
            ASSERT_TRUE(_rsp.result()==ErrorCode::SUCCESS) << "DoHeartBeat fail,detail:" << _rsp.DebugString();
        }

        void DoAppendEntriesCommit() {

            std::string _target_ip = ::RaftCore::Config::FLAGS_target_ip ;
            std::string _real_ip = this->m_follower_svc_addr;

            if (_target_ip != "default_none")
                _real_ip = _target_ip;

            std::shared_ptr<::grpc::Channel>   _channel = grpc::CreateChannel(_real_ip, grpc::InsecureChannelCredentials());
            std::unique_ptr<::raft::RaftService::Stub>  _stub = ::raft::RaftService::NewStub(_channel);

            AppendEntriesRequest  _req;
            AppendEntriesResponse _rsp;

            _req.mutable_base()->set_addr(this->m_leader_addr);
            _req.mutable_base()->set_term(0);

            //8057:overlap, 8062:exact match,8063:disorder.
            int _start = ::RaftCore::Config::FLAGS_append_entries_start_idx;
            int _sum   = 10;
            for (int i = _start; i < _start + _sum; ++i) {
                auto _p_entity = _req.add_replicate_entity();

                auto _p_entity_id = _p_entity->mutable_entity_id();
                _p_entity_id->set_term(0);
                _p_entity_id->set_idx(i);

                auto _p_pre_entity_id = _p_entity->mutable_pre_log_id();
                _p_pre_entity_id->set_term(0);
                _p_pre_entity_id->set_idx(i==0?0:i-1);

                auto _p_wop = _p_entity->mutable_write_op();
                std::string _idx = std::to_string(i);
                _p_wop->set_key("key_" + _idx);
                _p_wop->set_value("val_" + _idx);
            }

            ::grpc::ClientContext    _contextX;
            std::chrono::system_clock::time_point _deadline = std::chrono::system_clock::now() +
                    std::chrono::milliseconds(::RaftCore::Config::FLAGS_leader_append_entries_rpc_timeo_ms);
            _contextX.set_deadline(_deadline);

            ::grpc::Status _status = _stub->AppendEntries(&_contextX, _req, &_rsp);
            ASSERT_TRUE(_status.ok()) << ", error_code:" << _status.error_code() << ",err msg:" << _status.error_message();
            ASSERT_TRUE(_rsp.comm_rsp().result()==ErrorCode::SUCCESS ||
                        _rsp.comm_rsp().result()==ErrorCode::SUCCESS_MERGED) << "DoAppendEntriesCommit fail,detail:" << _rsp.DebugString();

            //Committing
            CommitEntryRequest      _commit_req;
            CommitEntryResponse     _commit_rsp;

            _commit_req.mutable_base()->set_addr(this->m_leader_addr);
            _commit_req.mutable_base()->set_term(0);

            auto _p_entity_id = _commit_req.mutable_entity_id();
            _p_entity_id->set_term(0);
            _p_entity_id->set_idx(_start + _sum - 1);

            ::grpc::ClientContext    _context;
            _deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(::RaftCore::Config::FLAGS_leader_commit_entries_rpc_timeo_ms);
            _context.set_deadline(_deadline);

            _status = _stub->CommitEntries(&_context, _commit_req, &_commit_rsp);
            ASSERT_TRUE(_status.ok());
            ASSERT_TRUE(_commit_rsp.comm_rsp().result()==ErrorCode::SUCCESS ||
                        _commit_rsp.comm_rsp().result()==ErrorCode::ALREADY_COMMITTED) << "DoAppendEntriesCommit fail,detail:" << _commit_rsp.DebugString();
        }

        void DoSyncData() {

            std::shared_ptr<::grpc::Channel>   _channel = grpc::CreateChannel(this->m_follower_svc_addr, grpc::InsecureChannelCredentials());
            std::unique_ptr<::raft::RaftService::Stub>  _stub = ::raft::RaftService::NewStub(_channel);

            ::grpc::ClientContext    _context;

            std::shared_ptr<::grpc::ClientReaderWriter<::raft::SyncDataRequest,::raft::SyncDataResponse>>  _stream =  _stub->SyncData(&_context);

            ::raft::SyncDataRequest _req;
            _req.mutable_base()->set_term(0);
            _req.mutable_base()->set_addr(this->m_leader_addr);
            _req.set_msg_type(::raft::SyncDataMsgType::PREPARE);
            _stream->Write(_req);

            ::raft::SyncDataResponse    _rsp;

            _rsp.Clear();
            CHECK(_stream->Read(&_rsp));
            CHECK_EQ(_rsp.comm_rsp().result(), ErrorCode::PREPARE_CONFRIMED) << "sync data fail,msg:" << _rsp.DebugString();

            //Sync data.
            int _write_times = 5;
            int _counter = 10;

            for (int j = 0; j < _write_times; ++j) {

                _req.clear_entity();
                _req.set_msg_type(::raft::SyncDataMsgType::SYNC_DATA);

                int _start = j * _counter;
                int _end   = _start + _counter;

                for (int i = _start; i < _end; ++i) {
                    auto _p_entity = _req.add_entity();

                    auto _p_pre_log_id = _p_entity->mutable_pre_log_id();
                    _p_pre_log_id->set_term(0);
                    _p_pre_log_id->set_idx(i-1);

                    auto _p_entity_id = _p_entity->mutable_entity_id();
                    _p_entity_id->set_term(0);
                    _p_entity_id->set_idx(i);

                    std::string _idx = std::to_string(i);

                    auto _p_wop = _p_entity->mutable_write_op();
                    _p_wop->set_key("resync_data_key_" + _idx);
                    _p_wop->set_value("resync_data_val_" + _idx);
                }

                _stream->Write(_req);

                _rsp.Clear();
                _stream->Read(&_rsp);

                CHECK_EQ(_rsp.comm_rsp().result(), ErrorCode::SYNC_DATA_CONFRIMED) << "sync data fail,msg:" << _rsp.DebugString();
            }

            //Sync log.
            int _log_write_times = 5;
            int _log_counter     = 10;
            int _log_start = _log_write_times * _log_counter;

            for (int j = 0; j < _log_write_times; ++j) {

                _req.clear_entity();
                _req.set_msg_type(::raft::SyncDataMsgType::SYNC_LOG);

                int _start = _log_start + j * _log_counter;
                int _end   = _start + _log_counter;

                for (int i = _start; i < _end; ++i) {
                    auto _p_entity = _req.add_entity();

                    auto _p_pre_log_id = _p_entity->mutable_pre_log_id();
                    _p_pre_log_id->set_term(0);
                    _p_pre_log_id->set_idx(i-1);

                    auto _p_entity_id = _p_entity->mutable_entity_id();
                    _p_entity_id->set_term(0);
                    _p_entity_id->set_idx(i);

                    std::string _idx = std::to_string(i);

                    auto _p_wop = _p_entity->mutable_write_op();
                    _p_wop->set_key("resync_log_key_" + _idx);
                    _p_wop->set_value("resync_log_val_" + _idx);
                }

                _stream->Write(_req);

                _rsp.Clear();
                _stream->Read(&_rsp);

                CHECK_EQ(_rsp.comm_rsp().result(), ErrorCode::SYNC_LOG_CONFRIMED) << "sync data fail,msg:" << _rsp.DebugString();
            }

            CHECK(_stream->WritesDone()) << "client writes done fail.";
            ::grpc::Status _status = _stream->Finish();

            CHECK(_status.ok()) << "error_code:" << _status.error_code() << ",err msg:"
                << _status.error_message();
        }
};

class TestFollowerServiceClient : public TestBase, public TestFollowerServiceBanchmark {

    public:

        TestFollowerServiceClient() {}

        virtual ~TestFollowerServiceClient() {}

    protected:

        std::string GetLeaderAddr()const noexcept override {
            return this->m_leader_addr;
        }
};

TEST_F(TestFollowerService, GeneralOperation) {

    this->DoAppendEntriesCommit();
    this->DoSyncData();
    this->DoHeartBeat();
}

TEST_F(TestFollowerService, ContinuousWorking) {
    this->DoAppendEntriesCommit();
}

TEST_F(TestFollowerService, ConcurrentOperation) {

    auto _tp = this->StartTimeing();

    //Be sure to remove binlog file before this test.

    int _test_thread_num = ::RaftCore::Config::FLAGS_concurrent_client_thread_num;
    if (_test_thread_num <= 0)
        _test_thread_num = this->m_cpu_cores;

    std::mutex  _mutex;

    std::atomic<int> _counter;
    _counter.store(0);

    int _sum = 1000;
    auto _op = [&](int thread_idx) {

        std::shared_ptr<::grpc::Channel>   _channel = grpc::CreateChannel(this->m_follower_svc_addr, grpc::InsecureChannelCredentials());
        std::unique_ptr<::raft::RaftService::Stub>  _stub = ::raft::RaftService::NewStub(_channel);

        std::chrono::system_clock::time_point _deadline = std::chrono::system_clock::now() + std::chrono::seconds(1);
        //_context.set_deadline(_deadline);

        AppendEntriesRequest  _req;
        AppendEntriesResponse _rsp;

        CommitEntryRequest      _commit_req;
        CommitEntryResponse     _commit_rsp;

        int _run_times = 1000;
        for (int k = 0; k < _run_times; ++k) {

            _req.Clear();
            _req.mutable_base()->set_term(0);
            _req.mutable_base()->set_addr(this->m_leader_addr);

            int _write_num   = 10;
            for (int i = 0; i < _write_num; ++i) {

                int _cur_idx = k * _write_num * _test_thread_num + thread_idx * _write_num + i;

                auto _p_entity = _req.add_replicate_entity();

                auto _p_entity_id = _p_entity->mutable_entity_id();
                _p_entity_id->set_term(0);
                _p_entity_id->set_idx(_cur_idx);

                auto _p_pre_entity_id = _p_entity->mutable_pre_log_id();
                _p_pre_entity_id->set_term(0);
                _p_pre_entity_id->set_idx(_cur_idx==0?0:_cur_idx-1);

                auto _p_wop = _p_entity->mutable_write_op();
                std::string _idx = std::to_string(_cur_idx);
                _p_wop->set_key("key_" + _idx);
                _p_wop->set_value("val_" + _idx);
            }

            int _start = k * _write_num * _test_thread_num + thread_idx * _write_num;
            int _end   = k * _write_num * _test_thread_num + thread_idx * _write_num + _write_num - 1;

            _mutex.lock();
            std::cout << "thread " << thread_idx <<  " write log from " << _start  << " to " << _end << std::endl;
            _mutex.unlock();

            ::grpc::ClientContext    _contextX;
            ::grpc::Status _status = _stub->AppendEntries(&_contextX, _req, &_rsp);

            _counter.fetch_add(1);

            ASSERT_TRUE(_status.ok());
            ASSERT_TRUE(_rsp.comm_rsp().result()==ErrorCode::SUCCESS || _rsp.comm_rsp().result()==ErrorCode::SUCCESS_MERGED) << "DoAppendEntriesCommit fail,detail:" << _rsp.DebugString();

            //Committing
            _commit_req.Clear();
            _commit_req.mutable_base()->set_addr(this->m_leader_addr);
            _commit_req.mutable_base()->set_term(0);

            auto _p_entity_id = _commit_req.mutable_entity_id();
            _p_entity_id->set_term(0);
            _p_entity_id->set_idx(_end);

            ::grpc::ClientContext    _context;
            _status = _stub->CommitEntries(&_context, _commit_req, &_commit_rsp);
            ASSERT_TRUE(_status.ok());
            ASSERT_TRUE(_rsp.comm_rsp().result()==ErrorCode::SUCCESS) << "DoAppendEntriesCommit fail,detail:" << _rsp.DebugString();
        }

    };

    this->LaunchMultipleThread(_op,_test_thread_num);

    std::cout << "total req send&recv:" << _counter.load() << std::endl;

    this->EndTiming(_tp, "Follower service benchmark cost");

    std::cout << "sleeping... CHECK if the memory cost is decreasing...???";
    std::this_thread::sleep_for(std::chrono::seconds(5));
}

TEST_F(TestFollowerService, Benchmark) {

    this->DoBenchmark(false);
}

TEST_F(TestFollowerServiceClient, Benchmark) {

    this->DoBenchmark();
}

#endif
