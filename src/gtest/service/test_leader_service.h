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

#ifndef __GTEST_LEADER_SERVICE_H__
#define __GTEST_LEADER_SERVICE_H__

#include <list>
#include <memory>
#include <chrono>

#include "gtest/service/test_benchmark.h"
#include "leader/leader_view.h"

class TestLeaderServiceBanchmark : public BenchmarkBase {

public:

    TestLeaderServiceBanchmark() {
        uint32_t _buf_len = ::RaftCore::Config::FLAGS_value_len + 1;
        this->m_val_buf = (char*)malloc(_buf_len);
        std::memset(this->m_val_buf, 'a', _buf_len);
        this->m_val_buf[_buf_len - 1] = '\0';
    }

    virtual ~TestLeaderServiceBanchmark() {
        free(this->m_val_buf);
    }

    virtual void EntrustBatch(std::shared_ptr<Channel> &shp_channel,
        std::shared_ptr<CompletionQueue> &shp_cq, uint32_t total,
        const FIdxGenertor &generator)noexcept override {

        WriteBenchmarkClient *_p_client = new WriteBenchmarkClient(shp_channel, shp_cq, total);

        for (std::size_t n = 0; n < total; ++n) {

            auto us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - this->m_start_tp);

            std::shared_ptr<ClientWriteRequest> _shp_req(new ClientWriteRequest());

            auto * _req = _shp_req->mutable_req();

            uint32_t _req_idx = generator(n);
            _req->set_key("leader_benchmark_key_" + std::to_string(_req_idx));

            _req->set_value(std::string(this->m_val_buf));
            _shp_req->set_timestamp(us.count());

            auto _req_setter = [&_shp_req](std::shared_ptr<ClientWriteRequest>& _target)->void {
                _target = _shp_req;
            };
            auto _f_prepare = std::bind(&::raft::RaftService::Stub::PrepareAsyncWrite,
                _p_client->GetStub().get(), std::placeholders::_1,
                std::placeholders::_2, std::placeholders::_3);

            _p_client->EntrustRequest(_req_setter, _f_prepare, ::RaftCore::Config::FLAGS_client_write_timo_ms);
        }
    }

private:

    char* m_val_buf = nullptr;

};

class TestLeaderService : public TestCluster , public TestLeaderServiceBanchmark {

    public:

        TestLeaderService() {

            //Give a long enough timeout value to facilitate unit test in debug mode l.
            //::RaftCore::Config::FLAGS_leader_append_entries_rpc_timeo_ms = 5 *1000;
            //::RaftCore::Config::FLAGS_leader_commit_entries_rpc_timeo_ms = 100 *1000;
            //::RaftCore::Config::FLAGS_leader_resync_log_rpc_timeo_ms     = 100 *1000;
            //::RaftCore::Config::FLAGS_leader_heartbeat_rpc_timeo_ms      = 100 *1000;
        }

        virtual void SetUp() override {}

        virtual void TearDown() override {}

    protected:

        void ClientRead() {

            std::shared_ptr<::grpc::Channel>   _channel = grpc::CreateChannel(this->m_leader_addr, grpc::InsecureChannelCredentials());
            std::unique_ptr<::raft::RaftService::Stub>  _stub = ::raft::RaftService::NewStub(_channel);

            //std::chrono::system_clock::time_point _deadline = std::chrono::system_clock::now() + std::chrono::seconds(1);
            //_context.set_deadline(_deadline);

            ::raft::ClientReadRequest  _r_req;
            ::raft::ClientReadResponse _r_rsp;

            _r_req.set_key("client_key_test");

            ::grpc::ClientContext    _contextX;
            ::grpc::Status _status = _stub->Read(&_contextX, _r_req, &_r_rsp);
            ASSERT_TRUE(_status.ok());
            ASSERT_TRUE(_r_rsp.client_comm_rsp().result()==::raft::ErrorCode::SUCCESS) << "ClientRead fail,detail:" << _r_rsp.DebugString();
            //ASSERT_STREQ(_r_rsp.value().c_str(),"client_val_test") << "ClientRead value not correct:" << _r_rsp.DebugString();
        }
};

class TestLeaderServiceClient : public TestBase, public TestLeaderServiceBanchmark {

    public:

        TestLeaderServiceClient() {}

        virtual ~TestLeaderServiceClient() {}

    protected:

};

TEST_F(TestLeaderService, GeneralOperation) {

    std::cout << "start general test.." << std::endl;

    this->ClientWrite();

    this->ClientRead();

    std::cout << "end general test.." << std::endl;
}

TEST_F(TestLeaderService, ConcurrentOperation) {

    //Be sure to remove binlog file before this test.

    auto _tp = this->StartTimeing();

    auto _op = [&](int thread_idx) {

        std::shared_ptr<::grpc::Channel>   _channel = grpc::CreateChannel(this->m_leader_addr, grpc::InsecureChannelCredentials());
        std::unique_ptr<::raft::RaftService::Stub>  _stub = ::raft::RaftService::NewStub(_channel);

        int _read_counter = 0;

        int _start = 0;
        int _run_times = 1000;
        for (int k = 0; k < _run_times; ++k) {

            int _write_num = 10;
            for (int i = 0; i < _write_num; ++i) {
                int _cur_idx = _start + k * _write_num * this->m_cpu_cores + thread_idx * _write_num + i;

                ClientWriteRequest  _w_req;
                ClientWriteResponse _w_rsp;

                std::string _key = "client_key_no_order_" +  std::to_string(_cur_idx);
                std::string _val = "client_val_no_order_" +  std::to_string(_cur_idx);

                auto *_p_wop = _w_req.mutable_req();
                _p_wop->set_key(_key);
                _p_wop->set_value(_val);

                ::grpc::ClientContext   _contextX;
                std::chrono::system_clock::time_point _deadline = std::chrono::system_clock::now() +
                    std::chrono::milliseconds(::RaftCore::Config::FLAGS_client_write_timo_ms);
                //_contextX.set_deadline(_deadline);

                ::grpc::Status _status = _stub->Write(&_contextX, _w_req, &_w_rsp);
                ASSERT_TRUE(_status.ok()) << "status wrong:" << _status.error_message();
                ASSERT_TRUE(_w_rsp.client_comm_rsp().result()==::raft::ErrorCode::SUCCESS) << "ClientWrite fail,detail:" << _w_rsp.DebugString();

                if (_read_counter++ > 20) {
                    ::raft::ClientReadRequest  _r_req;
                    ::raft::ClientReadResponse _r_rsp;
                    _r_req.set_key(_key);

                    ::grpc::ClientContext    _contextY;
                    std::chrono::system_clock::time_point _deadline = std::chrono::system_clock::now() + std::chrono::seconds(3);
                    //_contextY.set_deadline(_deadline);
                    ::grpc::Status _status = _stub->Read(&_contextY, _r_req, &_r_rsp);
                    ASSERT_TRUE(_status.ok()) << "status wrong:" << _status.error_message();
                    ASSERT_TRUE(_r_rsp.client_comm_rsp().result()==::raft::ErrorCode::SUCCESS) << "ClientRead fail,detail:" << _r_rsp.DebugString();

                    std::cout << "read key:" << _key << ",val:" << _r_rsp.value() << std::endl;
                    _read_counter = 0;
                }

                //std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    };

    this->LaunchMultipleThread(_op);

    this->EndTiming(_tp, "leader service benchmark cost");

    std::cout << "sleeping... CHECK if the memory cost is decreasing?????";
    std::this_thread::sleep_for(std::chrono::seconds(20));
}

TEST_F(TestLeaderService, Benchmark) {

    this->DoBenchmark(false);
}

TEST_F(TestLeaderServiceClient, Benchmark) {

    this->DoBenchmark();
}


#endif
