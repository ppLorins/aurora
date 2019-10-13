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

#ifndef __GTEST_SERVICE_H__
#define __GTEST_SERVICE_H__

#include <list>
#include <memory>
#include <chrono>
#include <ctime>

#include "gtest/test_base.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using ::raft::ClientWriteRequest;
using ::raft::ClientWriteResponse;
using ::raft::AppendEntriesRequest;
using ::raft::AppendEntriesResponse;
using ::raft::CommitEntryRequest;
using ::raft::CommitEntryResponse;
using ::raft::ErrorCode;

template<typename T,typename R>
using FPrepareAsync = std::function<std::unique_ptr< ::grpc::ClientAsyncResponseReader<R>>(
    ::grpc::ClientContext*,const T&, CompletionQueue*)>;

class BenchmarkTime {

public:

    BenchmarkTime() {
        this->m_start_tp = std::chrono::system_clock::from_time_t(std::mktime(&this->m_start_tm));
    }

protected:

    static uint64_t GetAvgUSLantency() {
        return m_total_latency.load();
    }

protected:

    std::chrono::time_point<std::chrono::system_clock>  m_start_tp;

    static std::atomic<uint64_t>       m_total_latency;

private:

    //2019-09-26
    std::tm    m_start_tm = { 0, 0, 0, 26, 9 - 1, 2019 - 1900 };
};

std::atomic<uint64_t>   BenchmarkTime::m_total_latency = 0;

class BenchmarkReact : public BenchmarkTime {
public:
    virtual void React(bool cq_result) noexcept = 0;
};

template<typename T,typename R>
class BenchmarkClient : public BenchmarkReact {

public:

    BenchmarkClient(std::shared_ptr<Channel> shp_channel, std::shared_ptr<CompletionQueue> shp_cq) {
        this->m_channel = shp_channel;
        this->m_cq = shp_cq;
        this->m_stub = ::raft::RaftService::NewStub(shp_channel);
        this->m_client_context.reset(new ::grpc::ClientContext());
    }

    virtual ~BenchmarkClient() {}

    virtual void React(bool cq_result) noexcept override {

        if (!cq_result) {
            LOG(ERROR) << "UnaryBenchmarkClient got false result from CQ.";
            this->Release();
            return;
        }

        this->Responder(this->m_final_status, this->m_response);
        this->Release();
    }

    void EntrustRequest(std::function<void(std::shared_ptr<T>&)> req_setter,
        const FPrepareAsync<T, R> &f_prepare_async, uint32_t timeo_ms, uint64_t idx = 0) noexcept {

        req_setter(this->m_shp_request);

        std::chrono::time_point<std::chrono::system_clock> _deadline = std::chrono::system_clock::now() +
            std::chrono::milliseconds(timeo_ms);

        //VLOG(89) << "idx:" << idx << ",start set timeout deadline,plus100";

        this->m_client_context->set_deadline(_deadline);

        //std::time_t _now = std::chrono::system_clock::to_time_t(_deadline);
        //VLOG(89) << "idx:" << idx << ",time deadline set to:" << std::put_time(std::localtime(&_now),"%H:%M:%S");

        this->m_reader = f_prepare_async(this->m_client_context.get(), *this->m_shp_request, this->m_cq.get());
        this->m_reader->StartCall();
        this->m_reader->Finish(&this->m_response, &m_final_status, this);
    }

    std::shared_ptr<::raft::RaftService::Stub> GetStub() noexcept {
        return this->m_stub;
    }

    uint64_t LogLatency(uint64_t start_us, uint64_t idx) const {
        auto _now_us = (std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now() - this->m_start_tp)).count();
        auto _latency = _now_us - start_us;
        VLOG(2) << "single req latency(us):" << _latency << ",idx:" << idx;
        return _latency;
    }

protected:

    virtual void Release() noexcept = 0;

    virtual void Responder(const ::grpc::Status& status, const R&  rsp) noexcept = 0;

protected:

    std::shared_ptr<::grpc::Channel>     m_channel;

    std::shared_ptr<CompletionQueue>     m_cq;

    std::unique_ptr<::grpc::ClientAsyncResponseReader<R>>    m_reader;

    std::shared_ptr<T>   m_shp_request;

    R   m_response;

    std::shared_ptr<::grpc::ClientContext>    m_client_context;

    std::shared_ptr<::raft::RaftService::Stub>    m_stub;

    ::grpc::Status  m_final_status;

private:

    BenchmarkClient(const BenchmarkClient&) = delete;

    BenchmarkClient& operator=(const BenchmarkClient&) = delete;
};

class CommitEntrieBenchmarkClient : public BenchmarkClient<CommitEntryRequest, CommitEntryResponse> {

public:

    CommitEntrieBenchmarkClient(std::shared_ptr<::grpc::Channel> shp_channel, std::shared_ptr<::grpc::CompletionQueue> shp_cq) :
        BenchmarkClient<CommitEntryRequest, CommitEntryResponse>(shp_channel,shp_cq) {}

    virtual ~CommitEntrieBenchmarkClient() {}

    virtual void Responder(const ::grpc::Status& status,
        const ::raft::CommitEntryResponse&  rsp) noexcept override {

        const auto &_idx = this->m_shp_request->entity_id().idx();

        VLOG(89) << "Commit got index:" << _idx;

        CHECK(status.ok()) << "error_code:" << status.error_code() << ",err msg"
            << status.error_message() << ",idx:" << _idx;

        const ::raft::CommonResponse& comm_rsp = rsp.comm_rsp();
        auto _error_code = comm_rsp.result();
        ASSERT_TRUE(_error_code == ErrorCode::SUCCESS || _error_code == ErrorCode::ALREADY_COMMITTED)
            << int(_error_code);
    }

    virtual void Release() noexcept override {
        delete this;
    }
};

class AppendEntrieBenchmarkClient : public BenchmarkClient<AppendEntriesRequest, AppendEntriesResponse> {

public:

    AppendEntrieBenchmarkClient(std::shared_ptr<::grpc::Channel> shp_channel, std::shared_ptr<::grpc::CompletionQueue> shp_cq) :
        BenchmarkClient<AppendEntriesRequest, AppendEntriesResponse>(shp_channel,shp_cq) {}

    virtual ~AppendEntrieBenchmarkClient() {}

    virtual void Responder(const ::grpc::Status& status,
        const AppendEntriesResponse&  rsp) noexcept override {

        int _lst_idx = this->m_shp_request->replicate_entity().size() - 1;
        uint64_t _lst_log_idx = this->m_shp_request->replicate_entity(_lst_idx).entity_id().idx();

        VLOG(89) << "appendEntries got index:" << _lst_log_idx;

        CHECK(status.ok()) << "error_code:" << status.error_code() << ",err msg:"
            << status.error_message() << ",idx:" <<  _lst_log_idx;

        const ::raft::CommonResponse& comm_rsp = rsp.comm_rsp();
        auto _error_code = comm_rsp.result();
        ASSERT_TRUE(_error_code == ErrorCode::SUCCESS || _error_code == ErrorCode::SUCCESS_MERGED)
            << int(_error_code);

        auto _start_us = std::atoll(comm_rsp.err_msg().c_str());
        auto _lantency_us = this->LogLatency(_start_us, _lst_log_idx);
        m_total_latency.fetch_add(_lantency_us);

        if (!::RaftCore::Config::FLAGS_do_commit)
            return;

        //Entrust commit request.
        std::shared_ptr<CommitEntryRequest>   _shp_commit_req(new CommitEntryRequest());
        std::string _local_addr = std::string(_AURORA_LOCAL_IP_) + ":"
                                + std::to_string(_RAFT_UNIT_TEST_LEADER_PORT_);
        _shp_commit_req->mutable_base()->set_addr(_local_addr);
        _shp_commit_req->mutable_base()->set_term(0);

        auto _p_entity_id = _shp_commit_req->mutable_entity_id();
        _p_entity_id->set_term(0);

        _p_entity_id->set_idx(_lst_log_idx);

        auto * _p_commit_client = new CommitEntrieBenchmarkClient(this->m_channel, this->m_cq);

        auto _req_setter = [&](std::shared_ptr<::raft::CommitEntryRequest>& _target)->void {
            _target = _shp_commit_req;
        };
        auto _f_prepare =  std::bind(&::raft::RaftService::Stub::PrepareAsyncCommitEntries,
                                    _p_commit_client->GetStub().get(), std::placeholders::_1,
                                    std::placeholders::_2, std::placeholders::_3);
        _p_commit_client->EntrustRequest(_req_setter, _f_prepare,
                                    ::RaftCore::Config::FLAGS_leader_commit_entries_rpc_timeo_ms);

        VLOG(89) << "client entrust commit of idx:" << _lst_log_idx;
    }

    virtual void Release() noexcept override {
        delete this;
    }
};

class WriteBenchmarkClient : public BenchmarkClient<ClientWriteRequest, ClientWriteResponse>{

public:

    WriteBenchmarkClient(std::shared_ptr<::grpc::Channel> shp_channel,
        std::shared_ptr<::grpc::CompletionQueue> shp_cq, int idx) :
        BenchmarkClient<ClientWriteRequest, ClientWriteResponse>(shp_channel, shp_cq), m_idx(idx) {}

    virtual ~WriteBenchmarkClient() {}

    virtual void Responder(const ::grpc::Status& status,
        const ClientWriteResponse&  rsp) noexcept override {

        VLOG(89) << "fetch,idx:" << this->m_idx;

        if (!status.ok()) {
            LOG(ERROR) << "error_code:" << status.error_code() << ",err msg"
                << status.error_message() << ", idx:" << m_idx;
            return;
        }

        const ::raft::ClientCommonResponse& _client_comm_rsp = rsp.client_comm_rsp();
        auto _error_code = _client_comm_rsp.result();
        ASSERT_TRUE(_error_code == ErrorCode::SUCCESS) << "err code:" << int(_error_code)
            << ",err msg:" << _client_comm_rsp.err_msg() << ",idx:" << this->m_idx;

        auto _start_us = std::atoll(_client_comm_rsp.err_msg().c_str());
        auto _lantency_us = this->LogLatency(_start_us, this->m_idx);
        m_total_latency.fetch_add(_lantency_us);
    }

    virtual void Release() noexcept override {
        delete this;
    }

private:

    uint64_t m_idx;
};

class BenchmarkBase : public BenchmarkTime {

public:

    BenchmarkBase(bool leader_svc = true) {

        std::string _leader_addr = std::string(_AURORA_LOCAL_IP_) + ":" + std::to_string(_RAFT_UNIT_TEST_LEADER_PORT_);
        std::string _follower_addr = std::string(_AURORA_LOCAL_IP_) + ":" + std::to_string(_RAFT_UNIT_TEST_FOLLWER_PORT_);

        this->m_leader_svc  = leader_svc;
        this->m_target_addr = this->m_leader_svc ? _leader_addr : _follower_addr;

        std::string _target_ip = ::RaftCore::Config::FLAGS_target_ip;
        if (_target_ip != "default_none")
            this->m_target_addr = _target_ip;

        this->m_thread_num_per_cq = ::RaftCore::Config::FLAGS_benchmark_client_thread_num_per_cq;
        this->m_cq_num = ::RaftCore::Config::FLAGS_benchmark_client_cq_num;

        this->m_req_num_per_thread = ::RaftCore::Config::FLAGS_follower_svc_benchmark_req_round;
        if (::RaftCore::Config::FLAGS_do_commit)
            this->m_req_num_per_thread *= 2;;

        if (this->m_leader_svc)
            this->m_req_num_per_thread = ::RaftCore::Config::FLAGS_leader_svc_benchmark_req_count;

        this->m_total_req_num = this->m_req_num_per_thread * this->m_thread_num_per_cq * this->m_cq_num;
    }

    virtual ~BenchmarkBase() {
        for (auto &_cq : this->m_vec_cq)
            _cq->Shutdown();
    }

    virtual void EntrustClient2CQ(std::shared_ptr<Channel> shp_channel,
        std::shared_ptr<CompletionQueue> shp_cq, int idx)noexcept = 0;

    void DoBenchmark(bool pure_client = true)noexcept {

        std::vector<std::shared_ptr<::grpc::Channel>>   _vec_channel;

        for (std::size_t i = 0; i < ::RaftCore::Config::FLAGS_conn_per_link; ++i) {
            auto _channel_args = ::grpc::ChannelArguments();

            std::string _key = "key_" + std::to_string(i);
            std::string _val = "val_" + std::to_string(i);
            _channel_args.SetString(_key,_val);

            _vec_channel.emplace_back(::grpc::CreateCustomChannel(this->m_target_addr, grpc::InsecureChannelCredentials(), _channel_args));
        }

        for (std::size_t i = 0; i < this->m_cq_num; ++i)
            this->m_vec_cq.emplace_back(new CompletionQueue());

        auto _thread_func = [&](int cq_idx) {
            void* tag;
            bool ok;

            auto _start = std::chrono::steady_clock::now();

            uint32_t  _cur_got_num = 0;

            auto _shp_cq = this->m_vec_cq[cq_idx];

            while (true) {

                if (_cur_got_num >= this->m_req_num_per_thread)
                    break;

                _shp_cq->Next(&tag, &ok);

                BenchmarkReact* _p_ins = static_cast<BenchmarkReact*>(tag);
                _p_ins->React(ok);

                _cur_got_num++;

                continue;
            }

            auto _end = std::chrono::steady_clock::now();
            auto _ms = std::chrono::duration_cast<std::chrono::milliseconds>(_end - _start);

            std::cout << "thread " << std::this_thread::get_id() << " inner time cost:" << _ms.count() << std::endl;

            uint32_t _throughput = (uint32_t)(_cur_got_num / float(_ms.count()) * 1000);

            std::cout << "thread " << std::this_thread::get_id() << " inner throughput : " << _throughput << std::endl;
        };

        //start the polling thread on CQ first.
        std::vector<std::thread*>   _polling_threads;
        for (std::size_t i = 0; i < this->m_cq_num; ++i) {
            for (std::size_t j = 0; j < this->m_thread_num_per_cq; ++j) {
                std::thread *_pthread = new std::thread(_thread_func, i);
                _polling_threads.push_back(_pthread);
                VLOG(89) << "clientCQ thread : " << _pthread->get_id() << " for CQ:" << i << " started.";
            }
        }

        std::cout << "set timeout value begin" << std::endl << std::flush;

        //start entrusting the requests.
        auto _entrust_reqs = [&](int cq_idx, int thread_idx) {

            int _channel_num = _vec_channel.size();

            int _total_thread_num = this->m_thread_num_per_cq * this->m_cq_num;
            int _total_thread_idx = this->m_thread_num_per_cq * cq_idx + thread_idx;

            auto &_shp_channel = _vec_channel[_total_thread_idx % _channel_num];

            for (std::size_t i = 0; i < this->m_req_num_per_thread; ++i) {
                int req_idx = i * _total_thread_num + _total_thread_idx;
                this->EntrustClient2CQ(_shp_channel, this->m_vec_cq[cq_idx], req_idx);
                VLOG(89) << "entrusted idx:" << req_idx;
            }
        };

        auto _start = std::chrono::steady_clock::now();

        std::vector<std::thread*>   _entrusting_threads;

        for (std::size_t n = 0; n < this->m_cq_num; ++n) {
            for (std::size_t j = 0; j < this->m_thread_num_per_cq; ++j) {
                auto* _p_thread = new std::thread(_entrust_reqs, n, j);
                _entrusting_threads.push_back(_p_thread);
            }
        }

        for (auto &_item : _polling_threads)
            _item->join();

        auto _end = std::chrono::steady_clock::now();
        auto _ms = std::chrono::duration_cast<std::chrono::milliseconds>(_end - _start);

        std::cout << "req number " << m_total_req_num << " total cost(ms):" << _ms.count() << std::endl;

        float _throughput = m_total_req_num / float(_ms.count()) * 1000;

        uint64_t _total_latency = GetAvgUSLantency();

        uint64_t _avg_latenct_us = (uint64_t)(_total_latency / float(m_total_req_num));

        std::cout << " inner throughput : " << _throughput << ",avg latency(us):" << _avg_latenct_us << std::endl;

        //Entrusting threads are the least to wait.
        for (auto &_item : _entrusting_threads)
            _item->join();

        if (pure_client)
            return;

        int _waiting_finish_seconds = 5;
        std::cout << "waiting for ongoing remote processing to be finished for " << _waiting_finish_seconds << " seconds.";
        std::this_thread::sleep_for(std::chrono::seconds(_waiting_finish_seconds));
    }

private:

    std::string  m_target_addr = "";

    uint32_t    m_req_num_per_thread = 0;

    uint32_t    m_cq_num = 0;

    uint32_t    m_thread_num_per_cq = 0;

    uint32_t    m_total_req_num = 0;

    std::vector<std::shared_ptr<CompletionQueue>>   m_vec_cq;

    bool m_leader_svc = false;

private:

    BenchmarkBase(const BenchmarkBase&) = delete;

    BenchmarkBase& operator=(const BenchmarkBase&) = delete;
};

#endif
