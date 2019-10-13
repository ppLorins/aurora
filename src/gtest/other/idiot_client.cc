/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>

#include "grpc/grpc.h"
#include "grpc++/grpc++.h"
#include "grpc/support/log.h"
#include "grpc++/server_context.h"
#include "grpc++/security/server_credentials.h"
#include "grpc++/completion_queue.h"

#include "raft.pb.h"
#include "raft.grpc.pb.h"

using ::grpc::Server;
using ::grpc::ServerAsyncResponseWriter;
using ::grpc::ServerBuilder;
using ::grpc::ServerContext;
using ::grpc::CompletionQueue;
using ::grpc::ServerCompletionQueue;
using ::grpc::Status;

class ChannelMgr {

public:

    static void Initialize(int conn_size,std::string addr)noexcept {

        for (int i = 0; i < conn_size; ++i) {
            auto _channel_args = ::grpc::ChannelArguments();
            std::string _key = "key_" + std::to_string(i);
            std::string _val = "val_" + std::to_string(i);
            _channel_args.SetString(_key,_val);

            auto shp_channel = ::grpc::CreateCustomChannel(addr, grpc::InsecureChannelCredentials(), _channel_args);

            m_channel_pool.emplace_back(shp_channel);
        }
    }

    static std::shared_ptr<::grpc::Channel> GetOneChannel()noexcept {

        static std::atomic<uint32_t>  _idx;

        uint32_t _old_val = _idx.fetch_add(1);

        uint32_t _pool_idx = _old_val % m_channel_pool.size();

        return m_channel_pool[_pool_idx];
    }

    static std::vector<std::shared_ptr<::grpc::Channel>>   m_channel_pool;
};

std::vector<std::shared_ptr<::grpc::Channel>>   ChannelMgr::m_channel_pool;

uint32_t g_count  = 50000;

std::string g_my_addr  = "127.0.0.1:10010";

struct AsyncClientCall {
    ::raft::AppendEntriesResponse reply;

    ::grpc::ClientContext context;

    Status status;

    std::unique_ptr<::grpc::ClientAsyncResponseReader<::raft::AppendEntriesResponse>> response_reader;
};

void AsyncCompleteRpc(CompletionQueue* polling_cq) {
    void* got_tag;
    bool ok = false;

    uint32_t _counter = 0;

    auto _start = std::chrono::steady_clock::now();

    //std::cout << "thread " << std::this_thread::get_id() << " start timer" << std::endl;;

    while (polling_cq->Next(&got_tag, &ok)) {

        //std::cout << "before counter:" << _counter << std::endl;

        //std::this_thread::sleep_for(std::chrono::seconds(2));

        AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
        GPR_ASSERT(ok);
        if (!call->status.ok()) {
            std::cout << call->status.error_code() << ",msg:" << call->status.error_message();
            GPR_ASSERT(false);
        }

        delete call;
        if (++_counter >= g_count)
            break;
    }

    auto _end = std::chrono::steady_clock::now();
    auto _ms = std::chrono::duration_cast<std::chrono::milliseconds>(_end - _start);

    std::cout << "thread " << std::this_thread::get_id() << " inner time cost:" << _ms.count() << std::endl;

    uint32_t _throughput = g_count / float(_ms.count()) * 1000;

    std::cout << "thread " << std::this_thread::get_id() << " inner throughput : " << _throughput << std::endl;
}

class GreeterClient {
  public:
    explicit GreeterClient(std::shared_ptr<::grpc::Channel> shp_channel,CompletionQueue* in_cq) {
        stub_ = ::raft::RaftService::NewStub(shp_channel);
        this->cq_ = in_cq;
        //this->cq_ = new CompletionQueue();
    }

    void EntrustSayHello(int idx) {
        //Shouldn't start with 0 when doing appendEntries.
        idx += 1;

        ::raft::AppendEntriesRequest request;

        request.mutable_base()->set_addr(g_my_addr);
        request.mutable_base()->set_term(0);

        auto _p_entry = request.add_replicate_entity();
        auto _p_entity_id = _p_entry->mutable_entity_id();
        _p_entity_id->set_term(0);
        _p_entity_id->set_idx(idx);

        auto _p_pre_entity_id = _p_entry->mutable_pre_log_id();
        _p_pre_entity_id->set_term(0);
        _p_pre_entity_id->set_idx(idx - 1);

        auto _p_wop = _p_entry->mutable_write_op();

        _p_wop->set_key("follower_benchmark_key_" + std::to_string(idx));
        _p_wop->set_value("follower_benchmark_val_" + std::to_string(idx));

        AsyncClientCall* call = new AsyncClientCall;

        std::chrono::time_point<std::chrono::system_clock> _deadline = std::chrono::system_clock::now()
                        + std::chrono::milliseconds(3100);
        //call->context.set_deadline(_deadline);

        call->response_reader = stub_->PrepareAsyncAppendEntries(&call->context, request, cq_);
        call->response_reader->StartCall();
        call->response_reader->Finish(&call->reply, &call->status, (void*)call);
    }

  private:

    std::unique_ptr<::raft::RaftService::Stub> stub_;

    CompletionQueue* cq_;
};


int main(int argc, char** argv) {

    if (argc != 7) {
        std::cout << "Usage:./program --count_per_thread=xx --thread_per_cq=xx --cq=xx --addr=xx --conn=xx --my_addr=xx";
        return 0;
    }

    const char * target_str =  "--count_per_thread=";
    auto p_target = std::strstr(argv[1],target_str);
    if (p_target == nullptr) {
        printf("para error argv[1] should be --count_per_thread=xx \n");
        return 0;
    }
    p_target += std::strlen(target_str);
    g_count = std::atoi(p_target);

    uint32_t thread_num = 1;
    target_str = "--thread_per_cq=";
    p_target = std::strstr(argv[2],target_str);
    if (p_target == nullptr) {
        printf("para error argv[2] should be --thread_per_cq=xx \n");
        return 0;
    }
    p_target += std::strlen(target_str);
    thread_num = std::atoi(p_target);

    target_str = "--cq=";
    p_target = std::strstr(argv[3],target_str);
    if (p_target == nullptr) {
        printf("para error argv[3] should be --cq=xx \n");
        return 0;
    }
    p_target += std::strlen(target_str);
    int _cq_num = std::atoi(p_target);

    std::string _addr = "localhost:50051";
    target_str = "--addr=";
    p_target = std::strstr(argv[4],target_str);
    if (p_target == nullptr) {
        printf("para error argv[4] should be --addr=xx \n");
        return 0;
    }
    p_target += std::strlen(target_str);
    _addr = p_target;

    target_str = "--conn=";
    p_target = std::strstr(argv[5],target_str);
    if (p_target == nullptr) {
        printf("para error argv[5] should be --conn=xx \n");
        return 0;
    }
    p_target += std::strlen(target_str);
    int _conn_size = std::atoi(p_target);

    target_str = "--my_addr=";
    p_target = std::strstr(argv[6],target_str);
    if (p_target == nullptr) {
        printf("para error argv[6] should be --my_addr=xx \n");
        return 0;
    }
    p_target += std::strlen(target_str);
    g_my_addr = p_target;

    ChannelMgr::Initialize(_conn_size, _addr);

    //std::cout << "req for each thread:" << g_count << std::endl;

    //start the polling thread on CQ first.
    std::vector<std::thread*> _vec_t;
    std::vector<CompletionQueue*> _vec_cq;

    for (int i = 0; i < _cq_num; ++i) {
        auto * _p_cq = new CompletionQueue;
        _vec_cq.push_back(_p_cq);

        for (uint32_t i = 0; i < thread_num; i++)
            _vec_t.push_back(new std::thread(AsyncCompleteRpc,_p_cq));
    }

    std::vector<std::thread*> _vec_entrusting_threads;

    auto _entrust_reqs = [&](int cq_idx, int thread_idx) {
        GreeterClient _greeter_client(ChannelMgr::GetOneChannel(), _vec_cq[cq_idx]);

        int _total_thread_num = thread_num * _cq_num;
        int _total_thread_idx = thread_num * cq_idx + thread_idx;

        for (int i = 0; i < g_count; i++) {
            int req_idx = i * _total_thread_num + _total_thread_idx;
            _greeter_client.EntrustSayHello(req_idx);  // The actual RPC call!
        }
    };

    auto _start = std::chrono::steady_clock::now();

    //start entrusting the requests.
    for (int i = 0; i < _cq_num; ++i) {
        for (int m = 0; m < thread_num; ++m) {
            std::thread* _p_t = new std::thread(_entrust_reqs, i, m);
            _vec_entrusting_threads.emplace_back(_p_t);
        }
    }

    //Waiting entrusting thread to finish.
    for (uint32_t i = 0; i < _vec_entrusting_threads.size(); i++)
        _vec_entrusting_threads[i]->join();

    std::cout << "entrusting done." << std::endl << std::flush;

    //Waiting polling thread to finish.
    for (uint32_t i = 0; i < _vec_t.size(); i++)
        _vec_t[i]->join();

    int  _total = _cq_num * thread_num * g_count;
    std::cout << "g_count:" << _total << std::endl;

    auto _end = std::chrono::steady_clock::now();
    auto _ms = std::chrono::duration_cast<std::chrono::milliseconds>(_end - _start);

    std::cout << "time cost:" << _ms.count() << std::endl;

    uint32_t _throughput = _total / float(_ms.count()) * 1000;

    std::cout << "final throughput : " << _throughput << std::endl;

    return 0;
}
