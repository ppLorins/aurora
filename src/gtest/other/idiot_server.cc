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

#include <memory>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

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

int g_thread_pair_num = 1;
int g_cq_pair_num = 1;
int g_pool = 1;

class ServerImpl final {
 public:
  ~ServerImpl() {
    server_->Shutdown();
    // Always shutdown the completion queue after the server.
    for (const auto& _cq : m_notify_cq)
        _cq->Shutdown();

    //for (const auto& _cq : m_call_cq)
    //    _cq->Shutdown();

  }

  // There is no shutdown handling in this code.
  void Run() {
    std::string server_address("0.0.0.0:60051");

    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service_" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *asynchronous* service.
    builder.RegisterService(&service_);
    // Get hold of the completion queue used for the asynchronous communication
    // with the gRPC runtime.

    for (int i = 0; i < g_cq_pair_num; ++i) {
        //cq_ = builder.AddCompletionQueue();

        m_notify_cq.emplace_back(builder.AddCompletionQueue());
        std::cout << "notify_cq:" << m_notify_cq[m_notify_cq.size() - 1].get() << " added." << std::endl;

        //m_call_cq.emplace_back(builder.AddCompletionQueue());
        //std::cout <<"call_cq:" << m_call_cq[m_call_cq.size() - 1].get()  << " added." << std::endl;
    }

    // Finally assemble the server.
    server_ = builder.BuildAndStart();
    std::cout << "Server listening on " << server_address << std::endl;

    // Proceed to the server's main loop.
    std::vector<std::thread*> _vec_threads;

    for (int i = 0; i < g_thread_pair_num ; ++i) {
        int _cq_idx = i % g_cq_pair_num;
        for (int j = 0; j < g_pool; ++j)
            new CallData(&service_,m_notify_cq[_cq_idx].get());

        _vec_threads.emplace_back(new std::thread(&ServerImpl::HandleRpcs, this, m_notify_cq[_cq_idx].get()));
    }

    std::cout << g_thread_pair_num << " working aysnc threads spawned" << std::endl;

    for (const auto& _t : _vec_threads)
        _t->join();
  }

 private:
  // Class encompassing the state and logic needed to serve a request.
    class CallData {
     public:
      CallData(::raft::RaftService::AsyncService* service, ::grpc::ServerCompletionQueue* notify_cq)
          : service_(service), notify_cq_(notify_cq), responder_(&ctx_), status_(CREATE) {
        Proceed();
      }

      void Proceed() {
        if (status_ == CREATE) {
          status_ = PROCESS;

          service_->RequestAppendEntries(&ctx_, &request_, &responder_, notify_cq_, notify_cq_, this);
        } else if (status_ == PROCESS) {
            new CallData(service_, notify_cq_);

          reply_.mutable_comm_rsp()->set_result(::raft::ErrorCode::SUCCESS);

          //std::cout << "i'm here" << std::endl;

          status_ = FINISH;
          responder_.Finish(reply_, ::grpc::Status::OK, this);
        } else {
          delete this;
        }
      }

     private:
      ::raft::RaftService::AsyncService* service_;
      ::grpc::ServerCompletionQueue* notify_cq_;

      ::grpc::ServerContext ctx_;

      ::raft::AppendEntriesRequest request_;
      ::raft::AppendEntriesResponse reply_;

      ::grpc::ServerAsyncResponseWriter<::raft::AppendEntriesResponse> responder_;

      enum CallStatus { CREATE, PROCESS, FINISH };
      CallStatus status_;  // The current serving state.
    };

      void HandleRpcs(ServerCompletionQueue *poll_cq) {
         uint32_t _counter = 0;
        void* tag;
        bool ok;
        while (true) {

          GPR_ASSERT(poll_cq->Next(&tag, &ok));
          GPR_ASSERT(ok);

          static_cast<CallData*>(tag)->Proceed();
        }
      }

      std::vector<std::unique_ptr<ServerCompletionQueue>>  m_notify_cq;

      //std::vector<std::unique_ptr<ServerCompletionQueue>>  m_call_cq;

      ::raft::RaftService::AsyncService service_;
      std::unique_ptr<::grpc::Server> server_;
    };

const char* ParseCmdPara( char* argv,const char* para) {
    auto p_target = std::strstr(argv,para);
    if (p_target == nullptr) {
        printf("para error argv[%s] should be %s \n",argv,para);
        return nullptr;
    }
    p_target += std::strlen(para);
    return p_target;
}

int main(int argc, char** argv) {

  if (argc != 4) {
      std::cout << "Usage:./program --thread_pair=xx --cq_pair=xx --pool=xx";
      return 0;
  }

  g_thread_pair_num = std::atoi(ParseCmdPara(argv[1],"--thread_pair="));
  g_cq_pair_num = std::atoi(ParseCmdPara(argv[2],"--cq_pair="));
  g_pool = std::atoi(ParseCmdPara(argv[3],"--pool="));

  ServerImpl server;
  server.Run();

  return 0;
}
