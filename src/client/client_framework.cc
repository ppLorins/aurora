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

#include "glog/logging.h"

#include "client/client_framework.h"

namespace RaftCore::Client {

template<typename T>
ClientFramework<T>::ClientFramework(std::shared_ptr<Channel> shp_channel) noexcept{
    this->m_stub = ::raft::RaftService::NewStub(shp_channel);
    this->Reset();
}

template<typename T>
void ClientFramework<T>::Reset() noexcept {
    this->m_client_context.reset(new ::grpc::ClientContext());
}

template<typename T>
std::shared_ptr<::raft::RaftService::Stub> ClientFramework<T>::GetStub() noexcept {
    return this->m_stub;
}

template<typename T>
ClientFramework<T>::~ClientFramework() noexcept{}

template<typename T,typename R>
ClientTpl<T, R>::ClientTpl(std::shared_ptr<Channel> shp_channel) noexcept : ClientFramework(shp_channel) {
    this->m_shp_request.reset(new T());
}

template<typename T,typename R>
ClientTpl<T,R>::~ClientTpl() noexcept {}

template<typename T,typename R>
SyncClient<T, R>::SyncClient(std::shared_ptr<Channel> shp_channel) noexcept : ClientTpl<T, R>(shp_channel) {}

template<typename T,typename R>
SyncClient<T, R>::~SyncClient() noexcept {}

template<typename T,typename R,typename CQ>
AsyncClient<T,R,CQ>::AsyncClient(std::shared_ptr<Channel> shp_channel,
    std::shared_ptr<CQ> shp_cq)noexcept : ClientTpl<T, R>(shp_channel),
    m_server_cq(shp_cq) {}

template<typename T,typename R,typename CQ>
AsyncClient<T,R,CQ>::~AsyncClient()noexcept {}

template<typename T,typename R>
UnarySyncClient<T, R>::UnarySyncClient(std::shared_ptr<Channel> shp_channel)noexcept :
    SyncClient<T, R>(shp_channel) {}

template<typename T,typename R>
const R&  UnarySyncClient<T, R>::DoRPC(std::function<void(std::shared_ptr<T>&)> req_setter,
    std::function<::grpc::Status(::grpc::ClientContext*,const T&,R*)> rpc, uint32_t timeo_ms,
    ::grpc::Status &ret_status)noexcept {

    req_setter(this->m_shp_request);
    std::chrono::system_clock::time_point deadline = std::chrono::system_clock::now() +
        std::chrono::milliseconds(timeo_ms);
    this->m_client_context->set_deadline(deadline);

    ret_status = rpc(this->m_client_context.get(), *this->m_shp_request, &this->m_response);
    return this->m_response;
}

template<typename T,typename R>
UnarySyncClient<T, R>::~UnarySyncClient()noexcept {}

template<typename T,typename R>
BidirectionalSyncClient<T, R>::BidirectionalSyncClient(std::shared_ptr<Channel> shp_channel)noexcept :
    SyncClient<T, R>(shp_channel) {}

template<typename T,typename R>
BidirectionalSyncClient<T, R>::~BidirectionalSyncClient()noexcept {}

template<typename T,typename R,typename Q,typename CQ>
UnaryAsyncClient<T,R,Q,CQ>::UnaryAsyncClient(std::shared_ptr<Channel> shp_channel,
    std::shared_ptr<CQ> shp_cq) noexcept : AsyncClient<T,R,CQ>(shp_channel, shp_cq) {
    static_assert(std::is_base_of<UnaryAsyncClient, Q>::value, "Q is not a derived from UnaryAsyncClient.");
}

template<typename T,typename R,typename Q,typename CQ>
void UnaryAsyncClient<T,R,Q,CQ>::React(bool cq_result) noexcept {

    if (!cq_result) {
        LOG(ERROR) << "UnaryAsyncClient got false result from CQ.";
        this->Release();
        return;
    }

    this->Responder(this->m_final_status, this->m_response);
    this->Release();
}

template<typename T,typename R,typename Q,typename CQ>
void UnaryAsyncClient<T,R,Q,CQ>::Release() noexcept {
    delete dynamic_cast<Q*>(this);
}

template<typename T,typename R,typename Q,typename CQ>
void UnaryAsyncClient<T,R,Q,CQ>::EntrustRequest(const std::function<void(std::shared_ptr<T>&)> &req_setter,
    const FPrepareAsync<T,R> &f_prepare_async, uint32_t timeo_ms) noexcept {
    req_setter(this->m_shp_request);

    std::chrono::time_point<std::chrono::system_clock> _deadline = std::chrono::system_clock::now() +
        std::chrono::milliseconds(timeo_ms);
    this->m_client_context->set_deadline(_deadline);

    this->m_reader = f_prepare_async(this->m_client_context.get(), *this->m_shp_request,
                                     this->m_server_cq.get());
    this->m_reader->StartCall();
    this->m_reader->Finish(&this->m_response, &this->m_final_status, dynamic_cast<ReactBase*>(this));
}

template<typename T,typename R,typename Q,typename CQ>
UnaryAsyncClient<T,R,Q,CQ>::~UnaryAsyncClient() noexcept {}

template<typename T,typename R,typename Q>
BidirectionalAsyncClient<T, R, Q>::BidirectionalAsyncClient(std::shared_ptr<Channel> shp_channel,
    std::shared_ptr<CompletionQueue> shp_cq) noexcept : AsyncClient<T,R>(shp_channel, shp_cq),
    m_async_rw(this->m_client_context.get()), m_status(ProcessStage::CONNECT) {
    static_assert(std::is_base_of<BidirectionalAsyncClient, Q>::value, "Q is not a derived from BidirectionalAsyncClient.");
}

template<typename T,typename R,typename Q>
BidirectionalAsyncClient<T,R,Q>::~BidirectionalAsyncClient() noexcept {}

template<typename T,typename R,typename Q>
void BidirectionalAsyncClient<T,R,Q>::React(bool cq_result) noexcept {

    Q* _p_downcast = dynamic_cast<Q*>(this);

    if (!cq_result && (this->m_status != ProcessStage::READ)) {
        LOG(ERROR) << "BidirectionalAsyncClient got false result from CQ.";
        delete _p_downcast;
        return;
    }

    switch (this->m_status) {
    case ProcessStage::READ:

        //Meaning client said it wants to end the stream either by a 'WritesDone' or 'finish' call.
        if (!cq_result) {
            this->m_async_rw.Finish(this->m_final_status, _p_downcast);
            this->m_status = ProcessStage::FINISH;
            break;
        }

        this->m_responder(this->m_final_status, this->m_response);
        break;

    case ProcessStage::WRITE:
        this->m_async_rw.Read(&this->m_response, _p_downcast);
        this->m_status = ProcessStage::READ;
        break;

    case ProcessStage::CONNECT:
        break;

    case ProcessStage::WRITES_DONE:
        this->m_async_rw.Finish(this->m_final_status, _p_downcast);
        this->m_status = ProcessStage::FINISH;
        break;

    case ProcessStage::FINISH:
        if (this->m_final_status.error_code() != ::grpc::StatusCode::OK) {
            LOG(ERROR) << "rpc fail,err code:" << this->m_final_status.error_code()
                << ",err msg:" << this->m_final_status.error_message();
        }
        delete _p_downcast;
        break;

    default:
        CHECK(false) << "Unexpected tag " << int(this->m_status);
    }
}

template<typename T,typename R,typename Q>
void BidirectionalAsyncClient<T, R, Q>::AsyncDo(std::function<void(std::shared_ptr<T>&)> req_setter) noexcept {
    req_setter(this->m_shp_request);
    this->m_async_rw.Write(this->m_shp_request,dynamic_cast<Q*>(this));
    this->m_status = ProcessStage::WRITE;
}

template<typename T,typename R,typename Q>
void BidirectionalAsyncClient<T, R, Q>::WriteDone() noexcept {
    this->m_async_rw.WritesDone(dynamic_cast<Q*>(this));
    this->m_status = ProcessStage::WRITES_DONE;
}

}

