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

#include "common/request_base.h"

namespace RaftCore::Common {

template<typename T,typename R,typename Q>
const char*  BidirectionalRequest<T, R, Q>::m_status_macro_names[] = { "READ","WRITE","CONNECT","DONE","FINISH" };

template<typename T>
RequestBase<T>::RequestBase() noexcept {}

template<typename T>
void RequestBase<T>::Initialize(std::shared_ptr<RaftService::AsyncService> shp_svc,
    std::shared_ptr<ServerCompletionQueue> &shp_notify_cq,
    std::shared_ptr<ServerCompletionQueue> &shp_call_cq) noexcept {
    this->m_async_service    = shp_svc;
    this->m_server_notify_cq = shp_notify_cq;
    this->m_server_call_cq   = shp_call_cq;
}

template<typename T>
RequestBase<T>::~RequestBase() noexcept{}

template<typename T,typename R>
RequestTpl<T, R>::RequestTpl() noexcept {}

template<typename T,typename R>
RequestTpl<T,R>::~RequestTpl() noexcept {}

template<typename T,typename R,typename Q>
UnaryRequest<T, R, Q>::UnaryRequest() noexcept : m_responder(&this->m_server_context) {
    static_assert(std::is_base_of<UnaryRequest, Q>::value, "Q is not a derived from UnaryRequest.");
    this->m_stage = ProcessStage::CREATE;
}

template<typename T,typename R,typename Q>
void UnaryRequest<T,R,Q>::React(bool cq_result) noexcept {

    Q* _p_downcast = dynamic_cast<Q*>(this);

    if (!cq_result) {
        LOG(ERROR) << "UnaryRequest got false result from CQ.";
        delete _p_downcast;
        return;
    }

    auto _status = ::grpc::Status::OK;
    switch (this->m_stage) {
    case ProcessStage::CREATE:
        /* Spawn a new subclass instance to serve new clients while we process
         the one for this . The instance will deallocate itself as
         part of its FINISH state.*/
        new Q(this->m_async_service,this->m_server_notify_cq,this->m_server_call_cq);

        // The actual processing.
        _status = this->Process();

        /* And we are done! Let the gRPC runtime know we've finished, using the
         memory address of this instance as the uniquely identifying tag for
         the event.*/
        this->m_stage = ProcessStage::FINISH;
        this->m_responder.Finish(this->m_response, _status, _p_downcast);
        break;

    case ProcessStage::FINISH:
        delete _p_downcast;
        break;

    default:
        CHECK(false) << "Unexpected tag " << int(this->m_stage);
        break;
    }
}

template<typename T,typename R,typename Q>
UnaryRequest<T,R,Q>::~UnaryRequest() noexcept {}

template<typename T,typename R,typename Q>
BidirectionalRequest<T, R, Q>::BidirectionalRequest() noexcept : m_reader_writer(&this->m_server_context) {
    this->m_stage = ProcessStage::CONNECT;
    this->m_server_context.AsyncNotifyWhenDone(this);
}

template<typename T,typename R,typename Q>
BidirectionalRequest<T,R,Q>::~BidirectionalRequest() noexcept {}

template<typename T,typename R,typename Q>
void BidirectionalRequest<T,R,Q>::React(bool cq_result) noexcept {

    Q* _p_downcast = dynamic_cast<Q*>(this);

    if (!cq_result && (this->m_stage != ProcessStage::READ)) {
        LOG(ERROR) << "BidirectionalRequest got false result from CQ, state:" << this->GetStageName();
        delete _p_downcast;
        return;
    }

    /*The `ServerAsyncReaderWriter::Finish()` call will resulting into two notifications for a single
        request.  Processing those two notifications simultaneously will causing problems. So we
        need a synchronization here.  */
    std::unique_lock<std::mutex> _wlock(this->m_mutex);

    auto _status = ::grpc::Status::OK;
    switch (this->m_stage) {
    case ProcessStage::READ:

        //Meaning client said it wants to end the stream either by a 'WritesDone' or 'finish' call.
        if (!cq_result) {
            this->m_reader_writer.Finish(::grpc::Status::OK, _p_downcast);
            this->m_stage = ProcessStage::DONE;
            break;
        }

        _status = this->Process();
        if (!_status.ok()) {
            LOG(ERROR) << "bidirectional request going to return a non-success result:"
                << _status.error_code() << ",msg:" << _status.error_message();
            this->m_reader_writer.Finish(::grpc::Status::OK, _p_downcast);
            this->m_stage = ProcessStage::DONE;
            break;
        }

        this->m_reader_writer.Write(this->m_response, _p_downcast);
        this->m_stage = ProcessStage::WRITE;
        break;

    case ProcessStage::WRITE:
        this->m_reader_writer.Read(&this->m_request, _p_downcast);
        this->m_stage = ProcessStage::READ;
        break;

    case ProcessStage::CONNECT:
        //Spawn a new instance to serve further incoming request.
        new Q(this->m_async_service,this->m_server_notify_cq,this->m_server_call_cq);

        this->m_reader_writer.Read(&this->m_request, _p_downcast);
        this->m_stage = ProcessStage::READ;
        break;

    case ProcessStage::DONE:
        this->m_stage = ProcessStage::FINISH;
        break;

    case ProcessStage::FINISH:
        _wlock.unlock();
        delete _p_downcast;
        break;

    default:
        CHECK(false) << "Unexpected tag " << int(this->m_stage);
    }
}

template<typename T,typename R,typename Q>
const char* BidirectionalRequest<T, R, Q>::GetStageName()const noexcept {
    return m_status_macro_names[(int)this->m_stage];
}

}

