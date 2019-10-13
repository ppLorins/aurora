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

#include "common/react_group.h"

namespace RaftCore::Common {

template<typename T>
ReactWorkGroup<T>::ReactWorkGroup(TypePtrCQ<T> shp_cq, TypeReactorFunc reactor, int therad_num) noexcept {
    this->m_shp_cq = shp_cq;
    this->m_reactor = reactor;
    this->m_polling_threads_num = therad_num;
}

template<typename T>
ReactWorkGroup<T>::~ReactWorkGroup() {}

template<typename T>
void ReactWorkGroup<T>::StartPolling() noexcept {
    for (int i = 0; i < this->m_polling_threads_num; ++i) {
        std::thread *_p_thread = new std::thread(&ReactWorkGroup<T>::GrpcPollingThread, this);
        this->m_vec_threads.emplace_back(_p_thread);
        LOG(INFO) << "polling thread:" << _p_thread->get_id() << " for cq :" << this->m_shp_cq.get()
            << " started.";
    }
}

template<typename T>
void ReactWorkGroup<T>::GrpcPollingThread() noexcept {
    void* tag;
    bool ok;
    ::RaftCore::Common::ReactInfo _info;

    while (this->m_shp_cq->Next(&tag, &ok)) {
        _info.Set(ok, tag);
        this->m_reactor(_info);
    }
}

template<typename T>
void ReactWorkGroup<T>::WaitPolling() noexcept {
    for (auto& _thread : this->m_vec_threads)
        _thread->join();
}

template<typename T>
TypePtrCQ<T> ReactWorkGroup<T>::GetCQ() noexcept {
    return this->m_shp_cq;
}

template<typename T>
void ReactWorkGroup<T>::ShutDownCQ() noexcept {
    this->m_shp_cq->Shutdown();
}

}

