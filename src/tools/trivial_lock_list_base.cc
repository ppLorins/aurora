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

#include "config/config.h"
#include "tools/trivial_lock_list_base.h"

namespace RaftCore::DataStructure {

template<typename T>
ThreadIDWrapper<T>::ThreadIDWrapper(std::thread::id tid)noexcept {
    this->m_tid = tid;
}

template<typename T>
ThreadIDWrapper<T>::~ThreadIDWrapper()noexcept {}

template<typename T>
bool ThreadIDWrapper<T>::operator<(const ThreadIDWrapper &other)const noexcept {
    return this->m_tid < other.m_tid;
}

template<typename T>
bool ThreadIDWrapper<T>::operator==(const ThreadIDWrapper &other)const noexcept {
    return this->m_tid == other.m_tid;
}

//TODO:figure out why this compiles fail under VS2015
//template <typename T>
//const TrivialLockSingleList<T>::ThreadIDWrapper& TrivialLockSingleList<T>::ThreadIDWrapper::operator=(
//    const ThreadIDWrapper &other)noexcept {
//    this->m_tid = other.m_tid;
//    return *this;
//}

template<typename T>
std::size_t ThreadIDWrapper<T>::Hash()const noexcept {
    return std::hash<std::thread::id>{}(this->m_tid);
}

template<typename T>
std::thread::id ThreadIDWrapper<T>::GetTid() const noexcept {
    return this->m_tid;
}

template<typename T>
OperationTracker<T>::OperationTracker()noexcept {
    uint32_t _slot_num = ::RaftCore::Config::FLAGS_list_op_tracker_hash_slot_num;
    this->m_p_insert_footprint = new LockFreeHashAtomic<ThreadIDWrapper<void>, T>(_slot_num);

    static_assert(std::is_base_of<OrderedTypeBase<T>,T>::value,"template parameter of OperationTracker invalid");
}

template<typename T>
OperationTracker<T>::~OperationTracker()noexcept {}

template <typename T>
void OperationTracker<T>::WaitForListClean(T* output_tail) noexcept {
    bool _finished = true;
    //Waiting for unfinished insertions inside the cut off list to be finished.
    auto _checker = [&](const std::shared_ptr<ThreadIDWrapper<void>> &k, const std::shared_ptr<T> &v) ->bool {
        if (!v)
            return true;

        if (*v.get() > *output_tail)
            return true;

        _finished = false;

        return false;
    };

    do {
        std::this_thread::yield();
        _finished = true;
        this->m_p_insert_footprint->Iterate(_checker);
    } while (!_finished);
}

} //end namespace

