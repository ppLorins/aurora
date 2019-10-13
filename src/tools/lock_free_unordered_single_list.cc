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
#include "tools/lock_free_unordered_single_list.h"

namespace RaftCore::DataStructure {

template <typename T>
template <typename ...Args>
UnorderedSingleListNode<T>::UnorderedSingleListNode(Args&&... args)noexcept {
    this->m_data = new T(std::forward<Args>(args)...);
    this->m_next.store(nullptr);
}

template <typename T>
UnorderedSingleListNode<T>::~UnorderedSingleListNode()noexcept {
    if (this->m_data != nullptr) {
        delete this->m_data;
        this->m_data = nullptr;
    }
}

template <typename T>
UnorderedSingleListNode<T>::UnorderedSingleListNode(T* p_src)noexcept {
    this->m_data = p_src;
    this->m_next.store(nullptr);
}

template <typename T>
LockFreeUnorderedSingleList<T>::LockFreeUnorderedSingleList() noexcept {
    this->m_head.store(nullptr);
}

template <typename T>
LockFreeUnorderedSingleList<T>::~LockFreeUnorderedSingleList() noexcept{}

template <typename T>
void LockFreeUnorderedSingleList<T>::SetDeleter(std::function<void(T*)> deleter)noexcept {
    this->m_deleter = deleter;
}

template <typename T>
void LockFreeUnorderedSingleList<T>::PushFront(T* src) noexcept {


    auto *_p_cur_head = this->m_head.load();
    auto * _p_new_node = new UnorderedSingleListNode<T>(src);
    _p_new_node->m_next = _p_cur_head;
    while (!this->m_head.compare_exchange_weak(_p_cur_head, _p_new_node))
        _p_new_node->m_next = _p_cur_head;
}

template <typename T>
void LockFreeUnorderedSingleList<T>::PurgeSingleList(uint32_t retain_num) noexcept {

    std::size_t _cur_num = 1;
    auto *_p_start_point = this->m_head.load();

    while (_p_start_point != nullptr) {
        _p_start_point = _p_start_point->m_next.load();
        _cur_num++;
        if (_cur_num >= retain_num)
            break;
    }

    if (_p_start_point == nullptr)
        return;

    auto *_p_cur = _p_start_point->m_next.load();
    _p_start_point->m_next.store(nullptr);

    std::size_t _released_num = 0;
    while (_p_cur != nullptr) {
        auto *_p_next = _p_cur->m_next.load();

        //Use the customizable deleter.
        this->m_deleter(_p_cur->m_data);
        _p_cur->m_data = nullptr;
        delete _p_cur;

        _p_cur = _p_next;
        _released_num++;
    }

    if (_released_num > 0)
        VLOG(89) << "released " << _released_num << " elements in singleList's garbage list.";
}

#ifdef _UNORDERED_SINGLE_LIST_TEST_
template <typename T>
uint32_t LockFreeUnorderedSingleList<T>::Size() noexcept {
    uint32_t _size = 0;
    auto *_p_cur = this->m_head.load();
    while (_p_cur != nullptr) {
        _p_cur = _p_cur->m_next;
        _size++;
    }

    return _size;
}

template <typename T>
void LockFreeUnorderedSingleList<T>::Iterate(std::function<void(T*)> func) noexcept {
    auto *_p_cur = this->m_head.load();
    while (_p_cur != nullptr) {
        auto *_p_next = _p_cur->m_next.load();
        func(_p_cur->m_data);
        _p_cur = _p_next;
    }
}

#endif

}
