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

#include "tools/lock_free_deque.h"

namespace RaftCore::DataStructure {

template <typename T>
LockFreeUnorderedSingleList<DequeNode<T>>  LockFreeDeque<T>::m_garbage;

template <typename T>
DequeNode<T>::DequeNode() noexcept{}

template <typename T>
DequeNode<T>::DequeNode(const std::shared_ptr<T>  &p_val) noexcept{
    this->m_atomic_next.store(nullptr);
    this->m_val = p_val;

    //This is an estimated value for security.
    //TODO:why this can't compile.
    //::RaftCore::Config::FLAGS_garbage_deque_retain_num = CommonView::m_cpu_cores;
}

template <typename T>
DequeNode<T>::~DequeNode() noexcept {}

template <typename T>
LockFreeDeque<T>::LockFreeDeque() noexcept{
    //The dummy node points to itself.
    this->m_dummy = new DequeNode<T>();
    this->m_dummy->m_atomic_next.store(this->m_dummy);

    //Head and tail are initially points the dummy node which indicating the entire deque is empty.
    this->m_head.store(this->m_dummy);
    this->m_tail.store(this->m_dummy);

#ifdef _DEQUE_TEST_
    this->m_logical_size.store(0);
    this->m_physical_size.store(0);
#endif
}

template <typename T>
LockFreeDeque<T>::~LockFreeDeque() noexcept{
    auto *_p_cur = this->m_head.load()->m_atomic_next.load();
    while (_p_cur != this->m_dummy) {
        auto _p_tmp = _p_cur->m_atomic_next.load();
        delete _p_cur;
        _p_cur = _p_tmp;
    }

    delete this->m_dummy;
}

template <typename T>
void LockFreeDeque<T>::Push(const std::shared_ptr<T> &p_one, uint32_t flag) noexcept {
    //Node node points to dummy.
    auto* _p_new_node = new DequeNode<T>(p_one);
    _p_new_node->m_atomic_next.store(this->m_dummy);
    _p_new_node->m_flag = flag;

    auto* _p_insert_after  = this->m_tail.load();
    std::atomic<DequeNode<T>*> *_p_insert_pos = &_p_insert_after->m_atomic_next;

    auto* _p_tmp  = this->m_dummy;
    /*Note:
      1. 'compare_exchange_weak' is a better approach for performance, but for now,
           it is acceptable to use 'compare_exchange_strong' making code simpler and being more readable.
      2. _p_insert_after may has been freed if the free operation didn't get deferred ,so the reference to _p_insert_pos->compare_exchange_strong
            will core in that scene.     */
    while (!_p_insert_pos->compare_exchange_strong(_p_tmp, _p_new_node)) {
        _p_insert_after = _p_tmp;
        _p_insert_pos = &_p_insert_after->m_atomic_next;

        /*Insert operation must append the '_p_new_node' at the end of the deque. So '_p_tmp' must be
          set to 'this->m_dummy' each time compare_exchange_strong fails.
        */
        _p_tmp = this->m_dummy;
    }

    auto * _p_from = _p_insert_after;
    while (!this->m_tail.compare_exchange_weak(_p_from, _p_new_node))
        _p_from = _p_insert_after;

#ifdef _DEQUE_TEST_
    if (flag == 0)
        this->m_logical_size.fetch_add(1);
    this->m_physical_size.fetch_add(1);
#endif
}

template <typename T>
std::shared_ptr<T> LockFreeDeque<T>::Pop() noexcept {

    while (true) {
        auto *_deque_node = this->PopNode();
        if (_deque_node == nullptr)
            return std::shared_ptr<T>();

        //Encountering a 'fake node'.
        if (_deque_node->m_flag == 1)
            continue;

        auto _transfer = _deque_node->m_val;

        //Once the ownership has been copied out, the node itself cannot hold it.
        _deque_node->m_val.reset();

        return _transfer;
    }
}

template <typename T>
DequeNode<T>* LockFreeDeque<T>::PopNode() noexcept {

    std::atomic<DequeNode<T>*> *_p_head_next = &this->m_head.load()->m_atomic_next;

    //Judge if list is empty
    auto *_p_cur = _p_head_next->load();
    if (_p_cur == this->m_dummy)
        return _p_cur;

    auto *_p_cur_next = _p_cur->m_atomic_next.load();

    while (true) {

        //If '_p_cur' is the last node at the moment.
        if (_p_cur_next == this->m_dummy) {
            //If '_p_cur' is a 'fake-node'.
            if (_p_cur->m_flag == 1)
                return nullptr;

            //'_p_cur' isn't a 'fake-node'.  Push a 'fake-node' first.
            this->Push(std::shared_ptr<T>(), 1);

            //'_p_cur' next pointer changed, update it.
            _p_cur_next = _p_cur->m_atomic_next.load();
        }

        /* _p_cur may has been freed if freeing process didn't get deferred , so the reference to
           _p_cur->m_atomic_next.load() will core in that scene.  */
        if (!_p_head_next->compare_exchange_strong(_p_cur, _p_cur_next)) {

            //Now, '_p_cur' is the next node about to be popped.
            _p_cur_next = _p_cur->m_atomic_next.load();
            continue;
        }

        break;
    }

    m_garbage.PushFront(_p_cur);

#ifdef _DEQUE_TEST_
    if (_p_cur->m_flag == 0)
        this->m_logical_size.fetch_sub(1);
    this->m_physical_size.fetch_sub(1);
#endif

    //Always return the ptr of the node successfully popped, regardless what the role it is.
    return _p_cur;
}

template <typename T>
void LockFreeDeque<T>::GC() noexcept {
    m_garbage.PurgeSingleList(::RaftCore::Config::FLAGS_garbage_deque_retain_num);
}

#ifdef _DEQUE_TEST_
template <typename T>
std::size_t LockFreeDeque<T>::GetLogicalSize() const noexcept {
    return this->m_logical_size.load();
}

template <typename T>
std::size_t LockFreeDeque<T>::GetPhysicalSize() const noexcept {
    return this->m_physical_size.load();
}

template <typename T>
std::size_t LockFreeDeque<T>::Size() const noexcept {
    return this->GetLogicalSize();
}

template <typename T>
std::size_t LockFreeDeque<T>::GetSizeByIterating() const noexcept {

    int _counter = 0;
    auto _cur = this->m_head.load()->m_atomic_next.load();
    while (_cur != this->m_dummy) {
        _cur = _cur->m_atomic_next.load();
        _counter++;
    }

    return _counter;
}
#endif


}
