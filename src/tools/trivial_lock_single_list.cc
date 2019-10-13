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

#include "tools/trivial_lock_single_list.h"

namespace RaftCore::DataStructure {

template <typename T>
SingleListNode<T>::SingleListNode(const std::shared_ptr<T>  &shp_val) noexcept{
    static_assert(std::is_base_of<OrderedTypeBase<T>,T>::value,"template parameter of TrivialLockSingleList invalid");
    this->m_val = shp_val;
    this->m_atomic_next.store(nullptr);
}

template <typename T>
SingleListNode<T>::~SingleListNode() noexcept {}

template <typename T>
bool SingleListNode<T>::operator<(const SingleListNode& other) const noexcept {
    return *this->m_val < *other.m_val;
}

template <typename T>
bool SingleListNode<T>::operator>(const SingleListNode& other) const noexcept {
    return *this->m_val > *other.m_val;
}

template <typename T>
bool SingleListNode<T>::operator==(const SingleListNode& other) const noexcept {
    return *this->m_val == *other.m_val;
}

template <typename T>
void SingleListNode<T>::Apply(SingleListNode<T>* phead, std::function<void(SingleListNode<T>*)> unary) noexcept {
    auto _p_cur  = phead;
    while (_p_cur != nullptr) {
        //Note: unary may modify _p_cur's next pointer after its execution.
        auto _p_next = _p_cur->m_atomic_next.load();
        unary(_p_cur);
        _p_cur = _p_next;
    }
}

template <typename T>
TrivialLockSingleList<T>::TrivialLockSingleList(const std::shared_ptr<T> &p_min, const std::shared_ptr<T> &p_max) noexcept {
    static_assert(std::is_base_of<OrderedTypeBase<T>,T>::value,"template parameter of TrivialLockSingleList invalid");

    this->m_head = new SingleListNode<T>(p_min);
    this->m_tail = new SingleListNode<T>(p_max);

    this->m_head->m_atomic_next.store(this->m_tail);
}

template <typename T>
TrivialLockSingleList<T>::~TrivialLockSingleList() noexcept{
    this->Clear();
}

template <typename T>
void TrivialLockSingleList<T>::Clear() noexcept {
    auto *_p_cur = this->m_head->m_atomic_next.load();
    while (_p_cur != this->m_tail) {
        auto tmp = _p_cur;
        _p_cur = _p_cur->m_atomic_next.load();
        delete tmp;
    }

    this->m_head->m_atomic_next.store(this->m_tail);
}

template <typename T>
SingleListNode<T>* TrivialLockSingleList<T>::SetEmpty() noexcept {
    auto *_p_old = this->m_head->m_atomic_next.load();
    while (!this->m_head->m_atomic_next.compare_exchange_weak(_p_old, this->m_tail))
        continue;

    if (_p_old == this->m_tail)
        return nullptr;

    auto *_p_cur  = _p_old;
    auto *_p_next = _p_cur->m_atomic_next.load();
    while (_p_next != this->m_tail) {
        _p_cur = _p_next;
        _p_next = _p_next->m_atomic_next.load();
    }

    _p_cur->m_atomic_next.store(nullptr);

    return _p_old;
}

template <typename T>
void TrivialLockSingleList<T>::Insert(const std::shared_ptr<T> &p_one) noexcept {
    SingleListNode<T>* new_node = new SingleListNode<T>(p_one);
    this->Insert(new_node);
}

template <typename T>
void TrivialLockSingleList<T>::Insert(SingleListNode<T>* new_node) noexcept {
    this->InsertTracker(new_node);
}

template <typename T>
void TrivialLockSingleList<T>::InsertTracker(SingleListNode<T>* new_node) noexcept {

    ThreadIDWrapper<void> *_p_tracker = new ThreadIDWrapper<void>(std::this_thread::get_id());

    bool _ownership_taken = false;
    if (this->m_p_insert_footprint->Upsert(_p_tracker, new_node))
        _ownership_taken = true;

    while (!this->InsertRaw(new_node))
        VLOG(89) << "-------redo InsertRaw!-----";

    //Since _p_tracker already exist, the return value must be true.
    CHECK(!this->m_p_insert_footprint->Upsert(_p_tracker, nullptr));

    if (!_ownership_taken)
        delete _p_tracker;
}

template <typename T>
bool TrivialLockSingleList<T>::InsertRaw(SingleListNode<T>* new_node) noexcept {

    const auto &_p_one = new_node->m_val;

    auto *_p_pre = this->m_head;
    auto *_p_cur  = _p_pre->m_atomic_next.load();

    while (true) {

        //Reaching the end of the cut head list, need to start over again.
        if (_p_cur == nullptr)
            return false;

        if (_p_cur == this->m_tail) {
            new_node->m_atomic_next.store(this->m_tail);
            if (!_p_pre->m_atomic_next.compare_exchange_strong(_p_cur, new_node))
                continue;
            break;
        }

        if (*_p_one == *_p_cur->m_val && !_p_cur->IsDeleted())
            return true;

        if (*_p_one > *_p_cur->m_val) {
            _p_pre = _p_cur;
            _p_cur = _p_cur->m_atomic_next.load();
            continue;
        }

        //Once get here, the new node should be inserted between _p_pre and _p_cur.
        new_node->m_atomic_next.store(_p_cur);

        if (!_p_pre->m_atomic_next.compare_exchange_strong(_p_cur, new_node))
            continue;

        break;
    }

    return true;
}

template <typename T>
bool TrivialLockSingleList<T>::Delete(const std::shared_ptr<T> &p_one) noexcept {

    auto *_p_node = new SingleListNode<T>(p_one);

    ThreadIDWrapper<void> *_p_tracker = new ThreadIDWrapper<void>(std::this_thread::get_id());

    bool _ownership_taken = false;
    if (this->m_p_insert_footprint->Upsert(_p_tracker, _p_node))
        _ownership_taken = true;

    //Default to that the element to be deleted not found.
    bool _ret_val = false;

    auto *_p_cur = this->m_head;
    while (_p_cur != this->m_tail) {
        if (*p_one != *_p_cur->m_val) {
            _p_cur = _p_cur->m_atomic_next.load();
            continue;
        }
        _p_cur->SetDeleted();
        _ret_val = true;
        break;
    }

    CHECK(!this->m_p_insert_footprint->Upsert(_p_tracker, nullptr));

    if (!_ownership_taken) {
        delete _p_tracker;
        delete _p_node;
    }

    return _ret_val;
}

template <typename T>
void TrivialLockSingleList<T>::SiftOutDeleted(SingleListNode<T>* &output_head) noexcept {
    auto *_to_remove = output_head;
    decltype(_to_remove) _p_pre = nullptr;

    while (_to_remove != nullptr) {
        bool _deleted = false;

        if (_to_remove->IsDeleted()) {

            auto _p_next = _to_remove->m_atomic_next.load();

            if (_p_pre)
                _p_pre->m_atomic_next.store(_p_next);
            else
                output_head = _p_next;

            _deleted = true;
        }

        auto  _tmp = _to_remove;
        _to_remove = _to_remove->m_atomic_next.load();

        if (_deleted)
            delete _tmp;
        else
            _p_pre = _tmp;
    }
}

template <typename T>
SingleListNode<T>* TrivialLockSingleList<T>::CutHead(std::function<bool(const T &one)> criteria) noexcept {

    /*Note: In the current design , there can be only one thread invoking this method.But it
        is allowed to have several other threads doing Insert at the mean time.  */
    std::unique_lock<std::recursive_mutex> _mutex_lock(this->m_recursive_mutex);

    auto *_p_pre   = this->m_head;
    auto *_p_cur   = _p_pre->m_atomic_next.load();
    auto *_p_start = _p_cur;

    if (_p_cur == this->m_tail)
        return nullptr;

    //VLOG(89) << "debug double list cutting starts from:" << _p_cur->m_val->PrintMe();

    while (true) {

        if (criteria(*_p_cur->m_val)) {
            _p_pre  = _p_cur;
            _p_cur  = _p_cur->m_atomic_next.load();
            if (_p_cur != this->m_tail)
                continue;
        }

        //No nodes are available
        if (_p_cur == _p_start)
            return nullptr;

        //-----------Start cutting head-----------//
        if (!_p_pre->m_atomic_next.compare_exchange_strong(_p_cur, nullptr)) {
            _p_cur = _p_pre;
            continue;
        }

        //Cutting done,just break out.
        break;
    }

    /*Note: Once goes here, _p_pre  is the latest item of the cut out list and
                            _p_cur is the first item of the remaining list.  */
    //Detach first node
    auto _p_tmp = _p_start;
    while (!this->m_head->m_atomic_next.compare_exchange_strong(_p_tmp, _p_cur)) {
        decltype(_p_cur) _p_tmp_x = nullptr;
        CHECK(_p_pre->m_atomic_next.compare_exchange_strong(_p_tmp_x, _p_cur));
        //VLOG(89) << "-------recursive cuthead occur!-----";
        return this->CutHead(criteria);
    }

    auto *_output_head = _p_start;

    //VLOG(89) << "cut head waitdone";

    this->WaitForListClean(_p_cur);

    //Now the list is cut off with the deleted elements. Need to erase the deleted elements
    this->SiftOutDeleted(_output_head);

    //VLOG(89) << "debug double list leave with something";

    return _output_head;
}

template <typename T>
SingleListNode<T>* TrivialLockSingleList<T>::CutHeadByValue(const T &val) noexcept {
    auto judge_smaller_equal = [&](const T &one) -> bool{ return one <= val; };

    //VLOG(89) << "start cuthead less than:" << val.PrintMe();

    return this->CutHead(judge_smaller_equal);
}

template <typename T>
void TrivialLockSingleList<T>::ReleaseCutHead(SingleListNode<T>* output_head) noexcept {
    auto _p_cur  = output_head;
    while (_p_cur != nullptr) {
        auto _p_next = _p_cur->m_atomic_next.load();
        delete _p_cur;
        _p_cur  = _p_next;
    }
}

template <typename T>
void TrivialLockSingleList<T>::IterateCutHead(std::function<bool(std::shared_ptr<T> &)> accessor, SingleListNode<T>* output_head) const noexcept {
    auto _p_cur  = output_head;
    while (_p_cur != nullptr) {
        auto _p_next = _p_cur->m_atomic_next.load();
        accessor(_p_cur->m_val);
        _p_cur  = _p_next;
    }
}

template <typename T>
void TrivialLockSingleList<T>::Iterate(std::function<bool(std::shared_ptr<T> &)> accessor) const noexcept {

    auto *_cur = this->m_head->m_atomic_next.load();
    while (_cur != nullptr) {

        if (_cur == this->m_tail)
            break;

        auto _p_tmp = _cur;
        _cur = _cur->m_atomic_next.load();

        if (_p_tmp->IsDeleted())
            continue;

        if (!accessor(_p_tmp->m_val))
            break;
    }
}

template <typename T>
bool TrivialLockSingleList<T>::Empty() const noexcept {
    return this->m_head->m_atomic_next.load() == this->m_tail;
}

#ifdef _SINGLE_LIST_TEST_
template <typename T>
int TrivialLockSingleList<T>::GetSize() const noexcept {

    int _size = 0;
    auto *_cur = this->m_head->m_atomic_next.load();
    while (_cur != this->m_tail) {
        if (!_cur->IsDeleted())
            _size++;
        _cur = _cur->m_atomic_next.load();
    }

    return _size;
}

template <typename T>
SingleListNode<T>* TrivialLockSingleList<T>::GetHead() const noexcept {
    return this->m_head;
}

#endif

}
