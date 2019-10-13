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

#include "tools/trivial_lock_double_list.h"

namespace RaftCore::DataStructure {

template <typename T>
DoubleListNode<T>::DoubleListNode(const std::shared_ptr<T>  &p_val) noexcept{

    static_assert(std::is_base_of<OrderedTypeBase<T>,T>::value,"template parameter of TrivialLockDoubleList invalid");

    this->m_atomic_pre.store(nullptr);
    this->m_atomic_next.store(nullptr);

    this->m_val = p_val;
}

template <typename T>
DoubleListNode<T>::~DoubleListNode() noexcept {}

template <typename T>
bool DoubleListNode<T>::operator<(const DoubleListNode& other) const noexcept {
    return *this->m_val < *other.m_val;
}

template <typename T>
bool DoubleListNode<T>::operator>(const DoubleListNode& other) const noexcept {
    return *this->m_val > *other.m_val;
}

template <typename T>
bool DoubleListNode<T>::operator==(const DoubleListNode& other) const noexcept {
    return *this->m_val == *other.m_val;
}

template <typename T>
void DoubleListNode<T>::Apply(DoubleListNode<T>* phead, std::function<void(DoubleListNode<T>*)> unary) noexcept {
    auto _p_cur  = phead;
    while (_p_cur != nullptr) {
        //Caution: unary may modify _p_cur's next pointer after its execution.
        auto _p_next = _p_cur->m_atomic_next.load();
        unary(_p_cur);
        _p_cur = _p_next;
    }
}

template <typename T>
TrivialLockDoubleList<T>::TrivialLockDoubleList(const std::shared_ptr<T> &p_min ,const std::shared_ptr<T> &p_max) noexcept{

    static_assert(std::is_base_of<OrderedTypeBase<T>,T>::value,"template parameter of TrivialLockDoubleList invalid");

    CHECK(p_min && p_max) << "TrivialLockDoubleList init fail";

    /* There are at least two nodes in the double linked list:
        1> the HEAD node with the minimum of T
        2> the TAIL node with the maximum of T

        This is for conveniently inserting.  */

    //Init dump head node<T>
    this->m_head = new DoubleListNode<T>(p_min);

    //Init dump tail node<T>
    this->m_tail = new DoubleListNode<T>(p_max);

    //Join the two together,no need to lock
    this->m_head->m_atomic_next.store(this->m_tail);
    this->m_tail->m_atomic_pre.store(this->m_head);
}

template <typename T>
TrivialLockDoubleList<T>::~TrivialLockDoubleList() noexcept{
    this->Clear();

    delete this->m_head;
    delete this->m_tail;
}

template <typename T>
void TrivialLockDoubleList<T>::Clear() noexcept {

    //Reserve head & tail node.
    auto _p_cur = this->m_head->m_atomic_next.load();
    while (_p_cur != this->m_tail) {
        auto tmp = _p_cur;
        _p_cur = _p_cur->m_atomic_next.load();
        delete tmp;
    }

    this->m_head->m_atomic_next.store(this->m_tail);
    this->m_tail->m_atomic_pre.store(this->m_head);
}

template <typename T>
void TrivialLockDoubleList<T>::Insert(const std::shared_ptr<T> &p_one) noexcept {
    DoubleListNode<T>* new_node = new DoubleListNode<T>(p_one);
    this->Insert(new_node);
}

template <typename T>
void TrivialLockDoubleList<T>::Insert(DoubleListNode<T>* new_node) noexcept {
    this->InsertTracker(new_node);
}

template <typename T>
void TrivialLockDoubleList<T>::InsertTracker(DoubleListNode<T>* new_node) noexcept {

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
bool TrivialLockDoubleList<T>::InsertRaw(DoubleListNode<T>* new_node) noexcept{

    const auto &_p_one = new_node->m_val;

    //The pointer points to the node just after the current node being iterated
    auto _p_next = this->m_tail;

    //The pointer to the current node being iterated, initially they are both pointing to the tail
    auto _p_cur = _p_next->m_atomic_pre.load();

    /* Note : The above two pointers are not necessarily being adjacent all the time.
                They may pointing to the same node in certain scenarios .  */
    while (true) {
        /*Note: Case where '_p_cur == nullptr' could happen: iterating reach the end of a cut head .
            Since _p_cur already points to the cut list, it need to start all over again.  */
        if (_p_cur == nullptr)
            return false;

        //Replace the old value with the new one in case of a partial comparison.
        if (*_p_one == *_p_cur->m_val && !_p_cur->IsDeleted()) {
            //TODO: delete then insert.
            //_p_cur->m_val = _p_one;
            return true;
        }

        /*Note:
          1.Deleted elements will be treated like the normal(non-deleted) ones. Since maintaining order of
            the list without considering the deleted elements is just equivalent to considering them,thinking
            about inserting a new node after a deleted node with a value greater than it violating nothing on
            the correctness , but the latter form will introduce huge complexity.
          2.For the case of inserting CAS fail and due to conflict with cutting head, the following judge will
            get satisfied AS BEFORE. And will trigger _p_cur==nullptr eventually, so it's safe to do a recursive
            insertion above.
        */
        if ( *_p_one < *_p_cur->m_val ) {
            //Both moving toward to the head direction
            _p_next = _p_cur;
            _p_cur  = _p_cur->m_atomic_pre.load();
            continue;
        }

        //For the deleted elements,insert the equivalent one just after it as above says.

        //Alway assume the new node should be inserted between _p_cur and _p_next at the moment
        new_node->m_atomic_pre.store(_p_cur);
        new_node->m_atomic_next.store(_p_next);

        //Start inserting....
        auto tmp_next = _p_next; // Copy it out first. This is very important !!
        if (!_p_cur->m_atomic_next.compare_exchange_strong(tmp_next, new_node)) {
            /*Collision happened. Other thread(s) have already modified the 'next'
              pointer of '_p_cur',we need to redo the inserting process.There are two
              scenarios where this could happen:
              1> other thread(s) are inserting new node.
              2> other thread(s) are cutting head.  */

            //Reset the conditions and start the inserting process from scratch all over again
            VLOG(89) << "insert_CAS_fail," << _p_cur << " insert_next_changefrom " << _p_next << " to " << tmp_next;
            _p_cur = _p_next;
            continue;
        }

        auto _p_tmp = _p_cur;
        bool _rst = _p_next->m_atomic_pre.compare_exchange_strong(_p_tmp, new_node);
        if(!_rst)
            CHECK(false) << "TrivialLockDoubleList<T>::Insert unexpected inserting status found,"
                        << "cannot CAS node's previous pointer,something terribly wrong happened."
                        << " insert_CAS_fail ," << _p_next << " insert_pre_changefrom "
                        << _p_cur << " to " <<  _p_tmp << ", head:" << this->m_head;

        //this->GetSize();

        //Here the new node should be inserted properly,stop iterating
        break;
    }

    return true;
}

template <typename T>
bool TrivialLockDoubleList<T>::Delete(const std::shared_ptr<T> &p_one) noexcept {

    //Use a stack memory to get around multiple thread allocation/deallocation issue.
    auto *_p_node = new DoubleListNode<T>(p_one);

    ThreadIDWrapper<void> *_p_tracker = new ThreadIDWrapper<void>(std::this_thread::get_id());

    bool _ownership_taken = false;
    if (this->m_p_insert_footprint->Upsert(_p_tracker, _p_node))
        _ownership_taken = true;

    //Default to that the element to be deleted not found.
    bool _ret_val = false;

    auto *_p_cur = this->m_tail->m_atomic_pre.load();

    //Reach head of the [cutoff] list.
    while (_p_cur != nullptr && _p_cur != this->m_head) {
        if (*p_one != *_p_cur->m_val) {
            _p_cur = _p_cur->m_atomic_pre.load();
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
void TrivialLockDoubleList<T>::DeleteAll() noexcept {
    //Reserve head & tail node.
    auto _p_cur = this->m_head->m_atomic_next.load();
    while (_p_cur != this->m_tail) {
        _p_cur->SetDeleted();
        _p_cur = _p_cur->m_atomic_next.load();
    }
}

//template <typename T>
//bool TrivialLockDoubleList<T>::MoveForward(DoubleListNode<T>* &p_pre,DoubleListNode<T>* &p_next) noexcept {
//
//    auto _reach_tail = [&]() {
//        return p_pre == this->m_tail || p_next == this->m_tail;
//    };
//
//    if (_reach_tail())
//        return false;
//
//    do {
//        /*If reach here , no elements could be inserted between p_pre  and p_next since they
//          are adjacent. So the following move forward operations are safe .  */
//        p_pre  = p_next;
//        p_next = p_next->m_atomic_next.load();
//        if (_reach_tail())
//            return false;
//    } while (p_pre->IsDeleted() && p_next->IsDeleted());
//
//    return true;
//}

template <typename T>
bool TrivialLockDoubleList<T>::MoveForward(DoubleListNode<T>* &p_pre,DoubleListNode<T>* &p_next) noexcept {

    p_pre  = p_next;
    p_next = this->FindNextNonDelete(p_next);
    if (p_next == this->m_tail)
        return false;

    return true;
}

template <typename T>
DoubleListNode<T>* TrivialLockDoubleList<T>::ExpandForward(DoubleListNode<T>* p_cur) noexcept {
    if (p_cur == this->m_tail)
        return p_cur;

    auto *_p_pre = p_cur;
    auto *_p_x   = _p_pre->m_atomic_next.load();

    while (_p_x != this->m_tail) {
        if (!_p_x->IsDeleted())
            break;

        if (_p_x->operator!=(*p_cur))
            break;

        _p_pre = _p_x;
        _p_x   = _p_pre->m_atomic_next.load();
    }

    return _p_pre;
}

template <typename T>
DoubleListNode<T>* TrivialLockDoubleList<T>::ExpandBackward(DoubleListNode<T>* p_cur) noexcept {
    if (p_cur == this->m_head)
        return p_cur;

    auto *_p_pre = p_cur;
    auto *_p_x   = _p_pre->m_atomic_pre.load();

    while (_p_x != this->m_head) {
        if (!_p_x->IsDeleted())
            break;

        if (_p_x->operator!=(*p_cur))
            break;

        _p_pre = _p_x;
        _p_x   = _p_pre->m_atomic_pre.load();
    }

    return _p_pre;
}

template <typename T>
DoubleListNode<T>* TrivialLockDoubleList<T>::FindNextNonDelete(DoubleListNode<T>* p_cur) noexcept {

    if (p_cur == this->m_tail)
        return this->m_tail;

    auto * _p_next_non_deleted = p_cur->m_atomic_next.load();
    while (_p_next_non_deleted != this->m_tail) {
        if (!_p_next_non_deleted->IsDeleted())
            break;
        _p_next_non_deleted = _p_next_non_deleted->m_atomic_next.load();
    }

    return _p_next_non_deleted;
}

template <typename T>
void TrivialLockDoubleList<T>::SiftOutDeleted(DoubleListNode<T>* &output_head) noexcept {
    auto _to_remove = output_head;
    while (_to_remove != nullptr) {
        bool _deleted = false;
        if (_to_remove->IsDeleted()) {
            auto _p_pre  = _to_remove->m_atomic_pre.load();
            auto _p_next = _to_remove->m_atomic_next.load();

            if (_p_pre)
                _p_pre->m_atomic_next.store(_p_next);
            else
                output_head = _p_next;

            if(_p_next)
                _p_next->m_atomic_pre.store(_p_pre);

            _deleted = true;
        }

        auto  tmp = _to_remove;
        _to_remove = _to_remove->m_atomic_next.load();

        if (_deleted)
            delete tmp;
    }
}

template <typename T>
DoubleListNode<T>* TrivialLockDoubleList<T>::CutHead(std::function<bool(const T &left, const T &right)> criteria) noexcept {

    /*Note: In the current design , there can be only one thread invoking this method.But it
        is allowed to have several other threads doing Insert at the mean time.  */
    std::unique_lock<std::recursive_mutex> _mutex_lock(this->m_recursive_mutex);

    //VLOG(89) << "list_debug pos1";

    auto *_p_cur = this->FindNextNonDelete(this->m_head);
    if (_p_cur == this->m_tail) {
        //VLOG(89) << "list_debug pos1.1";
        return nullptr;
    }

    //VLOG(89) << "list_debug pos2";

    //output_head always point the first element regardless of deleted or non-deleted.
    auto* _p_immediate_first = this->m_head->m_atomic_next.load();
    auto *output_head =  _p_immediate_first;

    //The first element will always be cut off.
    auto _p_next = this->FindNextNonDelete(_p_cur);

    while (true) {

        //VLOG(89) << "list_debug pos2.1";

        if (criteria(*_p_cur->m_val, *_p_next->m_val)) {
            //VLOG(89) << "list_debug pos2.2";
            if (this->MoveForward(_p_cur, _p_next)) {
                //VLOG(89) << "list_debug pos2.3";
                continue;
            }
        }

        //VLOG(89) << "list_debug pos3";

        //To get around the deleted elements, we need to expand '_p_cur' and '_p_next'.
        _p_cur  = this->ExpandForward(_p_cur);
        _p_next = this->ExpandBackward(_p_next);

        //-----------Start cutting head-----------//

        /*This is the tricky part,need to consider simultaneously Inserting and CutHeading : If we set _p_cur->next
           to nullptr successfully, no other threads could insert new node between _p_cur and _p_next.This is the critical
           safety guarantee for other operations.  */
        auto _p_tmp = _p_next;
        if (!_p_cur->m_atomic_next.compare_exchange_strong(_p_tmp, nullptr)) {
            /*Strong CAS fail ,means that other thread(s) already made _p_next to point to
            the newly inserted node.What we need to do is just redo the iterating from current node.
            Also _p_next need to be updated to the newly inserted node,otherwise the criteria will
            be evaluated to false forever.  */
            VLOG(89) << "cuthead_CAS_fail," << _p_cur << " next_changefrom " << _p_next << " to " << _p_tmp;
            _p_next = _p_tmp;
            continue;
        }

        //Cutting done,just break out.
        break;
    }

    /*Note: Once goes here, _p_cur  is the latest item of the cut out list and
                            _p_next is the first  item of the remaining list.  */

    //Connect the head with the first non adjacent node.
    auto _p_tmp = this->m_head->m_atomic_next.load();
    while (!this->m_head->m_atomic_next.compare_exchange_strong(_p_tmp,_p_next)) {
        /*If reach here , mean new nodes already been inserted between m_head and the cutting head.
          To avoid cut off non adjacent nodes,the cutting head process need starting allover again.    */

        //Recovery _p_cur's next pointer,should never fail.
        decltype(_p_next) _p_tmp = nullptr;
        CHECK(_p_cur->m_atomic_next.compare_exchange_strong(_p_tmp, _p_next));
        VLOG(89) << "-------recursive cuthead occur!-----";
        return this->CutHead(criteria);
    }

    //VLOG(89) << "list_debug pos4";

    _p_tmp = _p_cur;
    while (!_p_next->m_atomic_pre.compare_exchange_strong(_p_tmp, this->m_head)) {
        /* '_p_next->m_atomic_pre' may still points to the old place due to an uncomplete inserting process.
            What we need to do is just waiting for it pointing to the updated place where is exactly _p_cur.  */
        LOG(WARNING) << "cutting head tail->pre CAS_fail " << _p_next << " previous change from "
                << _p_cur << " to " << _p_tmp << ",head:" << this->m_head << ",tail:" << this->m_tail
                << ",do CAS again.";
        _p_tmp = _p_cur;
        continue;
    }

    //Detach first node
    output_head->m_atomic_pre.store(nullptr);

    //VLOG(89) << "list_debug pos5 " << output_head;

    //Waiting for all threads that are iterating in the cut off list to be finished.
    this->WaitForListClean(_p_cur);

    //VLOG(89) << "list_debug pos6 " << output_head;

    //auto *_tmp = output_head;
    //while (_tmp != nullptr) {
    //    VLOG(89) << "list_debug pos6.1 " << _tmp->IsDeleted();
    //    _tmp = _tmp->m_atomic_next.load();
    //}

    //Now the  list is cut off with the deleted elements. Need to erase the deleted elements
    this->SiftOutDeleted(output_head);

    //VLOG(89) << "list_debug pos7 " << output_head;

    return output_head;
}

template <typename T>
DoubleListNode<T>* TrivialLockDoubleList<T>::CutHead(std::function<bool(const T &one)> criteria) noexcept {

    /*Note: In the current design , there can be only one thread invoking this method.But it
        is allowed to have several other threads doing Insert at the mean time.  */
    std::unique_lock<std::recursive_mutex> _mutex_lock(this->m_recursive_mutex);

    //VLOG(89) << "debug double list enter";

    auto _p_cur  = this->m_head->m_atomic_next.load();
    if (_p_cur == this->m_tail)
        return nullptr;

    auto _p_pre      = this->m_head;
    auto _p_start    = _p_cur;
    auto output_head = _p_cur;

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

        /*This is the tricky part,need to consider simultaneously Inserting and CutHeading : If we set _p_cur->next
           to nullptr successfully, no other threads could insert new node between _p_pre and _p_cur.This is the critical
           safety guarantee for other operations.  */
        auto _p_tmp = _p_cur;
        if (!_p_pre->m_atomic_next.compare_exchange_strong(_p_tmp, nullptr)) {
            /*Strong CAS fail ,means that other thread(s) already made _p_cur to point to
            the newly inserted node.What we need to do is just redo the iterating from current node.
            Also _p_cur need to be updated to the newly inserted node,otherwise the criteria will
            be evaluated to false forever.  */
            _p_cur = _p_pre;
            continue;
        }

        //Cutting done,just break out.
        break;
    }

    /*Note: Once goes here, _p_pre  is the latest item of the cut out list and
                            _p_cur is the first  item of the remaining list.  */
    //Detach first node
    auto _p_tmp = _p_start;
    while (!this->m_head->m_atomic_next.compare_exchange_strong(_p_tmp, _p_cur)) {
        decltype(_p_cur) _p_tmp = nullptr;
        CHECK(_p_pre->m_atomic_next.compare_exchange_strong(_p_tmp, _p_cur));
        VLOG(89) << "-------recursive cuthead occur!-----";
        return this->CutHead(criteria);
    }

    _p_tmp = _p_pre;
    while (!_p_cur->m_atomic_pre.compare_exchange_strong(_p_tmp, this->m_head)) {
        LOG(WARNING) << "cutting head tail->pre CAS_fail " << _p_cur << " previous change from "
                << _p_pre << " to " << _p_tmp << ",head:" << this->m_head << ",tail:" << this->m_tail
                << ",do CAS again.";
        _p_tmp = _p_pre;
        continue;
    }

    output_head->m_atomic_pre.store(nullptr);

    //Waiting for all threads that are iterating in the cut off list to be finished.
    this->WaitForListClean(_p_pre);

    //Now the list is cut off with the deleted elements. Need to erase the deleted elements
    this->SiftOutDeleted(output_head);

    //VLOG(89) << "debug double list leave";

    return output_head;
}

template <typename T>
DoubleListNode<T>* TrivialLockDoubleList<T>::CutHeadByValue(const T &val) noexcept {
    auto judge_smaller_equal = [&](const T &one) -> bool{
        return one <= val;
    };

    return this->CutHead(judge_smaller_equal);
}

template <typename T>
void TrivialLockDoubleList<T>::ReleaseCutHead(DoubleListNode<T>* output_head) noexcept {
    auto _p_cur  = output_head;
    while (_p_cur != nullptr) {
        auto _p_next = _p_cur->m_atomic_next.load();
        delete _p_cur;
        _p_cur  = _p_next;
    }
}

template <typename T>
void TrivialLockDoubleList<T>::IterateCutHead(std::function<bool(T &)> accessor, DoubleListNode<T>* output_head) const noexcept {
    auto _p_cur  = output_head;
    while (_p_cur != nullptr) {
        auto _p_next = _p_cur->m_atomic_next.load();
        accessor(*_p_cur->m_val);
        _p_cur  = _p_next;
    }
}

template <typename T>
void TrivialLockDoubleList<T>::Iterate(std::function<bool(T &)> accessor) const noexcept {

    auto _cur = this->m_head->m_atomic_next.load();
    while (_cur) {

        if (_cur == this->m_tail)
            break;

        auto _p_tmp = _cur;
        _cur = _cur->m_atomic_next.load();

        if (_p_tmp->IsDeleted())
            continue;

        if (!accessor(*_p_tmp->m_val))
            break;
    }
}

template <typename T>
bool TrivialLockDoubleList<T>::Empty() const noexcept {
    return this->m_head->m_atomic_next.load() == this->m_tail;
}

#ifdef _TRIIAL_DOUBLE_LIST_TEST_
template <typename T>
int TrivialLockDoubleList<T>::GetSize() const noexcept {

    int _size = 0;
    auto _cur = this->m_head;
    while (_cur) {
        if (!_cur->IsDeleted())
            _size++;
        _cur = _cur->m_atomic_next.load();
    }

    //Exclude head & tail.
    return _size - 2;
}

template <typename T>
DoubleListNode<T>* TrivialLockDoubleList<T>::GetHead() const noexcept {
    return this->m_head;
}

#endif

}
