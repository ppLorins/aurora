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

#include "common/error_code.h"
#include "tools/utilities.h"
#include "tools/lock_free_queue.h"

namespace RaftCore::DataStructure {

template <typename T>
const char*  QueueNode<T>::m_status_macro_names[] = {"SLOT_EMPTY","SLOT_PRODUCING","SLOT_PRODUCED","SLOT_CONSUMING"};

template <typename T>
QueueNode<T>::QueueNode() noexcept {
    this->m_state.store(SlotState::SLOT_EMPTY);
}

template <typename T>
QueueNode<T>::~QueueNode() noexcept {}

template <typename T>
LockFreeQueue<T>::LockFreeQueue() noexcept{}

template <typename T>
void LockFreeQueue<T>::Initilize(TypeCallBackFunc  fn_cb,int queue_size) noexcept{

    this->m_element_size  = ::RaftCore::Tools::RoundUp(queue_size);
    this->m_element_mask  = ::RaftCore::Tools::GetMask(this->m_element_size);

    this->m_data = new QueueNode<T>[this->m_element_size];

    this->m_fn_cb = fn_cb;

    this->m_head.store(0);
    this->m_tail.store(0);
}

template <typename T>
LockFreeQueue<T>::~LockFreeQueue() noexcept{
    delete []this->m_data;
}

template <typename T>
uint32_t LockFreeQueue<T>::GetSize() const noexcept {
    uint32_t _cur_head = this->m_head.load();
    uint32_t _cur_tail = this->m_tail.load();

    uint32_t _size = 0;
    while (_cur_tail != _cur_head) {
        _cur_tail = (_cur_tail + 1) & this->m_element_mask;
        _size++;
    }

    return _size;
}

template <typename T>
uint32_t LockFreeQueue<T>::GetCapacity() const noexcept {
    return this->m_element_size;
}

template <typename T>
bool LockFreeQueue<T>::Empty() const noexcept {
    return this->m_head.load() == this->m_tail.load();
}

template <typename T>
int LockFreeQueue<T>::PopConsume() noexcept {

    std::shared_ptr<T> _p_item;
    int n_rst = this->Pop(_p_item);
    if (n_rst!=QUEUE_SUCC) {
        if (n_rst!=QUEUE_EMPTY)
            LOG(ERROR) << "Consumer : Pop failed,returned Val:" << n_rst;
        return n_rst;
    }

    if (!this->m_fn_cb(_p_item))
        LOG(ERROR) << "Consumer : Process entry failed";

    return QUEUE_SUCC;
}

template <typename T>
int LockFreeQueue<T>::Pop(std::shared_ptr<T> &ptr_element) noexcept {

    uint32_t _cur_tail = this->m_tail.load();

    //Queue empty
    if (_cur_tail == this->m_head.load())
        return QUEUE_EMPTY;

    uint32_t next = (_cur_tail + 1) & this->m_element_mask;

    //Get the position where to consume.
    /* Note:compare_exchange_weak are allowed to fail spuriously, which is, act as if *this != expected even if they are equal.
       Meaning when compare_exchange_weak return false:
       1> *this != expected and they are actually not equal, no problems.
       2> *this != expected but they are actually equal:
           1) _cur_tail will be replaced with _cur_tail itself, it doesn't change.
           2) next will be re-calculated , it also doesn't change.
       In a word,nothing wrong would happened under spuriously fail.  */
    while (!this->m_tail.compare_exchange_weak(_cur_tail, next)) {
        //Threads can go over the produced range, but it is not an error.
        if (_cur_tail == this->m_head.load()) {
            VLOG(89) << "consuming,found slot is not produced,at position:" << _cur_tail << ",probably due to empty" ;
            return QUEUE_EMPTY;
        }
        next = (_cur_tail + 1) & this->m_element_mask;
    }

    //Only one thread are allowed to operate on the element at index 'next'
    SlotState slot_state = SlotState::SLOT_PRODUCED;
    while (!this->m_data[next].m_state.compare_exchange_weak(slot_state, SlotState::SLOT_CONSUMING)) {

        bool _try_again = slot_state == SlotState::SLOT_PRODUCED || //CAS spurious fail.
            slot_state == SlotState::SLOT_EMPTY ||    //Producer is producing ,try again.
            slot_state == SlotState::SLOT_PRODUCING;   //Producer is producing ,try again.

        if (_try_again) {
            slot_state = SlotState::SLOT_PRODUCED;
            continue;
        }

        CHECK(false) << "cannot update state from produced to consuming at position:" << next << ",detected state:"
                    << QueueNode<T>::MacroToString(slot_state) << ",something is terribly wrong" ;
    }

    //Consuming
    ptr_element = this->m_data[next].m_val;
    this->m_data[next].m_val.reset();  //Release the ownership

    //Update slot state
    slot_state = SlotState::SLOT_CONSUMING;
    CHECK(this->m_data[next].m_state.compare_exchange_strong(slot_state,SlotState::SLOT_EMPTY))  << "cannot update state from consuming to empty at position:"
            << next << ",detected state:" << QueueNode<T>::MacroToString(slot_state) << ",something is terribly wrong" ;

    return QUEUE_SUCC;
}

template <typename T>
int LockFreeQueue<T>::Push(void* ptr_shp_element) noexcept {
    uint32_t _cur_head = this->m_head.load();

    uint32_t next = (_cur_head + 1) & this->m_element_mask;

    //Check the validity of position 'next'
    if (next == this->m_tail.load())
        return QUEUE_FULL;

    //Get the position where to produce.
    /*Note : spuriously fail of compare_exchange_weak is acceptable, since when it happened,_cur_head would remain the same
            as what is is before calling this function, and will go to next round of execution.As explained above.*/
    while (!this->m_head.compare_exchange_weak(_cur_head, next)) {
        next = (_cur_head + 1) & this->m_element_mask;
        if (next == this->m_tail.load()) {
            LOG(WARNING) << "producing,found slot is not empty,at position:" << next << ",probably due to full" ;
            return QUEUE_FULL;
        }
    }

    //Which is guaranteed here is that only one thread will be allowed to operate on the element at index 'next'.
    SlotState slot_state = SlotState::SLOT_EMPTY;
    while(!this->m_data[next].m_state.compare_exchange_weak(slot_state, SlotState::SLOT_PRODUCING)) {

        /*
        slot_state == SlotState::SLOT_EMPTY || //CAS spurious fail.
        slot_state == SlotState::SLOT_PRODUCED ||  //Consumer is consuming this node,try again.
        slot_state == SlotState::SLOT_CONSUMING || //Consumer is consuming this node,try again.
        slot_state == SlotState::SLOT_PRODUCING ;   //An overlapping producing occurred.
        */

        slot_state = SlotState::SLOT_EMPTY;
        continue;
    }

    //Producing
    this->m_data[next].m_val = *((std::shared_ptr<T>*)ptr_shp_element);

    //Update slot state
    slot_state = SlotState::SLOT_PRODUCING;
    CHECK(this->m_data[next].m_state.compare_exchange_strong(slot_state, SlotState::SLOT_PRODUCED))  << "cannot update state from producing to produced at position:"
        << next << ",detected state:" << QueueNode<T>::MacroToString(slot_state) << ",something is terribly wrong" ;

    return QUEUE_SUCC;
}

}
