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

#include "tools/lock_free_hash_specific.h"

namespace RaftCore::DataStructure {

template <typename T,typename R>
HashNodeAtomic<T, R>::HashNodeAtomic(const std::shared_ptr<T> &key,
    const std::shared_ptr<R> &val) noexcept : HashNode<T, R>(key, val) {}

template <typename T,typename R>
void HashNodeAtomic<T, R>::Update(R* val)noexcept {
    this->SpinLock();

    //Do not delete the managed ptr.
    this->m_shp_val.reset(val, [](auto *p) {});
    this->SpinUnLock();
}

template <typename T,typename R>
void HashNodeAtomic<T, R>::LockValue() noexcept {
    this->SpinLock();
}

template <typename T,typename R>
void HashNodeAtomic<T, R>::UnLockValue() noexcept {
    this->SpinUnLock();
}

template <typename T,typename R>
HashNodeAtomic<T, R>* HashNodeAtomic<T, R>::GetNext() const noexcept {
    return dynamic_cast<HashNodeAtomic<T, R>*>(this->m_next);
}

template <typename T,typename R>
LockFreeHashAtomic<T, R>::LockFreeHashAtomic(uint32_t slot_num)noexcept : LockFreeHash<T, R, HashNodeAtomic>(slot_num) {
}

template <typename T,typename R>
bool LockFreeHashAtomic<T, R>::Upsert(const T *key, R* p_avl) noexcept {
    std::size_t hash_val = key->Hash();
    std::size_t idx = hash_val & this->m_slots_mask;

    std::atomic<HashNodeAtomic<T, R>*> * p_atomic = this->m_solts[idx];
    HashNodeAtomic<T, R>* p_cur = p_atomic->load();

    while (p_cur != nullptr) {
        if (p_cur->operator==(*key) && !p_cur->IsDeleted()) {

            std::atomic<R*>  _atomic(p_avl);
            p_cur->Update(_atomic);
            return false;
        }

        //move next
        p_cur = p_cur->GetNext();
    }

    std::shared_ptr<T>  _shp_key(const_cast<T*>(key));

    //Here we need to use an empty deleter.
    std::shared_ptr<R>  _shp_val(p_avl, [](auto* p) {});

    this->Insert(_shp_key, _shp_val);

    return true;
}


} //end namespace

