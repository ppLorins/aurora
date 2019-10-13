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

#ifndef __AURORA_LOCK_FREE_HASH_SPECIFIC_H__
#define __AURORA_LOCK_FREE_HASH_SPECIFIC_H__

#include <thread>
#include <atomic>

#include "tools/lock_free_hash.h"

namespace RaftCore::DataStructure {

using ::RaftCore::DataStructure::HashNode;
using ::RaftCore::DataStructure::LockFreeHash;
using ::RaftCore::DataStructure::LockableNode;

//Partial specification.
template <typename T,typename R>
class HashNodeAtomic final : public HashNode<T, R>, public LockableNode<void> {

public:

    //using SpecifiedNode = HashNode<T, std::atomic<R*>>;

    HashNodeAtomic(const std::shared_ptr<T> &key,const std::shared_ptr<R> &val) noexcept;

    HashNodeAtomic* GetNext() const noexcept;

    void Update(R* val) noexcept;

    virtual void LockValue() noexcept override;

    virtual void UnLockValue() noexcept override;

private:

    HashNodeAtomic(const HashNodeAtomic&) = delete;

    HashNodeAtomic& operator=(const HashNodeAtomic&) = delete;
};

template <typename T,typename R>
class LockFreeHashAtomic final : public LockFreeHash<T, R, HashNodeAtomic> {

public:

    LockFreeHashAtomic(uint32_t slot_num=0)noexcept;

    bool Upsert(const T *key, R* p_avl) noexcept;

private:

    LockFreeHashAtomic(const LockFreeHashAtomic&) = delete;

    LockFreeHashAtomic& operator=(const LockFreeHashAtomic&) = delete;

};

} //end namespace

#include "tools/lock_free_hash_specific.cc"

#endif
