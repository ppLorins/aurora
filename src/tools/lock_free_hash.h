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

#ifndef __AURORA_LOCK_FREE_HASH_H__
#define __AURORA_LOCK_FREE_HASH_H__

#include <memory>
#include <list>
#include <map>
#include <type_traits>
#include <functional>

namespace RaftCore::DataStructure {

template <typename T>
class HashTypeBase {

public:

    HashTypeBase() noexcept{}

    virtual bool operator<(const T&)const noexcept  = 0;

    virtual bool operator==(const T&)const noexcept = 0;

    virtual const T& operator=(const T&)noexcept = 0;

    virtual std::size_t Hash() const noexcept = 0;

    virtual bool IsDeleted() const noexcept final;

    virtual void SetDeleted()  const noexcept final;

    virtual void SetValid()  const noexcept final;

private:

    mutable bool m_deleted = false;
};

template <typename T,typename R=void>
class HashNode {

public:

    HashNode(const std::shared_ptr<T> &key,const std::shared_ptr<R> &val) noexcept;

    virtual ~HashNode() noexcept;

    //virtual void Update(const std::shared_ptr<R> &val) noexcept;

    HashNode* GetNext() const noexcept;

    void SetNext(const HashNode<T,R> * const p_next) noexcept;

    std::shared_ptr<T> GetKey() const noexcept;

    std::size_t GetKeyHash() const noexcept;

    virtual std::shared_ptr<R> GetVal() const noexcept;

    void ModifyKey(std::function<void(std::shared_ptr<T>&)> op) noexcept;

    bool IsDeleted() const noexcept;

    void SetDeleted()  const noexcept;

    void SetValid()  const noexcept;

    void SetTag(uint32_t tag)  noexcept;

    uint32_t GetTag()  const noexcept;

    bool operator==(const HashNode<T,R>& one) const noexcept;

    bool operator==(const T& one) const noexcept;

    virtual void LockValue() noexcept;

    virtual void UnLockValue() noexcept;

protected:

    //mutable std::mutex  m_mutex;

    std::shared_ptr<T>    m_shp_key;

    std::shared_ptr<R>    m_shp_val;

    HashNode<T,R>*    m_next = nullptr;

    uint32_t  m_iterating_tag = 0;

private:

    HashNode(const HashNode&) = delete;

    HashNode& operator=(const HashNode&) = delete;
};


template <typename T,typename R=void, template<typename,typename> typename NodeType=HashNode>
class LockFreeHash {

public:

    typedef std::function<bool(const std::shared_ptr<R> &left, const std::shared_ptr<R> &right)> ValueComparator;

    LockFreeHash(uint32_t slot_num=0) noexcept;

    virtual ~LockFreeHash() noexcept;

    void Insert(const std::shared_ptr<T> &key, const std::shared_ptr<R> &val = nullptr, uint32_t tag = 0) noexcept;

    void Delete(const T &key) noexcept;

    bool Find(const T &key) const noexcept;

    /*Note: val pointer's ownership will be taken over. And the return value indicate whether the
      key pointer's ownership has been taken.  */
    //bool Upsert(const T *key, const std::shared_ptr<R> val = nullptr) noexcept;

    bool Read(const T &key, std::shared_ptr<R> &val) const noexcept;

    uint32_t Size() const noexcept;

    /*The GetOrderedBy* are time consuming operations when slots number is large,
        be sure not to invoke it in a real-time processing scenario.*/
    void GetOrderedByKey(std::list<std::shared_ptr<T>> &_output) const noexcept;

    void GetOrderedByValue(std::map<std::shared_ptr<R>,std::shared_ptr<T>,ValueComparator> &_output) const noexcept;

    //Map the operator op to every element in the hash.
    void Map(std::function<void(std::shared_ptr<T>&)> op) noexcept;

    //Read only iterator.
    void Iterate(std::function<bool(const std::shared_ptr<T> &k,const std::shared_ptr<R> &v)> op) noexcept;

    bool CheckCond(std::function<bool(const T &key)> criteria) const noexcept;

    //Clear inserted elements but not the base structure nodes of the current hash.
    void Clear(bool destruct = false) noexcept;

protected:

    uint32_t  m_slots_mask = 0;

    std::atomic<NodeType<T,R>*> **   m_solts = nullptr;

private:

    struct MyComparator {
        bool operator()(const std::shared_ptr<T> &a,const std::shared_ptr<T> &b) const{
            return *a < *b;
        }
    };

    struct MyEqualer {
        bool operator()(const std::shared_ptr<T> &a,const std::shared_ptr<T> &b) const{
            return *a == *b;
        }
    };

    struct MyHasher {
        std::size_t operator()(const std::shared_ptr<T> &a) const{
            return a->Hash();
        }
    };

    uint32_t  m_slots_num  = 0;

    std::atomic<uint32_t>   m_size;

private:

    LockFreeHash(const LockFreeHash&) = delete;

    LockFreeHash& operator=(const LockFreeHash&) = delete;

};

} //end namespace

/*This is for separating template class member function definitions
 from its .h file into a corresponding .cc file:
 https://www.codeproject.com/Articles/48575/How-to-define-a-template-class-in-a-h-file-and-imp.
*/

#include "tools/lock_free_hash.cc"

#endif
