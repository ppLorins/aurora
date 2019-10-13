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

#include <functional>
#include <unordered_set>

#include "common/comm_defs.h"
#include "tools/utilities.h"
#include "config/config.h"
#include "tools/lock_free_hash.h"

namespace RaftCore::DataStructure {

template <typename T>
bool HashTypeBase<T>::IsDeleted() const noexcept {
    return this->m_deleted;
}

template <typename T>
void HashTypeBase<T>::SetDeleted() const noexcept {
    this->m_deleted = true;
}

template <typename T>
void HashTypeBase<T>::SetValid() const noexcept{
    this->m_deleted = false;
}

template <typename T,typename R>
HashNode<T,R>::HashNode(const std::shared_ptr<T> &key,const std::shared_ptr<R> &val) noexcept{
    this->m_shp_key = key;
    this->m_shp_val = val;
    this->m_next = nullptr;
}

/*
template <typename T,typename R>
void HashNode<T, R>::Update(const std::shared_ptr<R> &val) noexcept {
    this->m_shp_val = val;
}*/

template <typename T,typename R>
HashNode<T, R>::~HashNode() noexcept {}

/*
template <typename T,typename R>
void HashNode<T, R>::Update(const std::shared_ptr<R> &val) noexcept {

    //this->m_mutex.lock();
    this->m_shp_val = val;
    //this->m_mutex.unlock();
}*/

template <typename T,typename R>
HashNode<T, R>* HashNode<T, R>::GetNext() const noexcept {
    return this->m_next;
}

template <typename T,typename R>
void HashNode<T,R>::SetNext(const HashNode<T,R> * const p_next) noexcept{
    this->m_next = const_cast<HashNode<T,R>*>(p_next);
}

template <typename T,typename R>
void HashNode<T,R>::ModifyKey(std::function<void(std::shared_ptr<T>&)> op) noexcept {
    op(this->m_shp_key);
}

template <typename T,typename R>
bool HashNode<T,R>::IsDeleted() const noexcept {
    return this->m_shp_key->IsDeleted();
}

template <typename T,typename R>
void HashNode<T,R>::SetDeleted()  const noexcept {
    this->m_shp_key->SetDeleted();
}

template <typename T,typename R>
void HashNode<T, R>::SetValid()  const noexcept {
    this->m_shp_key->SetValid();
}

template <typename T,typename R>
void HashNode<T, R>::SetTag(uint32_t tag) noexcept {
    this->m_iterating_tag = tag;
}

template <typename T,typename R>
uint32_t HashNode<T, R>::GetTag()  const noexcept {
    return this->m_iterating_tag;
}

template <typename T,typename R>
void HashNode<T, R>::LockValue() noexcept {}

template <typename T,typename R>
void HashNode<T, R>::UnLockValue() noexcept {}

template <typename T,typename R>
std::shared_ptr<T> HashNode<T,R>::GetKey() const noexcept {
    return this->m_shp_key;
}

template <typename T,typename R>
std::size_t HashNode<T, R>::GetKeyHash() const noexcept {
    return this->m_shp_key->Hash();
}

template <typename T,typename R>
std::shared_ptr<R> HashNode<T,R>::GetVal() const noexcept {
    return this->m_shp_val;
}

template <typename T,typename R>
bool HashNode<T,R>::operator==(const HashNode<T,R>& one) const noexcept {
    return (*this->m_shp_key) == *one.m_shp_key;
}

template <typename T,typename R>
bool HashNode<T,R>::operator==(const T& one) const noexcept {
    return (*this->m_shp_key) == one;
}

template <typename T,typename R,template<typename,typename> typename NodeType>
LockFreeHash<T, R, NodeType>::LockFreeHash(uint32_t slot_num) noexcept {

    static_assert(std::is_base_of<HashTypeBase<T>,T>::value,"template parameter of LockFreeHash invalid");

    int _one_slot_size = sizeof(void*) + sizeof(NodeType<T, R>) + sizeof(std::atomic<NodeType<T, R>*>)
                         + sizeof(T) + ::RaftCore::Tools::SizeOfX<R>();

    if (slot_num == 0)
        slot_num = ::RaftCore::Tools::RoundUp(::RaftCore::Config::FLAGS_binlog_meta_hash_buf_size * 1024 * 1024 / _one_slot_size);

    this->m_slots_num   = slot_num;
    this->m_slots_mask  = ::RaftCore::Tools::GetMask(this->m_slots_num);

    int buf_size = this->m_slots_num * sizeof(void*);
    this->m_solts = (std::atomic<NodeType<T,R>*> **)std::malloc(buf_size);

    for (std::size_t i = 0; i < this->m_slots_num; ++i)
        this->m_solts[i] = new std::atomic<NodeType<T,R>*>(nullptr);

    this->m_size.store(0);
}

template <typename T,typename R,template<typename,typename> typename NodeType>
LockFreeHash<T, R, NodeType>::~LockFreeHash() noexcept {
    this->Clear(true);
    std::free(this->m_solts);
}

template <typename T,typename R,template<typename,typename> typename NodeType>
void LockFreeHash<T, R, NodeType>::Clear(bool destruct) noexcept {

    assert(this->m_solts != nullptr);

    //int _tmp_1 = 0;

    for (std::size_t i = 0; i < this->m_slots_num; ++i) {
        std::atomic<NodeType<T,R>*> *_p_atomic = this->m_solts[i] ;
        NodeType<T,R>*  _p_cur = _p_atomic->load();

        while (_p_cur != nullptr) {
            auto *_tmp = _p_cur;
            _p_cur = _p_cur->GetNext();
            delete _tmp;
            //_tmp_1++;
        }
        if (destruct)
            delete _p_atomic;
        else
            _p_atomic->store(nullptr);
    }

    //VLOG(89) << "hash released " << _tmp_1 << " inserted elements";

    this->m_size.store(0);
}

template <typename T,typename R,template<typename,typename> typename NodeType>
void LockFreeHash<T, R, NodeType>::Insert(const std::shared_ptr<T> &key, const std::shared_ptr<R> &val, uint32_t tag) noexcept {
    std::size_t hash_val = key->Hash();
    std::size_t idx = hash_val & this->m_slots_mask;

    std::atomic<NodeType<T,R>*> * p_atomic = this->m_solts[idx];
    NodeType<T,R>* p_cur = p_atomic->load();

    bool _key_exist = false;
    while (p_cur != nullptr) {

        if (!p_cur->operator==(*key)) {
            p_cur = p_cur->GetNext(); //move next
            continue;
        }

        _key_exist = true;
        break;
    }

    NodeType<T,R>* p_old_head = p_atomic->load();
    NodeType<T,R>* p_new_node = new NodeType<T,R>(key,val);

    p_new_node->SetNext(p_old_head);
    p_new_node->SetTag(tag);

    /*"When a compare-and-exchange is in a loop, the weak version will yield better performance on some platforms"
     from https://en.cppreference.com/w/cpp/atomic/atomic/compare_exchange.
     Moreover , whether specify the following memory order or not influencing little on overall performance under my test. */
    while (!p_atomic->compare_exchange_weak(p_old_head, p_new_node, std::memory_order_acq_rel, std::memory_order_acquire))
        p_new_node->SetNext(p_old_head);

    /*Set the first existing key to be deleted , ensuring the key can be inserted at head.
      There is a time slice during which iterating could read redundant elements.This is avoided by recording what
      has been read when traversing. */
    if (!_key_exist)
        this->m_size.fetch_add(1);
    else
        p_cur->SetDeleted();
}

template <typename T,typename R,template<typename,typename> typename NodeType>
bool LockFreeHash<T, R, NodeType>::Find(const T &key) const noexcept {

    std::size_t hash_val = key.Hash();
    std::size_t idx = hash_val & this->m_slots_mask;

    std::atomic<NodeType<T,R>*> * p_atomic = this->m_solts[idx];
    NodeType<T,R>* p_cur = p_atomic->load();

    while (p_cur != nullptr) {
        if (p_cur->operator==(key) && !p_cur->IsDeleted())
            return true;

        //move next
        p_cur = p_cur->GetNext();
    }

    return false;
}

/*
template <typename T,typename R,template<typename,typename> typename NodeType>
bool LockFreeHash<T, R, NodeType>::Upsert(const T *key, const std::shared_ptr<R> val) noexcept {
    std::size_t hash_val = key->Hash();
    std::size_t idx = hash_val & this->m_slots_mask;

    std::atomic<NodeType<T,R>*> * p_atomic = this->m_solts[idx];
    NodeType<T,R>* p_cur = p_atomic->load();

    while (p_cur != nullptr) {
        if (p_cur->operator==(*key) && !p_cur->IsDeleted()) {
            p_cur->Update(val);
            return false;
        }

        //move next
        p_cur = p_cur->GetNext();
    }

    std::shared_ptr<T>  _shp_key(const_cast<T*>(key));
    this->Insert(_shp_key, val);

    return true;
}*/

template <typename T,typename R,template<typename,typename> typename NodeType>
uint32_t LockFreeHash<T, R, NodeType>::Size() const noexcept {
    return this->m_size.load();
}

template <typename T,typename R,template<typename,typename> typename NodeType>
bool LockFreeHash<T, R, NodeType>::Read(const T &key, std::shared_ptr<R> &val) const noexcept {

    std::size_t hash_val = key.Hash();
    std::size_t idx = hash_val & this->m_slots_mask;

    std::atomic<NodeType<T,R>*> * p_atomic = this->m_solts[idx];
    NodeType<T,R>* p_cur = p_atomic->load();

    while (p_cur != nullptr) {

        bool  _found = p_cur->operator==(key) && !p_cur->IsDeleted();
        if (!_found) {
            p_cur = p_cur->GetNext();
            continue;
        }

        val = p_cur->GetVal();
        return true;
    }

    return false;
}

template <typename T,typename R,template<typename,typename> typename NodeType>
void LockFreeHash<T, R, NodeType>::Delete(const T &key) noexcept {
    std::size_t hash_val = key.Hash();
    std::size_t idx = hash_val & this->m_slots_mask;

    std::atomic<NodeType<T,R>*> * p_atomic = this->m_solts[idx];
    NodeType<T,R>* p_cur = p_atomic->load();

    while (p_cur != nullptr) {
        if (p_cur->operator==(key) && !p_cur->IsDeleted()) {
            p_cur->SetDeleted();
            this->m_size.fetch_sub(1);
            return ;
        }

        //move next
        p_cur = p_cur->GetNext();
    }
}

template <typename T,typename R,template<typename,typename> typename NodeType>
void LockFreeHash<T, R, NodeType>::GetOrderedByKey(std::list<std::shared_ptr<T>> &_output) const noexcept {

    _output.clear();

    std::set<std::shared_ptr<T>,MyComparator>  _rb_tree;
    for (std::size_t i = 0; i < this->m_slots_num; ++i) {
        std::atomic<NodeType<T,R>*> *p_atomic = this->m_solts[i] ;
        const NodeType<T,R>*  p_cur = p_atomic->load();

        while (p_cur != nullptr) {
            auto _p_tmp = p_cur;
            p_cur = p_cur->GetNext();
            if(_p_tmp->IsDeleted())
                continue;

            //Avoid reading redundant keys.
            if (_rb_tree.find(_p_tmp->GetKey()) == _rb_tree.cend())
                _rb_tree.emplace(_p_tmp->GetKey());
        }
    }

    for (auto iter = _rb_tree.begin(); iter != _rb_tree.end(); iter++)
        _output.emplace_back(*iter);
}

template <typename T,typename R,template<typename,typename> typename NodeType>
void LockFreeHash<T, R, NodeType>::GetOrderedByValue(std::map<std::shared_ptr<R>, std::shared_ptr<T>, ValueComparator> &_output) const noexcept {

    std::unordered_set<std::shared_ptr<T>,MyHasher,MyEqualer> _traversed;

    for (std::size_t i = 0; i < this->m_slots_num; ++i) {
        std::atomic<NodeType<T,R>*> *p_atomic = this->m_solts[i] ;
        const NodeType<T,R>*  p_cur = p_atomic->load();

        while (p_cur != nullptr) {
            auto _p_tmp = p_cur;
            p_cur = p_cur->GetNext();
            if(_p_tmp->IsDeleted())
                continue;

            auto _shp_key = _p_tmp->GetKey();
            if (_traversed.find(_shp_key) != _traversed.cend())
                continue;

            _output.emplace(_p_tmp->GetVal(), _shp_key);
            _traversed.emplace(_shp_key);
        }
    }
}

template <typename T,typename R,template<typename,typename> typename NodeType>
void LockFreeHash<T, R, NodeType>::Map(std::function<void(std::shared_ptr<T>&)> op) noexcept {

    uint32_t _cur_tag = ::RaftCore::Tools::GenerateRandom(1, _MAX_UINT32_);

    for (std::size_t i = 0; i < this->m_slots_num; ++i) {

        std::atomic<NodeType<T, R>*> *p_atomic = this->m_solts[i];
        NodeType<T, R>*  p_cur = p_atomic->load();

        while (p_cur != nullptr) {

            auto _p_tmp = p_cur;
            p_cur = p_cur->GetNext();

            //If already been iterated or deleted,jump over.
            if (_p_tmp->GetTag() == _cur_tag || _p_tmp->IsDeleted())
                continue;

            std::size_t _old_hash_val = _p_tmp->GetKeyHash();

            //Do modifying.
            _p_tmp->ModifyKey(op);

            std::size_t _new_hash_val = _p_tmp->GetKeyHash();
            if (_old_hash_val == _new_hash_val)
                continue;

            //Copy the object out first.
            std::shared_ptr<T>  _shp_new_key = std::make_shared<T>(*_p_tmp->GetKey());

            //Set the original object to be deleted.
            _p_tmp->SetDeleted();

            //Insert the new element with a new tag.
            this->Insert(_shp_new_key,_p_tmp->GetVal(),_cur_tag);
        }
    }
}

template <typename T,typename R,template<typename,typename> typename NodeType>
bool LockFreeHash<T, R, NodeType>::CheckCond(std::function<bool(const T &key)> criteria) const noexcept {

    std::unordered_set<std::shared_ptr<T>,MyHasher,MyEqualer> _traversed;
    for (std::size_t i = 0; i < this->m_slots_num; ++i) {
        std::atomic<NodeType<T,R>*> *p_atomic = this->m_solts[i] ;
        const NodeType<T,R>*  p_cur = p_atomic->load();

        while (p_cur != nullptr) {
            auto _p_tmp = p_cur;
            p_cur = p_cur->GetNext();
            if(_p_tmp->IsDeleted())
                continue;

            auto _shp_key = _p_tmp->GetKey();
            if (_traversed.find(_shp_key) != _traversed.cend())
                continue;

            if (!criteria(*_shp_key))
                return false;
            _traversed.emplace(_shp_key);
        }
    }

    return true;
}

template <typename T,typename R,template<typename,typename> typename NodeType>
void LockFreeHash<T, R, NodeType>::Iterate(std::function<bool(const std::shared_ptr<T> &k, const std::shared_ptr<R> &v)> op) noexcept {

    std::unordered_set<std::shared_ptr<T>,MyHasher,MyEqualer> _traversed;
    for (std::size_t i = 0; i < this->m_slots_num; ++i) {

        std::atomic<NodeType<T, R>*> *p_atomic = this->m_solts[i];
        NodeType<T, R>*  p_cur = p_atomic->load();

        while (p_cur != nullptr) {

            auto *_p_tmp = p_cur;
            p_cur = p_cur->GetNext();

            //If already been iterated or deleted,jump over.
            if (_p_tmp->IsDeleted())
                continue;

            auto _shp_key = _p_tmp->GetKey();
            if (_traversed.find(_shp_key) != _traversed.cend())
                continue;

            _p_tmp->LockValue();
            bool _rst = op(_shp_key, _p_tmp->GetVal());
            _p_tmp->UnLockValue();

            if (!_rst)
                return;

            _traversed.emplace(_shp_key);
        }
    }
}

}
