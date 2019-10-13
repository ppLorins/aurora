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

#include "tools/data_structure_base.h"

namespace RaftCore::DataStructure {

template <typename T>
OrderedTypeBase<T>::OrderedTypeBase() noexcept{}

template <typename T>
OrderedTypeBase<T>::~OrderedTypeBase() noexcept{}

template <typename T>
bool OrderedTypeBase<T>::operator!=(const T&_other)const noexcept {
    return !this->operator==(_other);
}

template <typename T>
bool OrderedTypeBase<T>::operator<=(const T& _other)const noexcept {
    if (this->operator==(_other))
        return true;

    return this->operator<(_other);
}

template <typename T>
bool OrderedTypeBase<T>::operator>=(const T& _other)const noexcept {
    if (this->operator==(_other))
        return true;

    return this->operator>(_other);
}

template <typename T>
LogicalDelete<T>::LogicalDelete() noexcept {}

template <typename T>
LogicalDelete<T>::~LogicalDelete() noexcept {}

template <typename T>
bool LogicalDelete<T>::IsDeleted() const noexcept {
    return this->m_deleted;
}

template <typename T>
void LogicalDelete<T>::SetDeleted() noexcept {
    this->m_deleted = true;
}

template <typename T>
LockableNode<T>::LockableNode()noexcept {
    this->m_spin_lock.clear();
}

template <typename T>
LockableNode<T>::~LockableNode()noexcept {}

template <typename T>
void LockableNode<T>::SpinLock()noexcept {
    while (this->m_spin_lock.test_and_set());
}

template <typename T>
void LockableNode<T>::SpinUnLock()noexcept {
    this->m_spin_lock.clear();
}


} //end namespace

