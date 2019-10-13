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

#ifndef __AURORA_DATA_STRUCTURE_COMMON_H__
#define __AURORA_DATA_STRUCTURE_COMMON_H__

namespace RaftCore::DataStructure {

template <typename T>
class OrderedTypeBase {

public:

    OrderedTypeBase() noexcept;

    virtual ~OrderedTypeBase() noexcept;

    //The element of this list should be able to be compared with each other.
    virtual bool operator<(const T&)const noexcept = 0;

    virtual bool operator>(const T&)const noexcept = 0;

    virtual bool operator==(const T&)const noexcept = 0;

    virtual bool operator!=(const T&_other)const noexcept;

    //Should be non-final, providing a way for the subclass to override.
    virtual bool operator<=(const T& _other)const noexcept;

    virtual bool operator>=(const T& _other)const noexcept;

    //virtual std::string PrintMe() const noexcept { return ""; }
};

//Here template is just a padding.
template <typename T=void>
class LogicalDelete {

public:

    LogicalDelete() noexcept;

    virtual ~LogicalDelete() noexcept;

    virtual bool IsDeleted() const noexcept final;

    virtual void SetDeleted() noexcept final;

private:

    bool m_deleted = false;
};

//Here template is just a padding.
template <typename T=void>
class LockableNode {

public:

    LockableNode() noexcept;

    virtual ~LockableNode() noexcept;

protected:

    void SpinLock() noexcept;

    void SpinUnLock() noexcept;

private:

    std::atomic_flag    m_spin_lock;
};


} //end namespace

#include "tools/data_structure_base.cc"

#endif
