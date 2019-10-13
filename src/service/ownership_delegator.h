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

#ifndef __AURORA_OWNERSHIP_DELEGATOR_H__
#define __AURORA_OWNERSHIP_DELEGATOR_H__

#include <memory>

namespace RaftCore::Service {

template<typename T>
class OwnershipDelegator {

public:

    OwnershipDelegator();

    virtual ~OwnershipDelegator();

    void ResetOwnership(T *src) noexcept;

    void ReleaseOwnership() noexcept;

    std::shared_ptr<T> GetOwnership()noexcept;

    void CopyOwnership(std::shared_ptr<T> from)noexcept;

private:

    std::shared_ptr<T>      *m_p_shp_delegator = nullptr;

private:

    OwnershipDelegator(const OwnershipDelegator&) = delete;

    OwnershipDelegator& operator=(const OwnershipDelegator&) = delete;
};

}

#include "service/ownership_delegator.cc"

#endif
