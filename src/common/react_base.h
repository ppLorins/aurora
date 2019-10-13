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

#ifndef __AURORA_REACT_BASE_H__
#define __AURORA_REACT_BASE_H__

#include <memory>
#include <thread>
#include <functional>

#include "grpc++/completion_queue.h"

namespace RaftCore::Common {

struct ReactInfo {

    ReactInfo()noexcept;

    void Set(bool cq_result, void* tag)noexcept;

    ReactInfo(const ReactInfo &other)noexcept;

    bool    m_cq_result = false;
    void*   m_tag;
};

typedef std::function<void(ReactInfo)>  TypeReactorFunc;

//An empty wrapper for all the subclasses which need to implement 'React' method.
class ReactBase {

public:

    ReactBase()noexcept;

    virtual ~ReactBase()noexcept;

    virtual void React(bool cq_result = true) noexcept = 0;

    static void GeneralReacting(const ReactInfo &info)noexcept;

private:

    ReactBase(const ReactBase&) = delete;

    ReactBase& operator=(const ReactBase&) = delete;
};

} //end namespace

#endif
