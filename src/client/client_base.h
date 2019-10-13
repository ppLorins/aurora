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

#ifndef __AURORA_CLIENT_BASE_H__
#define __AURORA_CLIENT_BASE_H__

#include <vector>

namespace RaftCore::Client {

//For the prospective common properties .
class ClientBase {

public:

    ClientBase();

    virtual ~ClientBase();

    void PushCallBackArgs(void* cb_data)noexcept;

protected:

    void ClearCallBackArgs()noexcept;

    std::vector<void*>       m_callback_args;

private:

    ClientBase(const ClientBase&) = delete;

    ClientBase& operator=(const ClientBase&) = delete;
};

}

#endif
