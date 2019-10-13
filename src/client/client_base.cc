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

#include "client/client_base.h"

namespace RaftCore::Client {

ClientBase::ClientBase() {}

ClientBase::~ClientBase() {}

void ClientBase::PushCallBackArgs(void* cb_data) noexcept{
    this->m_callback_args.push_back(cb_data);
}

void ClientBase::ClearCallBackArgs() noexcept{
    this->m_callback_args.clear();
}

}
