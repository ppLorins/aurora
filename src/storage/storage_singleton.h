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

#ifndef __AURORA_STORAGE_SINGLETON_H__
#define __AURORA_STORAGE_SINGLETON_H__

#include "storage/storage.h"

namespace RaftCore::Storage {

using ::RaftCore::Storage::StorageMgr;

class StorageGlobal final{

public:

    static StorageMgr      m_instance;

private:

    StorageGlobal() = delete;

    virtual ~StorageGlobal() = delete;

    StorageGlobal(const StorageGlobal&) = delete;

    StorageGlobal& operator=(const StorageGlobal&) = delete;

};

} //end namespace


#endif
