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

#include "boost/filesystem.hpp"

#include "config/config.h"
#include "common/comm_defs.h"
#include "guid/guid_generator.h"

#define _AURORA_FILE_BUF_SIZE_ (1024)

namespace RaftCore::Guid {

std::atomic<uint64_t> GuidGenerator::m_last_released_guid;

void GuidGenerator::Initialize(uint64_t last_released) noexcept{
    m_last_released_guid.store(last_released);
}

void GuidGenerator::UnInitialize() noexcept{}

GuidGenerator::GUIDPair GuidGenerator::GenerateGuid() noexcept{

    uint64_t _old_val = m_last_released_guid.fetch_add(1);
    uint64_t _deserved_val = _old_val + 1;

    return { _old_val,_deserved_val };
}

void GuidGenerator::SetNextBasePoint(uint64_t base_point) noexcept {
    m_last_released_guid.store(base_point);
}

uint64_t GuidGenerator::GetLastReleasedGuid() noexcept {
    return m_last_released_guid.load();
}

}

