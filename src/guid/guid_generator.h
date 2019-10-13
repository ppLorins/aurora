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

#ifndef _AURORA_GUID_GENERATOR_H_
#define _AURORA_GUID_GENERATOR_H_

#include <atomic>

#define _GUID_ERR_ (0xFFFFFFFFFFFFFFFF)

namespace RaftCore::Guid {

class GuidGenerator final {

public:

    struct GUIDPair {

        //2^64 is 18446744073709551616,which can satisfy 1M/s requests for 584942 years.
        uint64_t  m_pre_guid;   //Based on which the allocated guid is generated
        uint64_t  m_cur_guid;  //Allocated guid

        bool operator<(const GUIDPair &other) const noexcept {
            return this->m_cur_guid < other.m_cur_guid;
        }
    };

public:

    static void Initialize(uint64_t last_released=0) noexcept;

    static void UnInitialize() noexcept;

    static GUIDPair GenerateGuid() noexcept;

    //Set m_last_released_guid to the given id , for fail recovery purpose.
    static void SetNextBasePoint(uint64_t base_point) noexcept;

    static uint64_t GetLastReleasedGuid() noexcept;

private:

    static std::atomic<uint64_t>        m_last_released_guid;

private:

    GuidGenerator() = delete;

    virtual ~GuidGenerator() = delete;

    GuidGenerator(const GuidGenerator&) = delete;

    GuidGenerator& operator=(const GuidGenerator&) = delete;
};

}


#endif
