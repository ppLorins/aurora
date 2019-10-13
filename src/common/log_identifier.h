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

#ifndef _AURORA_LOG_INDENTIFIER_H_
#define _AURORA_LOG_INDENTIFIER_H_

#include <ostream>

#include "protocol/raft.pb.h"

namespace RaftCore::Common {

/*This struct is used for being the instance of tempalte class std::atomic where we
cannot use ::raft::EntityID directly ,beacuse it is not TRIVIALLY COPYABLE.  */
struct LogIdentifier {

    /* Can't give a user-defined constructor for this struct , otherwise the compiler
        would complain 'the default constructor of "std::atomic<LogIdentifier>" cannot be referenced -- it is a deleted function',
        even though the LogIdentifier struct itself is TRIVIALLY COPABLE . This is a compiler (Microsoft (R) C/C++ Optimizing Compiler Version 19.00.24215.1 for x86)
        issue, ,by contrast, there is no such problem under clang : Apple LLVM version 7.0.2 (clang-700.1.81).
    */

    uint32_t    m_term = 0;   //Election term
    uint64_t    m_index = 0;  //The index under current election term

    //The followings mean to simulate copy-constructor,a work around of the problem described above.
    void Set(const LogIdentifier &_other)noexcept;

    void Set(uint32_t term, uint64_t index)noexcept;

    uint32_t GreaterThan(const LogIdentifier& _other) const noexcept;

    bool operator==(const LogIdentifier& _other) const noexcept;

    bool operator!=(const LogIdentifier& _other) const noexcept;

    bool operator< (const LogIdentifier &_other) const noexcept;

    bool operator<= (const LogIdentifier &_other) const noexcept;

    bool operator> (const LogIdentifier &_other) const noexcept;

    bool operator>= (const LogIdentifier &_other) const noexcept;

    std::string ToString() const noexcept;
};

std::ostream& operator<<(std::ostream& os, const LogIdentifier& obj);

LogIdentifier ConvertID(const ::raft::EntityID &entity_id);

bool EntityIDEqual(const ::raft::EntityID &left, const LogIdentifier &right);

bool EntityIDLarger(const ::raft::EntityID &left, const LogIdentifier &right);

bool EntityIDLargerEqual(const ::raft::EntityID &left, const LogIdentifier &right);

bool EntityIDSmaller(const ::raft::EntityID &left, const LogIdentifier &right);

}

#endif
