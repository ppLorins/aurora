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

#ifndef __AURORA_HASHABLE_STRING_H__
#define __AURORA_HASHABLE_STRING_H__

#include <string>

#include "common/comm_defs.h"
#include "tools/lock_free_hash.h"

namespace RaftCore::Storage {

using ::RaftCore::DataStructure::HashTypeBase;

class HashableString final: public HashTypeBase<HashableString> {

public:

    //Constructing a object for temporary usage like querying in hash.
    HashableString(const std::string &other,bool on_fly=false) noexcept;

    virtual ~HashableString() noexcept;

    virtual bool operator<(const HashableString &other)const noexcept override;

    virtual bool operator==(const HashableString &other)const noexcept override;

    virtual bool operator==(const std::string &other)const noexcept ;

    virtual const HashableString& operator=(const HashableString &other)noexcept override;

    virtual std::size_t Hash() const noexcept override;

    virtual const std::string& GetStr() const noexcept ;

    virtual const std::shared_ptr<std::string> GetStrPtr() const noexcept ;

private:

    std::shared_ptr<std::string>    m_shp_str;

};

typedef std::shared_ptr<HashableString>  TypePtrHashableString;

struct PtrHSHasher {
    std::size_t operator()(const TypePtrHashableString &shp_hashable_string)const;
};

struct PtrHSEqualer {
    bool operator()(const TypePtrHashableString &left, const TypePtrHashableString &right)const;
};

} //end namespace

#endif
