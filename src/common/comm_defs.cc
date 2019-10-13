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

#include  "common/comm_defs.h"

namespace RaftCore::Common {

bool EntityIDEqual(const ::raft::EntityID &left, const ::raft::EntityID &right) {
    return (left.term() == right.term() && left.idx() == right.idx());
}

bool EntityIDSmaller(const ::raft::EntityID &left, const ::raft::EntityID &right) {
    if (left.term() < right.term())
        return true;

    if (left.term() == right.term())
        return left.idx() < right.idx();

    return false;
}

bool EntityIDSmallerEqual(const ::raft::EntityID &left, const ::raft::EntityID &right) {
    if (left.term() < right.term())
        return true;

    if (left.term() == right.term())
        return left.idx() <= right.idx();

    return false;
}

}
