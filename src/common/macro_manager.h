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

#ifndef _AURORA_MACRO_MGR_H_
#define _AURORA_MACRO_MGR_H_

#ifdef _RAFT_UNIT_TEST_

#define _DEQUE_TEST_
#define _SVC_WRITE_TEST_
#define _SVC_APPEND_ENTRIES_TEST_
#define _COMMON_VIEW_TEST_
#define _LEADER_VIEW_TEST_
#define _FOLLOWER_VIEW_TEST_
#define _MEMBER_MANAGEMENT_TEST_
#define _UNORDERED_SINGLE_LIST_TEST_
#define _SINGLE_LIST_TEST_
#define _ELECTION_TEST_
#define _TRIIAL_DOUBLE_LIST_TEST_
#define _STORAGE_TEST_
#define _GLOBAL_TEST_

#endif


#endif
