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
#pragma warning( disable : 4290 )

#ifndef _AURORA_COMM_DEFS_H_
#define _AURORA_COMM_DEFS_H_

#include <ostream>
#include <shared_mutex>
#include <list>
#include <vector>
#include <tuple>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "protocol/raft.pb.h"

#include "common/macro_manager.h"

namespace RaftCore::Common {

#define _TWO_BYTES_ (2)
#define _FOUR_BYTES_ (4)
#define _EIGHT_BYTES_ (8)
#define _MAX_UINT16_ (0xFFFF)
#define _MAX_UINT32_ (0xFFFFFFFF)
#define _MAX_INT32_  (0x7FFFFFFF)
#define _MAX_UINT64_ (0xFFFFFFFFFFFFFFFF)

#define _RING_BUF_EMPTY_POS_   (-1)
#define _RING_BUF_INVALID_POS_ (-2)

#define _ROLE_STR_LEADER_       "leader"
#define _ROLE_STR_FOLLOWER_     "follower"
#define _ROLE_STR_CANDIDATE_    "candidate"
#define _ROLE_STR_UNKNOWN_      "unknown"
#define _ROLE_STR_TEST_         "test"

#define _AURORA_LOCAL_IP_ "127.0.0.1"

#ifdef _SVC_WRITE_TEST_
#define _WRITE_VAL_TS_ "write_val_ts_"
#endif

typedef std::shared_lock<std::shared_timed_mutex>    SharedLock;
typedef std::unique_lock<std::shared_timed_mutex>    UniqueLock;

typedef  SharedLock ReadLock;
typedef  UniqueLock WriteLock;

typedef std::list<std::shared_ptr<::raft::Entity>>   TypeEntityList;
typedef std::tuple<unsigned char*,int>               TypeBufferInfo;

enum class FinishStatus { NEGATIVE_FINISHED = 0, UNFINISHED, POSITIVE_FINISHED };

enum class PhaseID { PhaseI = 0, PhaseII };

enum class VoteType { PreVote = 0, Vote };

bool EntityIDSmaller(const ::raft::EntityID &left, const ::raft::EntityID &right);

bool EntityIDEqual(const ::raft::EntityID &left, const ::raft::EntityID &right);

bool EntityIDSmallerEqual(const ::raft::EntityID &left, const ::raft::EntityID &right);

template<typename T>
struct TwoPhaseCommitBatchTask {
    std::vector<T>    m_todo;
    std::vector<uint32_t>    m_flags;
};

}

/*Note:Additional definitions in other namespace of this project.These definitions may not be suitable to
  be located in their original namespace since otherwise will cause header files recursively including issues.*/
namespace RaftCore::Member {

    enum class EJointStatus { STABLE=0,  JOINT_CONSENSUS };

    enum class JointConsensusMask {
        IN_OLD_CLUSTER = 1,
        IN_NEW_CLUSTER = 2,
    };

}

#endif
