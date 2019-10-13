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

#ifndef __AURORA_ELECTION_H__
#define __AURORA_ELECTION_H__

#include <memory>
#include <shared_mutex>
#include <map>
#include <set>
#include <thread>

#include "protocol/raft.pb.h"

#include "common/comm_defs.h"
#include "common/log_identifier.h"
#include "tools/utilities.h"
#include "state/state_mgr.h"
#include "member/member_manager.h"
#include "topology/topology_mgr.h"

#define _AURORA_ELECTION_CONFIG_FILE_ "election.config"

namespace RaftCore::Election {

using ::RaftCore::Common::VoteType;
using ::RaftCore::Common::TwoPhaseCommitBatchTask;
using ::RaftCore::Common::LogIdentifier;
using ::RaftCore::State::RaftRole;
using ::RaftCore::Topology;
using ::RaftCore::Member::MemberMgr;
using ::raft::VoteRequest;

class ElectionMgr {

public:

    static void Initialize() noexcept;

    static void UnInitialize() noexcept;

    static std::string TryVote(uint32_t term,const std::string &addr)noexcept;

    static void AddVotingTerm(uint32_t term,const std::string &addr) noexcept;

    static void ElectionThread()noexcept;

    static void NotifyNewLeaderEvent(uint32_t term, const std::string addr)noexcept;

    //SwitchRole is not idempotent.
    static void SwitchRole(RaftRole target_role, const std::string &new_leader = "") noexcept;

    static void CallBack(const ::grpc::Status &status, const ::raft::VoteResponse& rsp, VoteType vote_type,uint32_t idx) noexcept;

#ifdef _ELECTION_TEST_
    static void WaitElectionThread()noexcept;
#endif

public:

    /*To make election process simple & clear & non error prone, avoiding multiple
      thread operations as much as possible.  */
    static std::shared_timed_mutex    m_election_mutex;

    //Persistent state on all servers:
    static std::atomic<uint32_t>    m_cur_term; //current term

    //This is a special variable passing through the server-wide lives.
    static volatile bool   m_leader_debut;

    static LogIdentifier   m_pre_term_lrl;

private:

    static void RenameBinlogNames(RaftRole old_role, RaftRole target_role) noexcept;

    static void CandidateRoutine()noexcept;

    static void LoadFile()noexcept;

    static void SaveFile() noexcept;

    static void Reset() noexcept;

    static bool BroadcastVoting(std::shared_ptr<VoteRequest> shp_req, const Topology &topo,
        VoteType vote_type) noexcept;

    //Return: if a higher term found during the increasing process.
    static bool IncreaseToMaxterm()noexcept;

    static void PollingCQ(std::shared_ptr<::grpc::CompletionQueue> shp_cq,int entrust_num)noexcept;

    static void PreparePrevoteTask(const Topology &topo)noexcept;

    static void SentNonOP(const std::string &tag) noexcept;

private:

    static MemberMgr::JointSummary  m_joint_snapshot;

    static std::map<uint32_t, std::string>     m_voted;

    static std::shared_timed_mutex    m_voted_mutex;

    static std::map<uint32_t, std::set<std::string>>     m_known_voting;

    static std::shared_timed_mutex    m_known_voting_mutex;

    static std::thread     *m_p_thread;

#ifdef _ELECTION_TEST_
    static volatile bool   m_candidate_routine_running;
#endif

    struct NewLeaderEvent {

        std::string   m_new_leader_addr;

        uint32_t    m_new_leader_term;

        //POT type,thread safe.
        volatile bool    m_notify_flag;
    };

    static NewLeaderEvent      m_new_leader_event;

    static uint32_t     m_cur_cluster_size;

    static uint32_t m_cur_cluster_vote_counter;

    static uint32_t m_new_cluster_vote_counter;

    static TwoPhaseCommitBatchTask<std::string>       m_phaseI_task;

    static TwoPhaseCommitBatchTask<std::string>       m_phaseII_task;

private:

    ElectionMgr() = delete;

    virtual ~ElectionMgr() = delete;

    ElectionMgr(const ElectionMgr&) = delete;

    ElectionMgr& operator=(const ElectionMgr&) = delete;

};

} //end namespace

#endif
