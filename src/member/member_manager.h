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

#ifndef __AURORA_MEMBER_MANAGER_H__
#define __AURORA_MEMBER_MANAGER_H__

#include <unordered_map>
#include <shared_mutex>
#include <set>

#include "protocol/raft.pb.h"

#include "common/comm_defs.h"
#include "leader/follower_entity.h"
#include "leader/leader_bg_task.h"

#define _AURORA_MEMBER_CONFIG_FILE_ "membership-change.config"

namespace RaftCore::Member {

using ::raft::MemberChangeInnerRequest;
using ::RaftCore::Leader::TypePtrFollowerEntity;
using ::RaftCore::Leader::BackGroundTask::TwoPhaseCommitContext;
using ::RaftCore::Common::PhaseID;
using ::RaftCore::Common::TwoPhaseCommitBatchTask;

class MemberMgr final {

public:

    struct JointTopology {

        const JointTopology& operator=(const JointTopology &one);

        const JointTopology& operator=(JointTopology &&one);

        void Reset()noexcept;

        void Update(const std::set<std::string> * p_new_cluster=nullptr)noexcept;

        std::set<std::string>     m_new_cluster;

        //Treat all new nodes all followers first.
        std::unordered_map<std::string,TypePtrFollowerEntity>    m_added_nodes;

        std::set<std::string>     m_removed_nodes;

        bool   m_leader_gone_away;

        std::string     m_old_leader;
    };

    struct JointSummary {

        void Reset()noexcept;

        EJointStatus     m_joint_status;

        JointTopology    m_joint_topology;

        //Increased monotonously,never go back.
        uint32_t    m_version;
    };

    class MemberChangeContext final : public TwoPhaseCommitContext{};

public:

    static void Initialize() noexcept;

    static void UnInitialize() noexcept;

    static void ResetMemchgEnv() noexcept;

    static const char* PullTrigger(const std::set<std::string> &new_cluster)noexcept;

    static void SwitchToJointConsensus(JointTopology &updated_topo,uint32_t version=_MAX_UINT32_)noexcept;

    static bool SwitchToStable()noexcept;

    static std::string FindPossibleAddress(const std::list<std::string> &nic_addrs)noexcept;

    static uint32_t GetVersion()noexcept;

    static void MemberChangePrepareCallBack(const ::grpc::Status &status,
        const ::raft::MemberChangeInnerResponse& rsp, void* ptr_follower, uint32_t idx)noexcept;

    static void MemberChangeCommitCallBack(const ::grpc::Status &status,
        const ::raft::MemberChangeInnerResponse& rsp, void* ptr_follower, uint32_t idx)noexcept;

    static void Statistic(const ::grpc::Status &status,
        const ::raft::MemberChangeInnerResponse& rsp, void* ptr_follower, uint32_t joint_flag,
        PhaseID phase_id) noexcept;

#ifdef _MEMBER_MANAGEMENT_TEST_
    static void PendingExecution()noexcept;

    static void ContinueExecution()noexcept;
#endif

    typedef std::shared_ptr<MemberChangeInnerRequest> TypePtrMemberChangReq;

public:

    /*Not persisted.If leader crashes, the new elected leader takes the responsibilities to continue.  */
    static JointSummary      m_joint_summary;

    static std::shared_timed_mutex    m_mutex;

    //No multi threads accessing, no lock for this.
    static MemberChangeContext   m_memchg_ctx;

private:

#ifdef _MEMBER_MANAGEMENT_TEST_
    static bool    m_execution_flag;
#endif

    static void PollingCQ(std::shared_ptr<::grpc::CompletionQueue> shp_cq,int entrust_num)noexcept;

    static void LoadFile() noexcept;

    static void SaveFile() noexcept;

    //The callback function used to notify a node are fully synced event.
    static void NotifyOnSynced(TypePtrFollowerEntity &shp_follower) noexcept;

    //The main logic of membership-change after all nodes synced.
    static void Routine() noexcept;

    inline static const char* MacroToString(EJointStatus enum_val) noexcept;

    inline static EJointStatus StringToMacro(const char* src) noexcept;

    static bool PropagateMemberChange(TypePtrMemberChangReq& shp_req, PhaseID phase_id) noexcept;

    static void WaitForSyncDataDone() noexcept;

private:

    //A temporary variable used for further switching.
    static JointTopology        m_joint_topo_snapshot;

    //CV used for notifying resync-data completion in membership change phase.
    static std::condition_variable             m_resync_data_cv;

    static std::mutex                          m_resync_data_cv_mutex;

    static std::atomic<bool>                   m_in_processing;

    static TwoPhaseCommitBatchTask<TypePtrFollowerEntity>       m_phaseI_task;

    static TwoPhaseCommitBatchTask<TypePtrFollowerEntity>       m_phaseII_task;

    static const char*  m_macro_names[];

private:

    MemberMgr() = delete;

    virtual ~MemberMgr() noexcept = delete;

    MemberMgr(const MemberMgr&) = delete;

    MemberMgr& operator=(const MemberMgr&) = delete;

};

} //end namespace

#endif
