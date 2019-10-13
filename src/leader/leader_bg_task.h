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

#ifndef __AURORA_LEADER_BG_TASK_H__
#define __AURORA_LEADER_BG_TASK_H__

#include <memory>

#include "grpc/grpc.h"
#include "grpc++/grpc++.h"

#include "protocol/raft.pb.h"
#include "protocol/raft.grpc.pb.h"

#include "common/log_identifier.h"
#include "tools/trivial_lock_double_list.h"
#include "client/client_impl.h"
#include "leader/follower_entity.h"

namespace RaftCore {
    namespace Service {
        class Write;
    }
}

namespace RaftCore::Leader::BackGroundTask {

using grpc::CompletionQueue;
using ::RaftCore::Common::LogIdentifier;
using ::RaftCore::Common::ReactInfo;
using ::RaftCore::Common::FinishStatus;
using ::RaftCore::Tools::TypeSysTimePoint;
using ::RaftCore::DataStructure::OrderedTypeBase;
using ::RaftCore::Client::SyncDataSyncClient;
using ::RaftCore::Leader::TypePtrFollowerEntity;
using ::RaftCore::Service::Write;

class TwoPhaseCommitContext {

    public:

        struct PhaseState {

            struct RpcStatistic {

                RpcStatistic();

                virtual ~RpcStatistic();

                std::atomic<int>    m_cq_entrust_num ;
                std::atomic<int>    m_succeed_num;
                std::atomic<int>    m_implicitly_fail_num;
                std::atomic<int>    m_explicitly_fail_num;

                int EventsGot()const noexcept;

                std::string Dump() const noexcept;

                void Reset() noexcept;
            };

            std::string Dump() const noexcept;

            PhaseState();

            virtual ~PhaseState();

            void Reset() noexcept;

            void Increase(uint32_t flag, std::atomic<int> &cur_cluster_data,
                          std::atomic<int> &new_cluster_data) noexcept;

            void IncreaseEntrust(uint32_t flag) noexcept;

            void IncreaseSuccess(uint32_t flag) noexcept;

            void IncreaseImplicitFail(uint32_t flag) noexcept;

            void IncreaseExplicitFail(uint32_t flag) noexcept;

            FinishStatus JudgeClusterDetermined(RpcStatistic &cluster_stat, std::size_t majority) noexcept;

            bool JudgeClusterPotentiallySucceed(RpcStatistic &cluster_stat, std::size_t majority) noexcept;

            bool JudgeFinished() noexcept;

            RpcStatistic    m_cur_cluster;
            RpcStatistic    m_new_cluster;

            std::set<TypePtrFollowerEntity>    m_conn_todo_set;
        };

    public:

        TwoPhaseCommitContext();

        virtual ~TwoPhaseCommitContext();

        FinishStatus JudgePhaseIDetermined() noexcept;

        bool JudgePhaseIPotentiallySucceed() noexcept;

        bool JudgeAllFinished() noexcept;

        std::string Dump() const noexcept;

        void Reset() noexcept;

        PhaseState      m_phaseI_state;
        PhaseState      m_phaseII_state;

        std::size_t m_cluster_size      = 0;
        std::size_t m_cluster_majority  = 0;

        std::size_t m_new_cluster_size      = 0;
        std::size_t m_new_cluster_majority  = 0;
};

/*Contains all information needed for a single client RPC context */
class LogReplicationContext final : public TwoPhaseCommitContext {

    public:

        LogReplicationContext()noexcept;

        virtual ~LogReplicationContext()noexcept;

        LogIdentifier   m_cur_log_id;

        /*Have to use a pointer getting around of header files recursively including .
          Can't use the struct forward declaration here, shit.  */
        void*   m_p_joint_snapshot; //A snapshot for consistent reading.
};

class ReSyncLogContext final {

    public:

        LogIdentifier    m_last_sync_point;

        TypePtrFollowerEntity    m_follower;

        std::function<void(TypePtrFollowerEntity&)>     m_on_success_cb;

        bool   m_hold_pre_lcl = false;
};

class SyncDataContenxt final {

    public:

        SyncDataContenxt(TypePtrFollowerEntity &shp_follower) noexcept;

        virtual ~SyncDataContenxt() noexcept;

        bool IsBeginning() const noexcept;

    public:

        LogIdentifier                                   m_last_sync;

        TypePtrFollowerEntity                           m_follower;

        std::shared_ptr<SyncDataSyncClient>             m_shp_client;

        std::function<void(TypePtrFollowerEntity&)>     m_on_success_cb;

        ::grpc::Status     m_final_status;
};

class ClientReactContext final {

    public:

        ReactInfo   m_react_info;
};

class CutEmptyContext final : public OrderedTypeBase<CutEmptyContext> {

    public:

        CutEmptyContext(int value_flag = 0)noexcept;

        virtual ~CutEmptyContext()noexcept;

        virtual bool operator<(const CutEmptyContext& other)const noexcept override;

        virtual bool operator>(const CutEmptyContext& other)const noexcept override;

        virtual bool operator==(const CutEmptyContext& other)const noexcept override;

        std::shared_ptr<Write>     m_write_request;

        TypeSysTimePoint    m_generation_tp;

        /*<0: minimal value;
          >0:max value;
          ==0:comparable value.  */
        int   m_value_flag = 0;

        std::atomic<bool>   m_processed_flag;

        bool m_log_flag = false;
};

} //end namespace

#endif
