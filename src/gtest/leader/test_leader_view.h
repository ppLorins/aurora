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

#ifndef __GTEST_LEADER_VIEW_H__
#define __GTEST_LEADER_VIEW_H__

#include <list>
#include <memory>
#include <chrono>

#include "gtest/test_base.h"
#include "election/election.h"
#include "storage/storage_singleton.h"
#include "leader/memory_log_leader.h"

using ::RaftCore::Leader::LeaderView;
using ::RaftCore::Leader::BackGroundTask::LogReplicationContext;
using ::RaftCore::Leader::BackGroundTask::ReSyncLogContext;
using ::RaftCore::Leader::BackGroundTask::SyncDataContenxt;
using ::RaftCore::Leader::TypePtrFollowerEntity;
using ::RaftCore::Leader::MemoryLogItemLeader;
using ::RaftCore::Leader::FollowerStatus;
using ::RaftCore::Election::ElectionMgr;
using ::RaftCore::Storage::StorageGlobal;

namespace fs = boost::filesystem;

class TestLeaderView : public TestMultipleBackendFollower {

    public:

        TestLeaderView() {}

    protected:

        virtual void SetUp() override {
            this->StartFollowers();

            //-----------------------Initializing server.-----------------------//
            ::RaftCore::Global::GlobalEnv::InitialEnv();
        }

        virtual void TearDown() override {
            std::cout << "destructor of TestMultipleBackendFollower called" << std::endl;

            ::RaftCore::Global::GlobalEnv::UnInitialEnv();

            this->EndFollowers();
        }
};

TEST_F(TestLeaderView, GeneralOperation) {

    auto _lrl = BinLogGlobal::m_instance.GetLastReplicated();

    LogIdentifier _lcl;
    _lcl.Set(0,_lrl.m_index - 10);

    StorageGlobal::m_instance.Set(_lcl, "lcl_key", "lcl_val");

    LogIdentifier _start_point;
    _start_point.Set(ElectionMgr::m_cur_term.load(), _lcl.m_index + 1);

    std::string _follower_addr = this->m_local_ip + ":" + std::to_string(this->m_follower_port);
    TypePtrFollowerEntity _shp_follower(new FollowerEntity(_follower_addr,FollowerStatus::RESYNC_LOG));

    std::shared_ptr<ReSyncLogContext> _shp_task(new ReSyncLogContext());
    _shp_task->m_last_sync_point.Set(_start_point);
    _shp_task->m_follower = _shp_follower;
    LeaderView::ReSyncLogCB(_shp_task);

    std::shared_ptr<SyncDataContenxt>  _shp_sync_data_ctx(new SyncDataContenxt(_shp_follower));
    LeaderView::SyncDataCB(_shp_sync_data_ctx);
}

#endif
