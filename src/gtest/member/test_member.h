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

#ifndef __GTEST_MEMBER_H__
#define __GTEST_MEMBER_H__

#include <list>
#include <memory>
#include <chrono>

#include "gtest/test_base.h"
#include "global/global_env.h"
#include "common/comm_defs.h"
#include "binlog/binlog_singleton.h"
#include "storage/storage_singleton.h"
#include "member/member_manager.h"

using ::RaftCore::Common::ReadLock;
using ::RaftCore::Member::MemberMgr;
using ::RaftCore::BinLog::BinLogGlobal;
using ::RaftCore::Storage::StorageGlobal;
using ::RaftCore::Leader::BackGroundTask::TwoPhaseCommitContext;

class TestMember : public TestCluster {

    public:

        //Start several normal servers and 2 empty servers.
        TestMember() : TestCluster(2) {}

    protected:

        virtual void SetUp() override {}

        virtual void TearDown() override {}

        virtual void SetStoragePoint(int backoff = 10) {
            auto _lrl = BinLogGlobal::m_instance.GetLastReplicated();
            LogIdentifier _lcl;

            _lcl.Set(0, _lrl.m_index - backoff);
            StorageGlobal::m_instance.Set(_lcl,"lcl_key","lcl_val");
        }

        virtual void IssueMemberChangeRequest() {
            std::shared_ptr<::grpc::Channel>   _channel = grpc::CreateChannel(this->m_leader_addr, grpc::InsecureChannelCredentials());
            std::unique_ptr<::raft::RaftService::Stub>  _stub = ::raft::RaftService::NewStub(_channel);

            ::raft::MemberChangeRequest  _memchg_req;
            ::raft::MemberChangeResponse _memchg_rsp;

            std::set<std::string> _new_cluster = this->m_cluster_leader_not_gone;
            if (::RaftCore::Config::FLAGS_member_leader_gone)
                _new_cluster = this->m_cluster_leader_gone;

            for (const auto &_item : _new_cluster) {
                auto *_node = _memchg_req.add_node_list();
                *_node = _item;
            }

            ::grpc::ClientContext    _contextX;
            ::grpc::Status _status = _stub->MembershipChange(&_contextX, _memchg_req, &_memchg_rsp);
            ASSERT_TRUE(_status.ok());
            ASSERT_TRUE(_memchg_rsp.client_comm_rsp().result()==::raft::ErrorCode::SUCCESS) << "ClientWrite fail,detail:" << _memchg_rsp.DebugString();
        }

    protected:

        std::set<std::string>   m_cluster_leader_not_gone{"127.0.0.1:10010","127.0.0.1:10022", // old nodes.
                                                  "127.0.0.1:10030","127.0.0.1:10031"}; // new empty nodes.

        std::set<std::string>   m_cluster_leader_gone{"127.0.0.1:10022", // old nodes.
                                              "127.0.0.1:10030","127.0.0.1:10031"}; // new empty nodes.

};

TEST_F(TestMember, GeneralOperation) {

    std::cout << "member version:" << MemberMgr::GetVersion();

    this->SetStoragePoint();

    std::set<std::string> _new_cluster = this->m_cluster_leader_not_gone;
    if (::RaftCore::Config::FLAGS_member_leader_gone)
        _new_cluster = this->m_cluster_leader_gone;

    MemberMgr::PullTrigger(_new_cluster);
    MemberMgr::ContinueExecution();

    auto _old_version = MemberMgr::GetVersion();

    //Give enough time to wait for the membership change finish.
    while(true) {
        ReadLock _r_lock(MemberMgr::m_mutex);
        auto _new_version = MemberMgr::m_joint_summary.m_version;
        //Note:A two phase membership replication will eventually causing version increased by 2.
        if (_new_version == (_old_version + 2))
            break;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    //Wait routine to finish.
    std::this_thread::sleep_for(std::chrono::seconds(2));

    //Wait for the new cluster to elect out a new leader.
    if (::RaftCore::Config::FLAGS_member_leader_gone)
        std::this_thread::sleep_for(std::chrono::seconds(5));
}

TEST_F(TestMember, WriteData) {

    this->SetStoragePoint();

    //Step 1. start membership changing but just finish phaseI.
    this->IssueMemberChangeRequest();

    //Waiting for sync-data process to finish and leader switched to joint consensus state.
    std::this_thread::sleep_for(std::chrono::seconds(20));

    //Step 2. start writing data through server's public rpc interface.
    this->ClientWrite("memberchg_key","memberchg_val");

    std::this_thread::sleep_for(std::chrono::seconds(3));

    //Step 3. manually check whether the appending log requests are propagated to both C-old and C-new,and judge by it.

    //Step 4. resume execution of the pending routine thread.
    MemberMgr::ContinueExecution();

    //Waiting for continue to complete.
    std::this_thread::sleep_for(std::chrono::seconds(2));

    //Step 5. write again
    this->ClientWrite("memberchg_second_key","memberchg_second_val");

    //Step 6. manually check whether the appending log requests are propagated to only C-new,and judge by it.
}

TEST_F(TestMember, Election) {

    //Use a short binlog.
    this->SetStoragePoint(5);

    //Step 1. start membership changing but just finish phaseI.
    this->IssueMemberChangeRequest();

    //Waiting for sync-data process to finish and leader switched to joint consensus state.
    std::this_thread::sleep_for(std::chrono::seconds(5));

    //Step 2. start election by start and then stop the heartbeat.
    ::RaftCore::Config::FLAGS_do_heartbeat = true;

    LOG(INFO) << "starting heartbeat..." << std::endl;;

    //Waiting for the heartbeat message to be sent out.
    std::this_thread::sleep_for(std::chrono::seconds(3));

    LOG(INFO) << "stopping heartbeat..." << std::endl;;

    //Stop sending heartbeat.
    ::RaftCore::Config::FLAGS_heartbeat_oneshot = true;

    LOG(INFO) << "wait heartbeat timeout..." << std::endl;;

    //Waiting for heartbeat timeout
    std::this_thread::sleep_for(std::chrono::seconds(3));

    //Waiting for election finished.
    std::this_thread::sleep_for(std::chrono::seconds(3));

    //Step 3. manually check whether the election requests are propagated to both C-old and C-new,and judge by it.

}


#endif
