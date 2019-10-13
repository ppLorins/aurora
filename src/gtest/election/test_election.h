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

#ifndef __GTEST_ELECTION_H__
#define __GTEST_ELECTION_H__

#include <list>
#include <memory>
#include <chrono>

#include "gtest/test_base.h"
#include "global/global_env.h"
#include "election/election.h"

using ::RaftCore::Election::ElectionMgr;
using ::RaftCore::Election::RaftRole;

class TestElection : public TestMultipleBackendFollower {

    public:

        TestElection() {}

    protected:

        virtual void SetUp() override {
            this->PrepareBinlogFiles();

            this->StartFollowersFunc(TestElection::GenerateFileName);

            ::RaftCore::Global::GlobalEnv::InitialEnv();
        }

        virtual void TearDown() override {
            ::RaftCore::Global::GlobalEnv::UnInitialEnv();
            this->EndFollowers();
        }

    private:

        void PrepareBinlogFiles()noexcept {
            int _log_entry_base_num = 5;
            for (int idx = 0; idx < _TEST_FOLLOWER_NUM_;++idx) {
                auto _role = this->GenerateFileRole(idx);
                auto _binlog_file = this->GenerateFileName(idx);

                this->ConstructBinlogFile(_binlog_file,_role , _log_entry_base_num++);
            }

            //Construct current binlog.
            this->ConstructBinlogFile(_TEST_LEADER_BINLOG_,"leader", _log_entry_base_num++);
        }

        void ConstructBinlogFile(const char* binlog_file,const char* role,
                                int entry_num) noexcept {

            if (fs::exists(fs::path(binlog_file)))
                ASSERT_TRUE(std::remove(binlog_file)==0);

            //testing file
            BinLogGlobal::m_instance.Initialize(role);

            //Construct binlog file.
            std::list<std::shared_ptr<::raft::Entity> > _input;
            for (int i = 0; i < entry_num;++i) {
                std::shared_ptr<::raft::Entity> _shp_entity(new ::raft::Entity());

                auto _p_entity_id = _shp_entity->mutable_entity_id();
                _p_entity_id->set_term(0);
                _p_entity_id->set_idx(i);

                auto _p_pre_entity_id = _shp_entity->mutable_pre_log_id();
                _p_pre_entity_id->set_term(0);
                _p_pre_entity_id->set_idx(i==0?0:i-1);

                auto _p_wop = _shp_entity->mutable_write_op();
                _p_wop->set_key("key_" + std::to_string(i));
                _p_wop->set_value("val_" + std::to_string(i));

                _input.emplace_back(_shp_entity);
            }
            ASSERT_TRUE(BinLogGlobal::m_instance.AppendEntry(_input));

            std::list<std::shared_ptr<FileMetaData::IdxPair>> _output;
            BinLogGlobal::m_instance.GetOrderedMeta(_output);

            //A routine test for binlog operator.
            ASSERT_TRUE(BinLogGlobal::m_instance.GetLastReplicated() == LogIdentifier(*_output.back()));
            ASSERT_STREQ(BinLogGlobal::m_instance.GetBinlogFileName().c_str(),binlog_file);

            BinLogGlobal::m_instance.UnInitialize();
        }

        static const char* GenerateFileName(int idx) noexcept {
            auto _role = GenerateFileRole(idx);

            static char _binlog_file[100] = {};
            std::snprintf(_binlog_file,sizeof(_binlog_file),"%s.%s",_AURORA_BINLOG_NAME_,_role);

            return _binlog_file;
        }

        static const char* GenerateFileRole(int idx) noexcept {
            static char _role[100] = {};
            std::snprintf(_role,sizeof(_role),"election-%d",idx);
            return _role;
        }
};

TEST_F(TestElection, GeneralOperation) {

    //ElectionMgr::Initialize();

    /*First  make sure term 3 is not in the 'election.config' config file.Only
        after that can we start the unit test.  */

    ASSERT_TRUE(ElectionMgr::TryVote(3, "12.34.56.78:100")=="");

    ASSERT_TRUE(ElectionMgr::TryVote(3, "12.34.56.78:101")=="12.34.56.78:100");

    ElectionMgr::AddVotingTerm(7, "12.34.56.78:200");

    ElectionMgr::AddVotingTerm(7, "12.34.56.78:201");

    ASSERT_TRUE(ElectionMgr::TryVote(4, "12.34.56.78:200")=="");

    ElectionMgr::SwitchRole(RaftRole::FOLLOWER,"12.34.56.78:300");

    ElectionMgr::SwitchRole(RaftRole::CANDIDATE);

    ElectionMgr::SwitchRole(RaftRole::LEADER);

    ElectionMgr::SwitchRole(RaftRole::FOLLOWER,"12.34.56.78:300");

    ElectionMgr::ElectionThread();

    //Waiting above thread to get fully started.
    std::this_thread::sleep_for(std::chrono::seconds(3));

    ElectionMgr::NotifyNewLeaderEvent(4,"12.34.56.78:300");

    ElectionMgr::WaitElectionThread();

    //ElectionMgr::UnInitialize();

    std::cout << "test election end." << std::endl;
}

TEST_F(TestElection, Election) {

    /*Waiting current leader sending heartbeat msg to followers thus triggering theirs heartbeat
        checking mechanism.  */
    std::this_thread::sleep_for(std::chrono::seconds(2));

    //Start grpc service.
    std::thread* _th = new std::thread([]() {
        ::RaftCore::Global::GlobalEnv::RunServer();
    });
    //Wait for server get fully started
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    ElectionMgr::SwitchRole(RaftRole::FOLLOWER,"12.34.56.78:300");

    //Waiting for detecting the fake leader has gone.
    std::cout << "Waiting for detecting the fake leader has gone..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(3));

    //Waiting for a new leader being elected out.
    std::cout << "Waiting for a new leader being elected out..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(3));

    Topology _topo;
    CTopologyMgr::Read(&_topo);

    std::list<std::string>  _valid_new_leaders{"127.0.0.1:10022","127.0.0.1:10010"};

    ASSERT_TRUE(std::find(_valid_new_leaders.cbegin(), _valid_new_leaders.cend(), _topo.m_leader) != _valid_new_leaders.cend())
            << "new leader invalid: " << _topo.m_leader;

    std::cout << "new leader elected :" << _topo.m_leader << " under term:" << ElectionMgr::m_cur_term.load() << std::endl;

    //Waiting for the new leader finish syncing logs with its followers.
    std::cout << "Waiting for the new leader finish syncing logs with its followers..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));

    ::RaftCore::Global::GlobalEnv::StopServer();

    //Waiting for the above spawned thread exist.
    std::this_thread::sleep_for(std::chrono::seconds(1));
}


#endif
