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

#ifndef __GTEST_BASE_H__
#define __GTEST_BASE_H__

#ifndef _RAFT_UNIT_TEST_
#error You must define the '_RAFT_UNIT_TEST_' macro to start unit testing ,otherwise some functions wont running correctly.
#endif

#include "grpc++/channel.h"
#include "grpc++/create_channel.h"

#include "boost/process.hpp"
#include "boost/process/environment.hpp"
#include "boost/filesystem.hpp"

#include "gtest/gtest.h"

#include "config/config.h"
#include "global/global_env.h"
#include "topology/topology_mgr.h"
#include "tools/utilities.h"
#include "guid/guid_generator.h"
#include "election/election.h"
#include "member/member_manager.h"

#define _RAFT_UNIT_TEST_LEADER_PORT_ (10010)
#define _RAFT_UNIT_TEST_FOLLWER_PORT_ (10020)
#define _RAFT_UNIT_TEST_NEW_NODES_PORT_ (10030)

#define _TEST_LEADER_BINLOG_   "raft.binlog.leader"
#define _TEST_FOLLOWER_BINLOG_ "raft.binlog.follower"
#define _TEST_TOPOLOGY_MEMCHG_FILE_ "topology.config.memchg"

#define _TEST_FOLLOWER_NUM_ (3)

namespace fs = boost::filesystem;
namespace bp = boost::process;

class TestBase : public ::testing::Test {

    public:

        TestBase() {
            this->m_leader_addr = this->m_local_ip + ":" + std::to_string(_RAFT_UNIT_TEST_LEADER_PORT_);
        }

        virtual ~TestBase() {}

    protected:

        virtual void SetUp() override {}

        virtual void TearDown() override {}

        auto StartTimeing() {
            return ::RaftCore::Tools::StartTimeing();
        }

        void EndTiming(const std::chrono::time_point<std::chrono::steady_clock> &tp_start,const char* operation_name) {
            ::RaftCore::Tools::EndTiming(tp_start,operation_name);
        }

        void LaunchMultipleThread(std::function<void(int thread_idx)> fn,int working_thread=0) {

            auto _tp = this->StartTimeing();

            std::vector<std::thread*> _vec;

            int _thread_num = this->m_cpu_cores;
            if (working_thread > 0)
                _thread_num = working_thread;

            for (int i = 0; i < _thread_num;++i) {
                std::thread* _p_thread = new std::thread(fn,i);
                _vec.push_back(_p_thread);
            }

            for (auto &_p_thread : _vec)
                _p_thread->join();

            this->EndTiming(_tp, "total thread");
        }

    protected:

        const int m_cpu_cores = std::thread::hardware_concurrency();

        const std::string   m_local_ip = _AURORA_LOCAL_IP_;

        std::string         m_leader_addr = "";

        const int           m_follower_port = _RAFT_UNIT_TEST_FOLLWER_PORT_;
};

class TestSingleBackendFollower : public TestBase {

   public:

        TestSingleBackendFollower() {

            this->m_follower_svc_addr = this->m_local_ip + ":" + std::to_string(this->m_follower_port);

            //Force the instance to listen on the specific port.
            ::RaftCore::Config::FLAGS_port = _RAFT_UNIT_TEST_FOLLWER_PORT_;

            //Init is a time consuming operation.
            ::RaftCore::Global::GlobalEnv::InitialEnv();

            m_thread = new std::thread([]() {
                ::RaftCore::Global::GlobalEnv::RunServer();
            });

            //Waiting for server to get fully start.
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }

        virtual ~TestSingleBackendFollower() {
            ::RaftCore::Global::GlobalEnv::StopServer();
            ::RaftCore::Global::GlobalEnv::UnInitialEnv();
            m_thread->join();
        }

    protected:

        std::string   m_follower_svc_addr ;

        std::thread * m_thread = nullptr;
};

class TestMultipleBackendFollower : public TestBase {

    public:

        TestMultipleBackendFollower() {}

        virtual ~TestMultipleBackendFollower() {}

    protected:

        virtual void StartFollowers(int empty_follower_num=0) final {
            auto _generator = [](int idx) ->const char*{ return _TEST_LEADER_BINLOG_; };
            this->StartFollowersFunc(_generator,empty_follower_num);
        }

        virtual void StartFollowersFunc(std::function<const char*(int)> generator,
            int empty_follower_num=0,int follower_num = _TEST_FOLLOWER_NUM_) final{

            //-----------------------Unifying config of leader and followers.-----------------------//
            ::RaftCore::CTopologyMgr::Initialize();
            ::RaftCore::Topology _topo;
            ::RaftCore::CTopologyMgr::Read(&_topo);

            _topo.m_leader = this->m_local_ip + ":" + std::to_string(::RaftCore::Config::FLAGS_port);

            int port_base = _RAFT_UNIT_TEST_FOLLWER_PORT_;

            _topo.m_followers.clear();
            for (int i = 0; i < follower_num; ++i) {
                int _port = port_base + i;
                _topo.m_followers.emplace(this->m_local_ip + ":" + std::to_string(_port));
            }
            ::RaftCore::CTopologyMgr::Update(_topo);
            ::RaftCore::CTopologyMgr::UnInitialize();

            this->m_main_path = fs::current_path();

            //-----------------------Starting server.-----------------------//
            std::unordered_map<const char*, const char*> _copies;

            _copies.emplace(_AURORA_ELECTION_CONFIG_FILE_,_AURORA_ELECTION_CONFIG_FILE_);
            _copies.emplace(_AURORA_MEMBER_CONFIG_FILE_,_AURORA_MEMBER_CONFIG_FILE_);
            _copies.emplace(_AURORA_TOPOLOGY_CONFFIG_FILE_, _AURORA_TOPOLOGY_CONFFIG_FILE_);

            for (int i = 0; i < follower_num; ++i) {
                int _port = port_base + i;
                auto src = generator(i);
                _copies.emplace(src, _TEST_FOLLOWER_BINLOG_);
                this->StartOneFollower(i,_port,_copies);
                _copies.erase(src);
            }

            //Empty followers use different topology config file.
            _copies.erase(_AURORA_TOPOLOGY_CONFFIG_FILE_);
            _copies.emplace(_TEST_TOPOLOGY_MEMCHG_FILE_,_AURORA_TOPOLOGY_CONFFIG_FILE_);

            //Start empty followers for member change testing.
            int _empty_follower_base = (follower_num / 10) * 10 + 10;
            for (int i = 0; i < empty_follower_num; ++i) {
                int _port = _RAFT_UNIT_TEST_NEW_NODES_PORT_ + i;
                int _follower_dir_idx = _empty_follower_base + i;
                this->StartOneFollower(_follower_dir_idx,_port,_copies);
            }

            //Working directory already changed,go back to the initial path.
            fs::current_path(this->m_main_path);

            //Waiting for the followers to get fully started.
            std::this_thread::sleep_for(std::chrono::seconds(3));
        }

        virtual void EndFollowers() final{
            for (auto& p_child : this->m_p_children)
                p_child->terminate();
        }

    protected:

        virtual void StartOneFollower(int follower_idx,int follower_port,
                const std::unordered_map<const char*,const char*> &copies) final{

            //Set up followers.

            char sz_path[256] = {0};
            std::snprintf(sz_path,sizeof(sz_path),"follower_%d",follower_idx);

            fs::current_path(this->m_main_path);

            //Initial followers' working directory.
            fs::path _dst_dir = fs::path(sz_path);
            fs::remove_all(_dst_dir);

            //This reassignment is trying to solve the 'access denied' problem.
            //_dst_dir = fs::path(sz_path);
            fs::create_directory(_dst_dir);

            //Copy server dependent files.
            for (auto &_pair_kv : copies)
                fs::copy_file(fs::path(_pair_kv.first),_dst_dir/_pair_kv.second);

            //Starting follower instances.
            fs::current_path(this->m_main_path / fs::path(sz_path));

#define _PARA_BUF_SIZE_ (64)
            char* _p_port = (char*)malloc(_PARA_BUF_SIZE_);
            std::snprintf(_p_port,_PARA_BUF_SIZE_,"--port=%d", follower_port);

            char* _p_a = (char*)malloc(_PARA_BUF_SIZE_);
            std::snprintf(_p_a,_PARA_BUF_SIZE_,"--iterating_wait_timeo_us=%d",::RaftCore::Config::FLAGS_iterating_wait_timeo_us);

            char* _p_b = (char*)malloc(_PARA_BUF_SIZE_);
            std::snprintf(_p_b,_PARA_BUF_SIZE_,"--election_heartbeat_timeo_ms=%d",::RaftCore::Config::FLAGS_election_heartbeat_timeo_ms);

            char* _p_c = (char*)malloc(_PARA_BUF_SIZE_);
            std::snprintf(_p_c,_PARA_BUF_SIZE_,"--checking_heartbeat=%d",::RaftCore::Config::FLAGS_checking_heartbeat);

            auto _env = boost::this_process::environment();
            _env["GLOG_v"] = std::to_string(::RaftCore::Config::FLAGS_child_glog_v).c_str();

#define _CMD_BUF_SIZE_ (2048)

#ifdef _WIN32
            const char *_p_path = "C:\\Users\\95\\Documents\\Visual Studio 2015\\Projects\\apollo\\%s\\raft_svr.exe";
            char*_p_exe_file = (char*)malloc(_CMD_BUF_SIZE_);
    #ifdef _DEBUG
            std::snprintf(_p_exe_file,_CMD_BUF_SIZE_,_p_path,"Debug");
    #else
            std::snprintf(_p_exe_file,_CMD_BUF_SIZE_,_p_path,"Release");
    #endif
#elif __APPLE__
            const char *_p_exe_file = "/Users/arthur/git/aurora/bin/debug/aurora";
#elif __linux__
            const char *_p_exe_file = "/root/git/aurora/bin/debug/aurora";
#else
            CHECK(false) << "unknown platform";
#endif

            bp::child *_p_child = new bp::child(_p_exe_file, _env,_p_port,_p_a,_p_b,_p_c);
            this->m_p_children.emplace_back(_p_child);
            std::cout << "started child process:" << _p_child->id() << std::endl;
        }

        std::vector<bp::child*>   m_p_children;

        fs::path                  m_main_path;
};

class TestCluster : public TestMultipleBackendFollower {

public:

    TestCluster(int empty_followers=0) {

        this->StartFollowers(empty_followers);

        ::RaftCore::Config::FLAGS_port = _RAFT_UNIT_TEST_LEADER_PORT_;

        //Init is a time consuming operation.
        ::RaftCore::Global::GlobalEnv::InitialEnv();

        std::thread *_thread = new std::thread([]() {
            ::RaftCore::Global::GlobalEnv::RunServer();
        });

        _thread->detach();

        //Waiting for server to get fully start.
        std::this_thread::sleep_for(std::chrono::seconds(3));
    }

    virtual ~TestCluster() {

        //Serve could be shutdown if old leader no long exist in the new cluster.
        if (::RaftCore::Global::GlobalEnv::IsRunning()) {
            ::RaftCore::Global::GlobalEnv::StopServer();
            ::RaftCore::Global::GlobalEnv::UnInitialEnv();
        }

        this->EndFollowers();
    }

    virtual void ClientWrite(const std::string &key="client_key_test",const std::string &val="client_val_test") {

        std::shared_ptr<::grpc::Channel>   _channel = grpc::CreateChannel(this->m_leader_addr, grpc::InsecureChannelCredentials());
        std::unique_ptr<::raft::RaftService::Stub>  _stub = ::raft::RaftService::NewStub(_channel);

        //std::chrono::system_clock::time_point _deadline = std::chrono::system_clock::now() + std::chrono::seconds(1);
        //_context.set_deadline(_deadline);

        ::raft::ClientWriteRequest  _w_req;
        ::raft::ClientWriteResponse _w_rsp;

        auto *_p_wop = _w_req.mutable_req();
        _p_wop->set_key(key);
        _p_wop->set_value(val);

        ::grpc::ClientContext    _contextX;
        ::grpc::Status _status = _stub->Write(&_contextX, _w_req, &_w_rsp);
        ASSERT_TRUE(_status.ok());
        ASSERT_TRUE(_w_rsp.client_comm_rsp().result()==::raft::ErrorCode::SUCCESS) << "ClientWrite fail,detail:" << _w_rsp.DebugString();
    }

};

#endif
