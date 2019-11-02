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

#include <string>

#include "grpc/grpc.h"
#include "grpc++/server_context.h"
#include "grpc++/security/server_credentials.h"

#include "common/request_base.h"
#include "binlog/binlog_singleton.h"
#include "storage/storage_singleton.h"
#include "service/service.h"
#include "topology/topology_mgr.h"
#include "leader/leader_view.h"
#include "follower/follower_view.h"
#include "candidate/candidate_view.h"
#include "guid/guid_generator.h"
#include "election/election.h"
#include "tools/timer.h"
#include "tools/lock_free_priority_queue.h"
#include "global/global_env.h"

namespace RaftCore::Global {

std::unique_ptr<::grpc::Server>   GlobalEnv::m_pserver;

volatile bool  GlobalEnv::m_running = false;

std::vector<ReactWorkGroup<>>    GlobalEnv::m_vec_notify_cq_workgroup;

std::vector<ReactWorkGroup<>>    GlobalEnv::m_vec_call_cq_workgroup;

std::vector<ReactWorkGroup<CompletionQueue>>    GlobalEnv::m_vec_client_cq_workgroup;

std::shared_ptr<::raft::RaftService::AsyncService>   GlobalEnv::m_async_service;

volatile bool  GlobalEnv::m_cq_fully_shutdown = false;

std::string   GlobalEnv::m_server_addr;

::grpc::ServerBuilder    GlobalEnv::m_builder;


bool GlobalEnv::IsRunning() noexcept {
    return m_running;
}

TypePtrCQ<CompletionQueue> GlobalEnv::GetClientCQInstance(uint32_t idx) noexcept {
    return m_vec_client_cq_workgroup[idx].GetCQ();
}

void GlobalEnv::InitGrpcEnv() noexcept {
    m_server_addr = std::string(::RaftCore::Config::FLAGS_ip) + ":" + std::to_string(::RaftCore::Config::FLAGS_port);
    m_builder.AddListeningPort(m_server_addr, ::grpc::InsecureServerCredentials());

    m_async_service.reset(new ::raft::RaftService::AsyncService());
    m_builder.RegisterService(m_async_service.get());

    m_vec_notify_cq_workgroup.clear();
    m_vec_call_cq_workgroup.clear();

    TypeReactorFunc  _reactor = ::RaftCore::Common::ReactBase::GeneralReacting;

    uint32_t _notify_cq_thread_num = ::RaftCore::Config::FLAGS_notify_cq_threads;
    uint32_t _notify_cq_num = ::RaftCore::Config::FLAGS_notify_cq_num;

    for (std::size_t i = 0; i < _notify_cq_num; ++i)
        m_vec_notify_cq_workgroup.emplace_back(m_builder.AddCompletionQueue(), _reactor, _notify_cq_thread_num);

    for (std::size_t i = 0; i < ::RaftCore::Config::FLAGS_call_cq_num; ++i) {
        uint32_t _call_cq_thread_num = ::RaftCore::Config::FLAGS_call_cq_threads;
        m_vec_call_cq_workgroup.emplace_back(m_builder.AddCompletionQueue(), _reactor, _call_cq_thread_num);
    }

    //Each CQ can only get one thread to achieve maximum entrusting speed.
    uint32_t _client_cq_num = _notify_cq_thread_num * _notify_cq_num;

    //The additional CQ is for the response processing dedicated CQ.
    if (::RaftCore::State::StateMgr::GetRole() == State::RaftRole::LEADER) {
        m_vec_client_cq_workgroup.clear();
        for (std::size_t i = 0; i < _client_cq_num; ++i) {

            uint32_t _client_cq_thread_num = ::RaftCore::Config::FLAGS_client_thread_num;
            TypeReactorFunc _client_reactor = ::RaftCore::Leader::LeaderView::ClientThreadReacting;

            TypePtrCQ<CompletionQueue>  _shp_cq(new CompletionQueue());
            m_vec_client_cq_workgroup.emplace_back(_shp_cq, _client_reactor, _client_cq_thread_num);
        }
    }
}

void GlobalEnv::StartGrpcService() noexcept{

    m_pserver = m_builder.BuildAndStart();

    //Spawning request pool instances must be after server successfully built.
    for (std::size_t i = 0; i < ::RaftCore::Config::FLAGS_notify_cq_num; ++i) {
        for (std::size_t j = 0; j < ::RaftCore::Config::FLAGS_request_pool_size; ++j)
            SpawnFamilyBucket(m_async_service, i);
    }

    LOG(INFO) << "Server listening on " << m_server_addr;

    auto _start_workgroup_threads = [&](auto &work_group) {
        for (auto &_item : work_group)
            _item.StartPolling();
    };

    LOG(INFO) << "spawning notify_cq polling threads.";
    _start_workgroup_threads(m_vec_notify_cq_workgroup);

    //After starting notify threads, we need to update the mapping.
    ::RaftCore::Leader::LeaderView::UpdateThreadMapping();

    LOG(INFO) << "spawning call_cq polling threads.";
    _start_workgroup_threads(m_vec_call_cq_workgroup);

    LOG(INFO) << "spawning client_cq polling threads.";
    _start_workgroup_threads(m_vec_client_cq_workgroup);

    m_running = true;

    auto _wait_workgroup_threads = [&](auto &work_group) {
        for (auto &_item : work_group)
            _item.WaitPolling();
    };

    LOG(INFO) << "waiting notify_cq polling threads to exist";
    _wait_workgroup_threads(m_vec_notify_cq_workgroup);

    LOG(INFO) << "waiting call_cq polling threads to exist";
    _wait_workgroup_threads(m_vec_call_cq_workgroup);

    LOG(INFO) << "waiting client_cq polling threads to exist";
    _wait_workgroup_threads(m_vec_client_cq_workgroup);

    m_cq_fully_shutdown = true;

    VLOG(89) << "fully shutdown set to true";
}

void GlobalEnv::SpawnFamilyBucket(std::shared_ptr<::raft::RaftService::AsyncService> shp_svc, std::size_t cq_idx) noexcept {

    auto _notify_cq = m_vec_notify_cq_workgroup[cq_idx].GetCQ();

    std::size_t _call_cq_idx = cq_idx % m_vec_call_cq_workgroup.size();
    auto _call_cq = m_vec_call_cq_workgroup[_call_cq_idx].GetCQ();

    //Entrusting a complete set of request instances to each CQ.
    new ::RaftCore::Service::Write(shp_svc, _notify_cq, _call_cq);
    new ::RaftCore::Service::Read(shp_svc, _notify_cq, _call_cq);
    new ::RaftCore::Service::MembershipChange(shp_svc, _notify_cq, _call_cq);
    new ::RaftCore::Service::AppendEntries(shp_svc, _notify_cq, _call_cq);
    new ::RaftCore::Service::CommitEntries(shp_svc, _notify_cq, _call_cq);
    new ::RaftCore::Service::SyncData(shp_svc, _notify_cq, _call_cq);
    new ::RaftCore::Service::MemberChangePrepare(shp_svc, _notify_cq, _call_cq);
    new ::RaftCore::Service::MemberChangeCommit(shp_svc, _notify_cq, _call_cq);
    new ::RaftCore::Service::PreVote(shp_svc, _notify_cq, _call_cq);
    new ::RaftCore::Service::Vote(shp_svc, _notify_cq, _call_cq);
    new ::RaftCore::Service::HeartBeat(shp_svc, _notify_cq, _call_cq);
}

void GlobalEnv::StopGrpcService() noexcept {

    m_pserver->Shutdown();
    m_pserver->Wait();

    auto _shutdown_workgroup_cqs = [&](auto &work_group) {
        for (auto &_item : work_group)
            _item.ShutDownCQ();
    };

    LOG(INFO) << "shutting down notify_cq.";
    _shutdown_workgroup_cqs(m_vec_notify_cq_workgroup);

    LOG(INFO) << "shutting down call_cq.";
    _shutdown_workgroup_cqs(m_vec_call_cq_workgroup);

    LOG(INFO) << "shutting down client_cq.";
    _shutdown_workgroup_cqs(m_vec_client_cq_workgroup);

    //Need to wait for all CQs shutdown.
    while (!m_cq_fully_shutdown);

    m_running = false;
}

void GlobalEnv::InitialEnv(bool switching_role) noexcept {

    LOG(INFO) << "start initialing global env.";

    //Check config validity first.
    CHECK(::RaftCore::Config::FLAGS_follower_check_heartbeat_interval_ms < ::RaftCore::Config::FLAGS_leader_heartbeat_interval_ms);
    CHECK(::RaftCore::Config::FLAGS_leader_heartbeat_interval_ms < ::RaftCore::Config::FLAGS_election_heartbeat_timeo_ms);
    CHECK(::RaftCore::Config::FLAGS_memory_table_max_item < ::RaftCore::Config::FLAGS_binlog_max_log_num);
    CHECK(::RaftCore::Config::FLAGS_garbage_deque_retain_num >= 1);
    CHECK(::RaftCore::Config::FLAGS_notify_cq_num * ::RaftCore::Config::FLAGS_notify_cq_threads <= ::RaftCore::Config::FLAGS_client_pool_size);

    //TODO: check #threads doesn't exceeds m_step_len

    //#-------------------------------Init topology-------------------------------#//
    ::RaftCore::CTopologyMgr::Initialize();
    ::RaftCore::Topology    global_topo;
    ::RaftCore::CTopologyMgr::Read(&global_topo);

    //#-------------------------------Init State Manager-------------------------------#//
    ::RaftCore::State::StateMgr::Initialize(global_topo);

    /*
    if (::RaftCore::State::StateMgr::AddressUndetermined()) {
        const auto &_nic_addrs = ::RaftCore::State::StateMgr::GetNICAddrs();
        auto _address = ::RaftCore::Member::MemberMgr::FindPossibleAddress(_nic_addrs);
        CHECK(!_address.empty()) << "can't find my address in both topology and membership config files.";
        ::RaftCore::State::StateMgr::SetMyAddr(_address);
    }*/

    //#-------------------------------Init Global Timer-------------------------------#//
    ::RaftCore::Timer::GlobalTimer::Initialize();

    const char* _p_role = ::RaftCore::State::StateMgr::GetRoleStr();
    LOG(INFO) << "--------------------started as " << _p_role << "--------------------";

    //#-------------------------------Init Storage-------------------------------#//
    ::RaftCore::Storage::StorageGlobal::m_instance.Initialize(_p_role);

    /*BinLogGlobal must be initialized after StorageGlobal to avoid opening the binlog file for
      multiple times.  */

    //#-------------------------------Init Binlog Operator-------------------------------#//
    ::RaftCore::BinLog::BinLogGlobal::m_instance.Initialize(_p_role);

    //#-------------------------------Init Guid File-------------------------------#//
    auto _lrl = ::RaftCore::BinLog::BinLogGlobal::m_instance.GetLastReplicated();
    ::RaftCore::Guid::GuidGenerator::Initialize(_lrl.m_index);

    //#-------------------------------Init grpc env-------------------------------#//
    auto _current_role = ::RaftCore::State::StateMgr::GetRole();
    if (!switching_role)
        InitGrpcEnv();

    //#-------------------------------Init leader-------------------------------#//
    if (_current_role == ::RaftCore::State::RaftRole::LEADER) {
        //LeaderView initialization must be after grpc env initial.
        ::RaftCore::Leader::LeaderView::Initialize(global_topo);
    }

    //#-------------------------------Init Follower-------------------------------#//
    if (_current_role == ::RaftCore::State::RaftRole::FOLLOWER)
        ::RaftCore::Follower::FollowerView::Initialize(switching_role);

    //#-------------------------------Init Candidate-------------------------------#//
    if (_current_role == ::RaftCore::State::RaftRole::CANDIDATE)
        ::RaftCore::Candidate::CandidateView::Initialize();

    //#-------------------------------Init Election Manager.-------------------------------#//
    ::RaftCore::Election::ElectionMgr::Initialize();

    //#-------------------------------Init Membership Manager.-------------------------------#//
    ::RaftCore::Member::MemberMgr::Initialize();

    LOG(INFO) << "finish initialing global env.";
}

void GlobalEnv::UnInitialEnv(::RaftCore::State::RaftRole state) noexcept {

    auto _current_role = ::RaftCore::State::StateMgr::GetRole();
    bool _from_old_state = (state != ::RaftCore::State::RaftRole::UNKNOWN);
    if (_from_old_state)
        _current_role = state;

    //#-------------------------------UnInit Global Timer-------------------------------#//
    /*Note : This should firstly be done before state manager ,which is the dependee.*/
    ::RaftCore::Timer::GlobalTimer::UnInitialize();

    //#----------------------------UnInit Server State.-----------------------------#//
    /*Note : This should firstly be done to prevent server from serving newly coming requests.*/
    ::RaftCore::State::StateMgr::UnInitialize();

    ::RaftCore::CTopologyMgr::UnInitialize();

    //#-------------------------------UnInit guid file-------------------------------#//
    ::RaftCore::Guid::GuidGenerator::UnInitialize();

    //#-------------------------------UnInit binlog operator-------------------------------#//
    ::RaftCore::BinLog::BinLogGlobal::m_instance.UnInitialize();

    //#-------------------------------UnInit Storage-------------------------------#//
    ::RaftCore::Storage::StorageGlobal::m_instance.UnInitialize();

    //#-------------------------------UnInit leader-------------------------------#//
    if (_current_role == ::RaftCore::State::RaftRole::LEADER)
        ::RaftCore::Leader::LeaderView::UnInitialize();

    //#-------------------------------UnInit Follower-------------------------------#//
    if (_current_role == ::RaftCore::State::RaftRole::FOLLOWER)
        ::RaftCore::Follower::FollowerView::UnInitialize();

    //#-------------------------------UnInit Candidate-------------------------------#//
    if (_current_role == ::RaftCore::State::RaftRole::CANDIDATE)
        ::RaftCore::Candidate::CandidateView::UnInitialize();

    //#-------------------------------UnInit Election Manager.-------------------------------#//
    ::RaftCore::Election::ElectionMgr::UnInitialize();

    //#-------------------------------UnInit Membership Manager.-------------------------------#//
    ::RaftCore::Member::MemberMgr::UnInitialize();
}

void GlobalEnv::RunServer() noexcept{
    //#-------------------------------Start server-------------------------------#//
    StartGrpcService();
}

void GlobalEnv::StopServer() noexcept {
    StopGrpcService();
}

void GlobalEnv::ShutDown() noexcept {
    StopServer();
    UnInitialEnv();
}

}

