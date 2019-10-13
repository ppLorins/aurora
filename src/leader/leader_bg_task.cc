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

#include "service/service.h"
#include "member/member_manager.h"
#include "leader/leader_bg_task.h"

namespace RaftCore::Leader::BackGroundTask {

using ::RaftCore::Member::MemberMgr;

TwoPhaseCommitContext::PhaseState::RpcStatistic::RpcStatistic(){ this->Reset(); }

TwoPhaseCommitContext::PhaseState::RpcStatistic::~RpcStatistic(){}

std::string TwoPhaseCommitContext::PhaseState::RpcStatistic::Dump() const noexcept {
    char sz_buf[256] = { 0 };
    std::snprintf(sz_buf,sizeof(sz_buf),"cq_entrust_num:%d,events_got:%d,succeed_num:%d,implicitly_fail_num:%d,explicitly_fail_num:%d",
                this->m_cq_entrust_num.load(),this->EventsGot(),this->m_succeed_num.load(),
                this->m_implicitly_fail_num.load(),this->m_explicitly_fail_num.load());
    return sz_buf;
}

int TwoPhaseCommitContext::PhaseState::RpcStatistic::EventsGot()const noexcept {
    return this->m_succeed_num.load() + this->m_explicitly_fail_num.load() + this->m_implicitly_fail_num.load();
}

void TwoPhaseCommitContext::PhaseState::RpcStatistic::Reset() noexcept {
    this->m_cq_entrust_num.store(0);
    this->m_succeed_num.store(0);
    this->m_implicitly_fail_num.store(0);
    this->m_explicitly_fail_num.store(0);
}

TwoPhaseCommitContext::PhaseState::PhaseState() { this->Reset(); }

TwoPhaseCommitContext::PhaseState::~PhaseState() {}

std::string TwoPhaseCommitContext::PhaseState::Dump() const noexcept{

    auto _cur_cluster_dump = this->m_cur_cluster.Dump();
    auto _new_cluster_dump = this->m_new_cluster.Dump();

    char sz_buf[512] = { 0 };
    std::snprintf(sz_buf,sizeof(sz_buf),"cur_cluster:[%s],new_cluster:[%s]",_cur_cluster_dump.c_str(),_new_cluster_dump.c_str());
    return sz_buf;
}

void TwoPhaseCommitContext::PhaseState::Reset() noexcept {
    this->m_cur_cluster.Reset();
    this->m_new_cluster.Reset();
    this->m_conn_todo_set.clear();
}

void TwoPhaseCommitContext::PhaseState::Increase(uint32_t flag, std::atomic<int> &cur_cluster_data,
    std::atomic<int> &new_cluster_data) noexcept {
    if (flag & int(JointConsensusMask::IN_OLD_CLUSTER))
        cur_cluster_data.fetch_add(1);

    if (flag & int(JointConsensusMask::IN_NEW_CLUSTER))
        new_cluster_data.fetch_add(1);
}

void TwoPhaseCommitContext::PhaseState::IncreaseEntrust(uint32_t flag) noexcept {
    this->Increase(flag,this->m_cur_cluster.m_cq_entrust_num, this->m_new_cluster.m_cq_entrust_num);
}

void TwoPhaseCommitContext::PhaseState::IncreaseSuccess(uint32_t flag) noexcept {
    this->Increase(flag,this->m_cur_cluster.m_succeed_num, this->m_new_cluster.m_succeed_num);
}

void TwoPhaseCommitContext::PhaseState::IncreaseImplicitFail(uint32_t flag) noexcept {
    this->Increase(flag,this->m_cur_cluster.m_implicitly_fail_num, this->m_new_cluster.m_implicitly_fail_num);
}

void TwoPhaseCommitContext::PhaseState::IncreaseExplicitFail(uint32_t flag) noexcept {
    this->Increase(flag,this->m_cur_cluster.m_explicitly_fail_num, this->m_new_cluster.m_explicitly_fail_num);
}

bool TwoPhaseCommitContext::PhaseState::JudgeClusterPotentiallySucceed(RpcStatistic &cluster_stat, std::size_t majority) noexcept {
    return (cluster_stat.m_succeed_num.load() + cluster_stat.m_implicitly_fail_num.load()) >= (int)majority;
}

FinishStatus TwoPhaseCommitContext::PhaseState::JudgeClusterDetermined(RpcStatistic &cluster_stat,std::size_t majority) noexcept {
    if (cluster_stat.m_succeed_num.load() >= (int)majority)
        return FinishStatus::POSITIVE_FINISHED;

    int _unknown = cluster_stat.m_cq_entrust_num - cluster_stat.EventsGot();
    if (cluster_stat.m_succeed_num.load() + _unknown < (int)majority)
        return FinishStatus::NEGATIVE_FINISHED;

    return FinishStatus::UNFINISHED;
}

bool TwoPhaseCommitContext::PhaseState::JudgeFinished() noexcept {
    return (this->m_cur_cluster.m_cq_entrust_num == this->m_cur_cluster.EventsGot() &&
            this->m_new_cluster.m_cq_entrust_num == this->m_new_cluster.EventsGot());
}

bool TwoPhaseCommitContext::JudgePhaseIPotentiallySucceed() noexcept {
    auto &_phaseI  = this->m_phaseI_state;

    if (!_phaseI.JudgeClusterPotentiallySucceed(_phaseI.m_cur_cluster, this->m_cluster_majority))
        return false;

    return _phaseI.JudgeClusterPotentiallySucceed(_phaseI.m_new_cluster, this->m_new_cluster_majority);
}

FinishStatus TwoPhaseCommitContext::JudgePhaseIDetermined() noexcept {
    auto &_phaseI = this->m_phaseI_state;

    FinishStatus _cur = _phaseI.JudgeClusterDetermined(_phaseI.m_cur_cluster, this->m_cluster_majority);
    FinishStatus _new = _phaseI.JudgeClusterDetermined(_phaseI.m_new_cluster, this->m_new_cluster_majority);

    if ((_cur == FinishStatus::NEGATIVE_FINISHED) || (_new == FinishStatus::NEGATIVE_FINISHED))
        return FinishStatus::NEGATIVE_FINISHED;

    if ((_cur == FinishStatus::POSITIVE_FINISHED) && (_new == FinishStatus::POSITIVE_FINISHED))
        return FinishStatus::POSITIVE_FINISHED;

    return FinishStatus::UNFINISHED;
}

TwoPhaseCommitContext::TwoPhaseCommitContext() { this->Reset(); }

TwoPhaseCommitContext::~TwoPhaseCommitContext() {}

bool TwoPhaseCommitContext::JudgeAllFinished() noexcept {

    if (!this->m_phaseI_state.JudgeFinished())
        return false;

    int _phaseII_obligation_x = this->m_phaseI_state.m_cur_cluster.m_cq_entrust_num.load();
    if (this->m_phaseII_state.m_cur_cluster.m_succeed_num.load() < _phaseII_obligation_x)
        return false;

    int _phaseII_obligation_y = this->m_phaseI_state.m_new_cluster.m_cq_entrust_num.load();
    if (this->m_phaseII_state.m_new_cluster.m_succeed_num.load() < _phaseII_obligation_y)
        return false;

    return  true;
}

std::string TwoPhaseCommitContext::Dump() const noexcept{

    auto _phaseI_dump  = this->m_phaseI_state.Dump();
    auto _phaseII_dump = this->m_phaseII_state.Dump();

    char sz_buf[1024] = { 0 };
    std::snprintf(sz_buf,sizeof(sz_buf),"phaseI_state:[%s],phaseII_state:[%s],m_cluster_size:%u,"
                "m_cluster_majority:%u,m_new_cluster_size:%u,m_new_cluster_majority:%u",
                _phaseI_dump.c_str(),_phaseII_dump.c_str(),(uint32_t)this->m_cluster_size,(uint32_t)this->m_cluster_majority,
                (uint32_t)this->m_new_cluster_size,(uint32_t)this->m_new_cluster_majority);
    return sz_buf;
}

void TwoPhaseCommitContext::Reset() noexcept {
    this->m_phaseI_state.Reset();
    this->m_phaseII_state.Reset();
}

SyncDataContenxt::SyncDataContenxt(TypePtrFollowerEntity &shp_follower) noexcept{
    //Sync Data will hold one connection until this job finished,since it is stateful.
    this->m_follower = shp_follower;

    auto _shp_channel = this->m_follower->m_shp_channel_pool->GetOneChannel();
    this->m_shp_client.reset(new BackGroundTask::SyncDataSyncClient(_shp_channel));

    this->m_last_sync.Set(0, 0);
}

SyncDataContenxt::~SyncDataContenxt() noexcept {}

bool SyncDataContenxt::IsBeginning() const noexcept {
    return (this->m_last_sync.m_term == 0 && this->m_last_sync.m_index == 0);
}

LogReplicationContext::LogReplicationContext()noexcept {
    this->m_p_joint_snapshot = new MemberMgr::JointSummary();
}

LogReplicationContext::~LogReplicationContext()noexcept{
    delete (MemberMgr::JointSummary*)this->m_p_joint_snapshot;
}

CutEmptyContext::CutEmptyContext(int value_flag)noexcept{
    this->m_value_flag = value_flag;
    this->m_generation_tp = std::chrono::system_clock::now();
    this->m_processed_flag.store(false);
}

CutEmptyContext::~CutEmptyContext()noexcept{}

bool CutEmptyContext::operator<(const CutEmptyContext& other)const noexcept {

    if (this->m_value_flag < 0 || other.m_value_flag > 0)
        return true;

    if (this->m_value_flag > 0 || other.m_value_flag < 0)
        return false;

    const auto &_shp_req       = this->m_write_request->GetReqCtx();
    const auto &_shp_req_other = other.m_write_request->GetReqCtx();

    return _shp_req->m_cur_log_id < _shp_req_other->m_cur_log_id;
}

bool CutEmptyContext::operator>(const CutEmptyContext& other)const noexcept {

    if (this->m_value_flag < 0 || other.m_value_flag > 0)
        return false;

    if (this->m_value_flag > 0 || other.m_value_flag < 0)
        return true;

    const auto &_shp_req       = this->m_write_request->GetReqCtx();
    const auto &_shp_req_other = other.m_write_request->GetReqCtx();

    return _shp_req->m_cur_log_id > _shp_req_other->m_cur_log_id;
}

bool CutEmptyContext::operator==(const CutEmptyContext& other)const noexcept {

    if (other.m_value_flag != 0 || this->m_value_flag != 0)
        return false;

    const auto &_shp_req       = this->m_write_request->GetReqCtx();
    const auto &_shp_req_other = other.m_write_request->GetReqCtx();

    return _shp_req->m_cur_log_id == _shp_req_other->m_cur_log_id;
}

}

