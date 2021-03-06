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

#include "common/comm_view.h"
#include "leader/leader_view.h"
#include "global/global_env.h"
#include "storage/storage.h"
#include "leader/follower_entity.h"

namespace RaftCore::Leader {

using ::RaftCore::Common::CommonView;
using ::RaftCore::Leader::LeaderView;
using ::RaftCore::Global::GlobalEnv;
using ::RaftCore::Storage::StorageMgr;
using ::RaftCore::Service::OwnershipDelegator;

const char* FollowerEntity::m_status_macro_names[] = { "NORMAL","RESYNC_LOG","RESYNC_DATA"};

FollowerEntity::FollowerEntity(const std::string &follower_addr, FollowerStatus status,
    uint32_t joint_consensus_flag, std::shared_ptr<CompletionQueue> input_cq) noexcept {

    this->m_shp_channel_pool.reset(new ChannelPool(follower_addr,::RaftCore::Config::FLAGS_channel_pool_size));
    auto _channel = this->m_shp_channel_pool->GetOneChannel();

    uint32_t _notify_threads_num = ::RaftCore::Config::FLAGS_notify_cq_threads * ::RaftCore::Config::FLAGS_notify_cq_num;
    for (std::size_t n = 0; n < _notify_threads_num; ++n) {
        this->m_append_client_pool[n].reset(new ClientPool<AppendEntriesAsyncClient>(this));
        this->m_commit_client_pool[n].reset(new ClientPool<CommitEntriesAsyncClient>(this));
    }

    uint32_t _pool_size = ::RaftCore::Config::FLAGS_client_pool_size;
    for (std::size_t i = 0; i < _pool_size; ++i) {
        uint32_t _idx = i % _notify_threads_num;
        auto shp_cq = input_cq;
        if (!shp_cq)
            shp_cq = GlobalEnv::GetClientCQInstance(_idx);

        auto* _p_client = new AppendEntriesAsyncClient(_channel, shp_cq);
        auto _shp_client = _p_client->OwnershipDelegator<AppendEntriesAsyncClient>::GetOwnership();

        this->m_append_client_pool[_idx]->Back(_shp_client);
    }

    uint32_t _group_commit = ::RaftCore::Config::FLAGS_group_commit_count;

    uint32_t _commit_client_size = _pool_size / _group_commit;
    CHECK(_commit_client_size > 0) << "pool_size:" << _pool_size << ",group_commit:" << _commit_client_size;

    VLOG(89) << "debug commit client size:" << _commit_client_size << ",addr:" << this->my_addr;

    for (std::size_t i = 0; i < _commit_client_size; ++i) {
        uint32_t _idx = i % _notify_threads_num;
        auto shp_cq = input_cq;
        if (!shp_cq)
            shp_cq = GlobalEnv::GetClientCQInstance(_idx);

        auto* _p_client = new CommitEntriesAsyncClient(_channel, shp_cq);
        auto _shp_client = _p_client->OwnershipDelegator<CommitEntriesAsyncClient>::GetOwnership();
        this->m_commit_client_pool[_idx]->Back(_shp_client);
    }

    this->m_joint_consensus_flag = joint_consensus_flag;
    this->my_addr    = follower_addr;
    this->m_status   = status;

    this->m_last_sent_committed.store(CommonView::m_zero_log_id);
}

FollowerEntity::~FollowerEntity() noexcept{}

bool FollowerEntity::UpdateLastSentCommitted(const LogIdentifier &to) noexcept {

    while (true) {
        auto _cur_last_commit = this->m_last_sent_committed.load();
        if (to <= _cur_last_commit)
            return false;

        if (!this->m_last_sent_committed.compare_exchange_weak(_cur_last_commit, to))
            continue;

        VLOG(89) << "m_last_sent_committed update to:" << to << ", addr:" << this->my_addr;
        break;
    }

    return true;
}

std::shared_ptr<AppendEntriesAsyncClient> FollowerEntity::FetchAppendClient(void* &pool) noexcept {
    auto _tid = std::this_thread::get_id();
    auto &_uptr_pool = this->m_append_client_pool[LeaderView::m_notify_thread_mapping[_tid]];
    pool = _uptr_pool.get();
    return _uptr_pool->Fetch();
}

std::shared_ptr<CommitEntriesAsyncClient> FollowerEntity::FetchCommitClient(void* &pool) noexcept {
    auto _tid = std::this_thread::get_id();
    auto &_uptr_pool = this->m_commit_client_pool[LeaderView::m_notify_thread_mapping[_tid]];
    pool = _uptr_pool.get();
    return _uptr_pool->Fetch();
}

}

