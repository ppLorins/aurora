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

#include "config/config.h"
#include "election/election.h"
#include "tools/timer.h"
#include "tools/lock_free_priority_queue.h"
#include "service/service.h"
#include "follower/follower_view.h"

namespace RaftCore::Follower {

using ::RaftCore::Service::AppendEntries;

std::condition_variable FollowerView::m_cv;

std::mutex    FollowerView::m_cv_mutex;

std::chrono::time_point<std::chrono::steady_clock>    FollowerView::m_last_heartbeat;

std::shared_timed_mutex    FollowerView::m_last_heartbeat_lock;

/* Note: each follower has exactly one pending_list , the head and tail nodes were
constant during the lifetime of this pending list, so it's okay to 'new' an object without
caring about when to 'delete' .  */
TrivialLockDoubleList<MemoryLogItemFollower>  FollowerView::m_phaseI_pending_list(std::shared_ptr<MemoryLogItemFollower>(new MemoryLogItemFollower(0x0, 0x0)),
    std::shared_ptr<MemoryLogItemFollower>(new MemoryLogItemFollower(_MAX_UINT32_, _MAX_UINT64_)));

TrivialLockDoubleList<MemoryLogItemFollower>  FollowerView::m_phaseII_pending_list(std::shared_ptr<MemoryLogItemFollower>(new MemoryLogItemFollower(0x0, 0x0)),
    std::shared_ptr<MemoryLogItemFollower>(new MemoryLogItemFollower(_MAX_UINT32_, _MAX_UINT64_)));

TrivialLockSingleList<DisorderMessageContext>   FollowerView::m_disorder_list(std::shared_ptr<DisorderMessageContext>(new DisorderMessageContext(-1)),
    std::shared_ptr<DisorderMessageContext>(new DisorderMessageContext(1)));

LockFreeUnorderedSingleList<DoubleListNode<MemoryLogItemFollower>>     FollowerView::m_garbage;

LockFreeUnorderedSingleList<SingleListNode<DisorderMessageContext>>    FollowerView::m_disorder_garbage;

using ::RaftCore::Common::ReadLock;
using ::RaftCore::Timer::GlobalTimer;
using ::RaftCore::Election::ElectionMgr;

void FollowerView::Initialize(bool switching_role) noexcept{

    CommonView::Initialize();

    //Register GC task to the global timer.
    CommonView::InstallGC<TrivialLockDoubleList,DoubleListNode,MemoryLogItemFollower>(&m_garbage);
    CommonView::InstallGC<TrivialLockSingleList,SingleListNode,DisorderMessageContext>(&m_disorder_garbage);

#ifdef _FOLLOWER_VIEW_TEST_
    auto _test = []()->bool {
        //VLOG(89) << "I'm alive to debug the stuck issue...";
        return true;
    };
    GlobalTimer::AddTask(1000, _test);
#endif

    //Initial to time epoch.
    m_last_heartbeat = std::chrono::time_point<std::chrono::steady_clock>();

    //Avoiding immediately checking heartbeat timeout after a switching role event happened.
    if (switching_role)
        m_last_heartbeat = std::chrono::steady_clock::now();

    decltype(m_last_heartbeat) * _p_last_heartbeat = &m_last_heartbeat;
    auto _check_heartbeat = [_p_last_heartbeat,switching_role]()->bool {

        //Just for unit test.
        if (!::RaftCore::Config::FLAGS_checking_heartbeat)
            return true;

        ReadLock _r_lock(FollowerView::m_last_heartbeat_lock);
        /*In a general startup case(non switching-role) , Only after receiving the 1st heartbeat msg from server,
            could the checking mechanism really getting started.*/
        if (!switching_role && (std::chrono::duration_cast<std::chrono::seconds>(_p_last_heartbeat->time_since_epoch()).count() == 0))
            return true;

        auto _diff = std::chrono::steady_clock::now() - (*_p_last_heartbeat);
        _r_lock.unlock();

        auto _diff_ms = std::chrono::duration_cast<std::chrono::milliseconds>(_diff).count();
        if (_diff_ms <= ::RaftCore::Config::FLAGS_election_heartbeat_timeo_ms)
            return true;

        LOG(INFO) << "leader heartbeat timeout line reached,start electing,diff_ms:" << _diff_ms;

        /*Election's heartbeat timeout happened ,starting to turn into candidate state.It will do this
          by creating a new thread to IMMEDIATELY re-initialize the global env , which will terminate
          the current timer thread the other way round. */
        ElectionMgr::ElectionThread();
        return false;
    };
    GlobalTimer::AddTask(::RaftCore::Config::FLAGS_follower_check_heartbeat_interval_ms,_check_heartbeat);

    CommonView::m_running_flag = true;

    //Start follower routine thread.
    for (std::size_t i = 0; i < ::RaftCore::Config::FLAGS_iterating_threads; ++i)
        CommonView::m_vec_routine.emplace_back(new std::thread(AppendEntries::DisorderLogRoutine));
}

void FollowerView::UnInitialize() noexcept {

    //Waiting for routine thread exit.
    CommonView::m_running_flag = false;

    for (auto* p_thread : CommonView::m_vec_routine) {
        p_thread->join();
        delete p_thread;
    }

    Clear();
    CommonView::UnInitialize();
}

void FollowerView::Clear() noexcept {
    m_phaseI_pending_list.Clear();
    m_phaseII_pending_list.Clear();
    m_disorder_list.Clear();
}

}

