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

#include <thread>

#include "tools/timer.h"
#include "storage/storage_singleton.h"
#include "common/comm_view.h"

namespace RaftCore::Common {

int   CommonView::m_cpu_cores;

LockFreePriotityQueue     CommonView::m_priority_queue;

volatile bool   CommonView::m_running_flag = false;

LogIdentifier  CommonView::m_zero_log_id;

LogIdentifier  CommonView::m_max_log_id;

std::vector<std::thread*>    CommonView::m_vec_routine;

using ::RaftCore::Timer::GlobalTimer;
using ::RaftCore::Storage::StorageGlobal;

void CommonView::Initialize() noexcept {

    m_zero_log_id.m_term = 0;
    m_zero_log_id.m_index = 0;

    m_max_log_id.m_term  = 0xFFFFFFFF;
    m_max_log_id.m_index = 0xFFFFFFFFFFFFFFFF;

    m_cpu_cores = std::thread::hardware_concurrency();
    CHECK(m_cpu_cores > 0);

    //Register storage's GC.
    auto  *_p_storage = &StorageGlobal::m_instance;
    auto _storage_gc = [_p_storage]()->bool {
        _p_storage->PurgeGarbage();
        return true;
    };

    bool _enable_sstable_gc = true;
#ifdef _COMMON_VIEW_TEST_
    _enable_sstable_gc = ::RaftCore::Config::FLAGS_enable_sstable_gc;
#endif
    if (_enable_sstable_gc)
        GlobalTimer::AddTask(::RaftCore::Config::FLAGS_sstable_purge_interval_second*1000,_storage_gc);

    int consumer_threads_num = ::RaftCore::Config::FLAGS_lockfree_queue_consumer_threads_num;
    if (consumer_threads_num == 0)
        consumer_threads_num = m_cpu_cores;

    //Start initializing the MCMP queue.
    m_priority_queue.Initialize(consumer_threads_num);
}

void CommonView::UnInitialize() noexcept {

    CommonView::m_vec_routine.clear();

    m_priority_queue.UnInitialize();
}

}

