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

#include <chrono>
#include <thread>

#include "common/comm_defs.h"
#include "config/config.h"
#include "tools/timer.h"

namespace RaftCore::Timer {

std::priority_queue<GlobalTimer::Task,std::deque<GlobalTimer::Task>,GlobalTimer::TaskCmp>    GlobalTimer::m_heap;

std::shared_timed_mutex     GlobalTimer::m_share_timed_mutex;

volatile GlobalTimer::ETimerThreadState  GlobalTimer::m_thread_state = GlobalTimer::ETimerThreadState::INITIALIZED;

using ::RaftCore::Common::WriteLock;
using ::RaftCore::Common::ReadLock;

bool GlobalTimer::TaskCmp::operator()(const Task &x, const Task &y) {
    return x.m_next_run > y.m_next_run;
}

void GlobalTimer::Task::operator=(const GlobalTimer::Task &one) {
    this->m_next_run    = one.m_next_run;
    this->m_interval_ms = one.m_interval_ms;
    this->m_processor   = one.m_processor;
}

void GlobalTimer::Initialize() noexcept {
    m_thread_state = ETimerThreadState::RUNNING;
    std::thread _t([&]() { ThreadEntrance(); });
    _t.detach();
}

void GlobalTimer::UnInitialize() noexcept {
    Stop();
    WriteLock _w_lock(m_share_timed_mutex);
    while (!m_heap.empty())
        m_heap.pop();
}

void GlobalTimer::AddTask(uint32_t interval_ms, std::function<bool()> processor) noexcept{

    //interval_ms==0 mean one shot, should be supported.
    if (interval_ms > 0)
        CHECK(interval_ms >= ::RaftCore::Config::FLAGS_timer_precision_ms) << "timer precision not enough :"
                << ::RaftCore::Config::FLAGS_timer_precision_ms << "|" << interval_ms;

    uint64_t _now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    WriteLock _w_lock(m_share_timed_mutex);
    m_heap.emplace(_now + interval_ms,interval_ms, processor);
}

void GlobalTimer::ThreadEntrance() noexcept {

    LOG(INFO) << "Global timer thread started.";

    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(::RaftCore::Config::FLAGS_timer_precision_ms));
        if (m_thread_state == ETimerThreadState::STOPPING)
            break;

        uint64_t _now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        WriteLock _w_lock(m_share_timed_mutex);
        while (!m_heap.empty()) {

            /*Avoiding one single task takes too much time thus timing out all the subsequent tasks.
               Falling into a deal loop.  */
            if (m_thread_state == ETimerThreadState::STOPPING)
                break;

            const auto &_task = m_heap.top();

            if (_task.m_next_run > _now)
                break;

            //Run the task.Returned value indicating whether that task wants to be executed next time.
            if (_task.m_processor()) {
                //Push the next round of execution of task.
                auto _new = _task;
                _new.m_next_run = _now + _task.m_interval_ms;

                //emplace() will causes issues under MSVC 2015.details see : https://gist.github.com/ppLorins/09de033a4b0748d883c8bf8fe12b7703
                //m_heap.emplace(_now + _task.m_interval_ms, _task.m_interval_ms, _task.m_processor);
                m_heap.push(_new);
            }

            m_heap.pop();
        }
    }

    m_thread_state = ETimerThreadState::STOPPED;

    LOG(INFO) << "Global timer thread exited.";
}

void GlobalTimer::Stop() noexcept {
    m_thread_state = ETimerThreadState::STOPPING;
    while (m_thread_state != ETimerThreadState::STOPPED)
        std::this_thread::sleep_for(std::chrono::microseconds(::RaftCore::Config::FLAGS_thread_stop_waiting_us));
}

}



