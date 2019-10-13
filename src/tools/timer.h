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

#ifndef __AURORA_TIMER_H__
#define __AURORA_TIMER_H__

#include <stdint.h>
#include <queue>
#include <functional>
#include <shared_mutex>

namespace RaftCore::Timer {

class GlobalTimer {

    public:

        static void Initialize() noexcept;

        static void UnInitialize() noexcept;

        static void AddTask(uint32_t interval_ms,std::function<bool()> processor) noexcept;

    private:

        static void Stop() noexcept;

        static void ThreadEntrance() noexcept;

    private:

        struct Task {

            Task(int x, std::function<bool()> y) : m_interval_ms(x),m_processor(y) {};

            Task(uint64_t a, int x, std::function<bool()> y) : m_next_run(a), m_interval_ms(x),m_processor(y) {};

            Task(uint64_t a, int x) : m_next_run(a), m_interval_ms(x) {};

            void operator=(const Task &one);

            uint64_t                m_next_run;
            int                     m_interval_ms;
            std::function<bool()>   m_processor;
        };

        struct TaskCmp {
            bool operator()(const Task &x,const Task &y);
        };

        enum class ETimerThreadState {INITIALIZED,RUNNING,STOPPING,STOPPED};

        static std::priority_queue<Task,std::deque<Task>,TaskCmp>    m_heap;

        static std::shared_timed_mutex      m_share_timed_mutex;

        static volatile ETimerThreadState   m_thread_state;

    private:

        GlobalTimer() = delete;

        virtual ~GlobalTimer() noexcept = delete;

        GlobalTimer(const GlobalTimer&) = delete;

        GlobalTimer& operator=(const GlobalTimer&) = delete;
};

}

#endif

