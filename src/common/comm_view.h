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

#ifndef __AURORA_COMM_VIEW_H__
#define __AURORA_COMM_VIEW_H__

#include "common/comm_defs.h"
#include "common/log_identifier.h"
#include "config/config.h"
#include "tools/lock_free_deque.h"
#include "tools/lock_free_unordered_single_list.h"
#include "tools/lock_free_priority_queue.h"
#include "tools/timer.h"

namespace RaftCore::Common {

using ::RaftCore::DataStructure::LockFreeUnorderedSingleList;
using ::RaftCore::DataStructure::LockFreePriotityQueue;
using ::RaftCore::Timer::GlobalTimer;
using ::RaftCore::Common::LogIdentifier;

class CommonView {

#ifdef _COMMON_VIEW_TEST_
public:
#else
protected:
#endif

    static void Initialize() noexcept;

    static void UnInitialize() noexcept;

public:

    static int                     m_cpu_cores;

    static LockFreePriotityQueue     m_priority_queue;

    //TODO: find why m_garbage can't be instantiated.
    //template<typename T>
    //static LockFreeDeque<DoubleListNode<T>>     m_garbage;

    //This is the running flag for leader&follower routine threads.
    static volatile bool   m_running_flag;

    static LogIdentifier     m_zero_log_id;

    static LogIdentifier     m_max_log_id;

protected:

    template<template<typename> typename W, template<typename> typename N,typename T>
    static void InstallGC(LockFreeUnorderedSingleList<N<T>> *p_ref_garbage) noexcept {
        p_ref_garbage->SetDeleter(W<T>::ReleaseCutHead);

        auto _pending_list_gc = [p_ref_garbage]()->bool {
            p_ref_garbage->PurgeSingleList(::RaftCore::Config::FLAGS_retain_num_unordered_single_list);
            return true;
        };

        GlobalTimer::AddTask(::RaftCore::Config::FLAGS_gc_interval_ms ,_pending_list_gc);
    }

    static std::vector<std::thread*>    m_vec_routine;
private:

    CommonView() = delete;

    virtual ~CommonView() = delete;

    CommonView(const CommonView&) = delete;

    CommonView& operator=(const CommonView&) = delete;

};

//template<typename T>
//LockFreeDeque<DoubleListNode<T>>   CommonView::m_garbage;

} //end namespace

#endif
