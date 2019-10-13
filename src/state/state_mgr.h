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

#ifndef __AURORA_STATE_MGR_H__
#define __AURORA_STATE_MGR_H__

#include <list>

#include "topology/topology_mgr.h"

namespace RaftCore::State {

enum class RaftRole{   UNKNOWN ,   //Init state
                        LEADER,     //Raft Leader
                        CANDIDATE , //Raft Candidate
                        FOLLOWER ,  //Raft Follower
                    };

class StateMgr final{

public:

    static void Initialize(const ::RaftCore::Topology  &global_topo) noexcept;

    static void UnInitialize() noexcept;

    static bool Ready() noexcept;

    static RaftRole GetRole() noexcept;

    static const char* GetRoleStr(RaftRole state=RaftRole::UNKNOWN) noexcept;

    static void SwitchTo(RaftRole state,const std::string &new_leader="") noexcept;

    static const std::string& GetMyAddr() noexcept;

    static const std::list<std::string>& GetNICAddrs() noexcept;

    static void SetMyAddr(const std::string& addr) noexcept;

    static bool AddressUndetermined() noexcept;

private:

    StateMgr() = delete;

    virtual ~StateMgr() noexcept = delete;

    StateMgr(const StateMgr &) = delete;

    StateMgr& operator=(const StateMgr &) = delete;

private:

    /* For the 'm_cur_state' variable ,multiple thread reading, one thread blind-writing, no need to lock.
      Yeah , hopefully things like EMSI protocol can keep CPU caches from being inconsistent.
      I believe it anyway.  */
    static RaftRole    m_cur_state;

    static bool    m_initialized;

    static std::string  m_my_addr;

    static std::list<std::string>  m_nic_addrs;
};

std::ostream& operator<<(std::ostream& os, const RaftRole& obj);


}


#endif
