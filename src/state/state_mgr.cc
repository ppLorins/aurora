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

#include <regex>

#include "common/comm_defs.h"
#include "config/config.h"
#include "tools/utilities.h"
#include "state/state_mgr.h"

namespace RaftCore::State {

RaftRole  StateMgr::m_cur_state = RaftRole::UNKNOWN;

bool  StateMgr::m_initialized = false;

std::string StateMgr::m_my_addr = "";

std::list<std::string>  StateMgr::m_nic_addrs;

using ::RaftCore::CTopologyMgr;
using ::RaftCore::Topology;

void StateMgr::Initialize(const ::RaftCore::Topology  &global_topo) noexcept {

    ::RaftCore::Tools::GetLocalIPs(m_nic_addrs);
    std::for_each(m_nic_addrs.begin(), m_nic_addrs.end(), [](std::string &_ip) {
        _ip += std::string(":" + std::to_string(::RaftCore::Config::FLAGS_port) );
    });

    std::list<std::string>  _find_in;
    if (global_topo.m_my_addr.empty())
        _find_in = m_nic_addrs;
    else
        _find_in.emplace_back(global_topo.m_my_addr);

    auto _init_topology = [&]() -> ::RaftCore::State::RaftRole {

        if (std::find(_find_in.cbegin(), _find_in.cend(), global_topo.m_leader) != std::end(_find_in)) {
            m_my_addr = global_topo.m_leader;
            return ::RaftCore::State::RaftRole::LEADER;
        }

        for (const auto & _item : global_topo.m_followers) {
            if (std::find(_find_in.cbegin(), _find_in.cend(), _item) == std::end(_find_in))
                continue;

            m_my_addr = _item;
            return ::RaftCore::State::RaftRole::FOLLOWER;
        }

        for (const auto & _item : global_topo.m_candidates) {
            if (std::find(_find_in.cbegin(), _find_in.cend(), _item) == std::end(_find_in))
                continue;

            m_my_addr = _item;
            return ::RaftCore::State::RaftRole::CANDIDATE;
        }

        //If I'm not in the topology list , assume I'm an empty node ready to be joined.
        return ::RaftCore::State::RaftRole::UNKNOWN;
    };

    m_cur_state = _init_topology();
    CHECK(m_cur_state != ::RaftCore::State::RaftRole::UNKNOWN) << "m_cur_state invalid.";

    m_initialized = true;
}

bool StateMgr::Ready() noexcept {
    return m_initialized;
}

void StateMgr::UnInitialize() noexcept {
    m_cur_state   = ::RaftCore::State::RaftRole::UNKNOWN;
    m_my_addr = "";
}

State::RaftRole StateMgr::GetRole()  noexcept{
    return m_cur_state;
}

const std::list<std::string>& StateMgr::GetNICAddrs() noexcept {
    return m_nic_addrs;
}

bool StateMgr::AddressUndetermined() noexcept {
    return m_my_addr.empty();
}

const char* StateMgr::GetRoleStr(RaftRole state) noexcept
{
    RaftRole _role = m_cur_state;
    if (state != RaftRole::UNKNOWN)
        _role = state;

    if (_role == RaftRole::LEADER)
        return _ROLE_STR_LEADER_;
    else if (_role == RaftRole::FOLLOWER)
        return _ROLE_STR_FOLLOWER_;
    else if (_role == RaftRole::CANDIDATE)
        return _ROLE_STR_CANDIDATE_;
    else if (_role == RaftRole::UNKNOWN)
        return _ROLE_STR_UNKNOWN_;
    else
        CHECK(false);

    return nullptr;
}

void StateMgr::SwitchTo(RaftRole state,const std::string &new_leader) noexcept {

    /*There are 4 valid transitions:
      1. Leader -> Follower. (step down.)
      2. Follower -> Candidate. (start electing.)
      3. Candidate -> Follower. (new leader elected but not me.)
      4. Candidate -> Follower. (new leader elected.It's me.) */

    Topology _topo;
    CTopologyMgr::Read(&_topo);

    //Update topology before switching role.
    if (m_cur_state == RaftRole::LEADER) {
        CHECK(state == RaftRole::FOLLOWER) << "invalid state transition found : Leader -> " << state;

        //Check new leader address format validity.
        std::regex _pattern("\\d{1,3}\.\\d{1,3}\.\\d{1,3}\.\\d{1,3}:\\d+");
        std::smatch _sm;
        CHECK(std::regex_match(new_leader, _sm, _pattern)) << "new leader format valid:" << new_leader;

        _topo.m_leader = new_leader;
        _topo.m_followers.erase(new_leader);
        _topo.m_candidates.erase(new_leader);
        _topo.m_followers.emplace(m_my_addr);

    } else if (m_cur_state == RaftRole::FOLLOWER) {
        CHECK(state == RaftRole::CANDIDATE) << "invalid state transition found : Follower -> " << state;

        _topo.m_followers.erase(m_my_addr);
        _topo.m_candidates.emplace(m_my_addr);

    } else if (m_cur_state == RaftRole::CANDIDATE) {
        CHECK(state == RaftRole::LEADER || state == RaftRole::FOLLOWER) << "invalid state transition found : Candidate -> " << state;

        _topo.m_candidates.erase(m_my_addr);

        if (state == RaftRole::LEADER) {
            _topo.m_followers.emplace(_topo.m_leader);
            _topo.m_candidates.erase(m_my_addr);
            _topo.m_leader = m_my_addr;
        }
        else if (state == RaftRole::FOLLOWER) {
            /* This transition can be caused of :
                1. pre-vote fail.
                2. a new leader has been detected.  */

            //This is for case 2.
            if (!new_leader.empty()) {
                _topo.m_followers.emplace(_topo.m_leader);
                _topo.m_followers.erase(new_leader);
                _topo.m_candidates.erase(new_leader);
                _topo.m_leader = new_leader;
            }

            _topo.m_followers.emplace(m_my_addr);
        }
    } else
        CHECK(false) << "unknown role found :" << m_cur_state;

    m_cur_state = state;

    //Writing new topology to config file.
    CTopologyMgr::Update(_topo);
}

const std::string& StateMgr::GetMyAddr() noexcept {
    return m_my_addr;
}

void StateMgr::SetMyAddr(const std::string& addr) noexcept {
    m_my_addr = addr;
}

std::ostream& operator<<(std::ostream& os, const RaftRole& obj) {
    os << StateMgr::GetRoleStr(obj);
    return os;
}

}
