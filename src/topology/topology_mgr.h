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

#ifndef __AURORA_TOPOLOGY_H__
#define __AURORA_TOPOLOGY_H__

#include <iostream>
#include <fstream>
#include <set>
#include <string>
#include <shared_mutex>

#define _AURORA_TOPOLOGY_CONFFIG_FILE_ "topology.config"

namespace RaftCore{

struct Topology {
    std::string                 m_leader = "";
    std::set<std::string>       m_followers;
    std::set<std::string>       m_candidates;
    std::string                 m_my_addr = "";

    Topology()noexcept;

    void Reset()noexcept;

    uint32_t GetClusterSize() const noexcept;

    bool InCurrentCluster(const std::string &node) noexcept;
};


std::ostream& operator<<(std::ostream& os, const Topology& obj);

class CTopologyMgr final{

public:

    static void Initialize() noexcept;

    static void UnInitialize() noexcept;

    static void Update(const Topology &input) noexcept;

    static void Read(Topology *p_output=nullptr) noexcept;

private:

    static bool Load() noexcept;

private:

    static Topology         m_ins;

    static std::fstream     m_file_stream;

    static std::shared_timed_mutex    m_mutex;

private:

    CTopologyMgr() = delete;

    virtual ~CTopologyMgr() noexcept = delete;

    CTopologyMgr(const CTopologyMgr&) = delete;

    CTopologyMgr& operator=(const CTopologyMgr&) = delete;

};

}


#endif