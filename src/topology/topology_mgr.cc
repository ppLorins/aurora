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
#include "topology/topology_mgr.h"

#define _AURORA_TOPOLOGY_LEADER_INDICATOR_    "leader"
#define _AURORA_TOPOLOGY_FOLLOWER_INDICATOR_  "followers"
#define _AURORA_TOPOLOGY_CANDIDATE_INDICATOR_ "candidates"
#define _AURORA_TOPOLOGY_MYADDR_INDICATOR_    "my_addr"

namespace RaftCore {

std::fstream  CTopologyMgr::m_file_stream;

Topology  CTopologyMgr::m_ins;

std::shared_timed_mutex    CTopologyMgr::m_mutex;

using ::RaftCore::Common::ReadLock;
using ::RaftCore::Common::WriteLock;

std::ostream& operator<<(std::ostream& os, const Topology& obj) {
    os << "------ Topology ------" << std::endl;
    os << "leader:" << std::endl << obj.m_leader << std::endl;

    os << "followers:" <<  std::endl;
    for (const auto &_item : obj.m_followers) {
        os << _item << std::endl;
    }

    os << "candidates:" <<  std::endl;
    for (const auto &_item : obj.m_candidates) {
        os << _item << std::endl;
    }

    os << "my_addr:" << obj.m_my_addr << std::endl;

    return os;
}

Topology::Topology()noexcept {
    this->Reset();
}

void Topology::Reset() noexcept{
    this->m_leader = "";
    this->m_followers.clear();
    this->m_candidates.clear();
    this->m_my_addr = "";
}

uint32_t Topology::GetClusterSize() const noexcept {
    return (uint32_t)this->m_followers.size() + (uint32_t)this->m_candidates.size() + 1;
}

bool Topology::InCurrentCluster(const std::string &node) noexcept{
    if (node == this->m_leader)
        return true;

    if (this->m_candidates.find(node) != this->m_candidates.cend())
        return true;

    if (this->m_followers.find(node) != this->m_followers.cend())
        return true;

    return false;
}

void CTopologyMgr::Initialize() noexcept {
    Load();
}

void CTopologyMgr::UnInitialize() noexcept {
    m_file_stream.close();
}

bool CTopologyMgr::Load() noexcept{

    m_file_stream.open(_AURORA_TOPOLOGY_CONFFIG_FILE_);
    if (!m_file_stream.is_open()) {
        LOG(ERROR) << "open topology config file " << _AURORA_TOPOLOGY_CONFFIG_FILE_ << " fail.";
        return false;
    }

    std::regex _pattern("(#*)\\d{1,3}\.\\d{1,3}\.\\d{1,3}\.\\d{1,3}:\\d+");
    std::smatch _sm;

    WriteLock _w_lock(m_mutex);

    m_ins.Reset();

    int _section_flg = 0; // 1: leader , 2: follower,3:candidate, 4:my_addr

    for (std::string _ori_line; std::getline(m_file_stream, _ori_line);) {

        std::string _line = "";
        //_line.reserve(_ori_line.length());
        std::copy_if(_ori_line.begin(), _ori_line.end(), std::back_inserter(_line), [](char c) { return c != '\r' && c != '\n'; });

        if (_line == _AURORA_TOPOLOGY_LEADER_INDICATOR_) {
            _section_flg = 1;
            continue;
        }

        if (_line == _AURORA_TOPOLOGY_FOLLOWER_INDICATOR_) {
            _section_flg = 2;
            continue;
        }

        if (_line == _AURORA_TOPOLOGY_CANDIDATE_INDICATOR_) {
            _section_flg = 3;
            continue;
        }

        if (_line == _AURORA_TOPOLOGY_MYADDR_INDICATOR_) {
            _section_flg = 4;
            continue;
        }

        if (!std::regex_match(_line, _sm, _pattern)) {
            LOG(ERROR) << "unrecognized line found when parsing topology config file, ignore it:" << _line;
            continue;
        }

        //Support comment.
        if (_sm[1] == "#")
            continue;

        if (_section_flg == 1)
            m_ins.m_leader = _line;
        else if (_section_flg == 2)
            m_ins.m_followers.emplace(_line);
        else if (_section_flg == 3)
            m_ins.m_candidates.emplace(_line);
        else if (_section_flg == 4)
            m_ins.m_my_addr = _line;
        else
            CHECK(false) << "unknown section flag : " << _section_flg;
    }

    //'m_my_addr' must be in the cluster.
    CHECK(m_ins.InCurrentCluster(m_ins.m_my_addr));

    return true;
}

void CTopologyMgr::Read(Topology *p_output) noexcept{

    if (p_output == nullptr) {
        LOG(ERROR) << "input data is null,invalid.";
        return ;
    }

    ReadLock _r_lock(m_mutex);
    p_output->m_leader = m_ins.m_leader;
    p_output->m_followers  = m_ins.m_followers;
    p_output->m_candidates = m_ins.m_candidates;
    p_output->m_my_addr    = m_ins.m_my_addr;
}

void CTopologyMgr::Update(const Topology &input) noexcept{

    WriteLock _w_lock(m_mutex);
    m_ins.m_leader = input.m_leader;
    m_ins.m_followers   = input.m_followers;
    m_ins.m_candidates  = input.m_candidates;
    m_ins.m_my_addr     = input.m_my_addr;

    m_file_stream.close();

    //Reopen file for re-writing and truncate the old contents
    m_file_stream.open(_AURORA_TOPOLOGY_CONFFIG_FILE_,std::ios_base::in | std::ios_base::out | std::ios_base::trunc);

    std::string content = "";
    content.append(std::string(_AURORA_TOPOLOGY_LEADER_INDICATOR_) + "\n");
    content.append(m_ins.m_leader + "\n");

    content.append(std::string(_AURORA_TOPOLOGY_FOLLOWER_INDICATOR_) + "\n");
    for (const auto &item : m_ins.m_followers)
        content.append((item + "\n"));

    content.append(std::string(_AURORA_TOPOLOGY_CANDIDATE_INDICATOR_) + "\n");
    for (const auto &item : m_ins.m_candidates)
        content.append((item + "\n"));

    content.append(std::string(_AURORA_TOPOLOGY_MYADDR_INDICATOR_) + "\n");
    content.append(m_ins.m_my_addr + "\n");

    m_file_stream.write(content.c_str(),content.length());
    m_file_stream.flush();
}


}