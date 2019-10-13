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

#ifndef __AURORA_CHANNEL_POOL_EX_H__
#define __AURORA_CHANNEL_POOL_EX_H__

#include <memory>
#include <unordered_map>

#include "grpc/grpc.h"
#include "grpc++/grpc++.h"

#include "protocol/raft.pb.h"
#include "protocol/raft.grpc.pb.h"

namespace RaftCore::Leader {

class ChannelPool final{

public:

    ChannelPool(const std::string &peer_addr,uint32_t pool_size) noexcept;

    virtual ~ChannelPool() noexcept;

    void HeartBeat(uint32_t term,const std::string &my_addr) noexcept;

    std::shared_ptr<::grpc::Channel> GetOneChannel() noexcept;

private:

    typedef std::vector<std::shared_ptr<::grpc::Channel>>  TypeVecChannel;

    //Read only after initialization.
    std::shared_ptr<TypeVecChannel>     m_channel_pool;

    //Relatively random accessing.
    std::atomic<uint32_t>    m_idx;

    std::string  m_peer_addr;

private:

    ChannelPool(const ChannelPool&) = delete;

    ChannelPool& operator=(const ChannelPool&) = delete;
};

} //end namespace

#endif
