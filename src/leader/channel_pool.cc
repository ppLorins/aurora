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

#include <typeinfo>

#include "grpc++/create_channel.h"

#include "config/config.h"
#include "client/client_impl.h"
#include "leader/channel_pool.h"

namespace RaftCore::Leader {

using ::RaftCore::Client::HeartbeatSyncClient;

ChannelPool::ChannelPool(const std::string &peer_addr, uint32_t pool_size) noexcept {

    this->m_channel_pool.reset(new TypeVecChannel());

    for (std::size_t i = 0; i < ::RaftCore::Config::FLAGS_conn_per_link; ++i) {
        for (std::size_t j = 0; j < pool_size; ++j) {
            auto _channel_args = ::grpc::ChannelArguments();

            std::string _key = "custom_key_" + std::to_string(i);
            std::string _val = "custom_val_" + std::to_string(i);
            _channel_args.SetString(_key, _val);

            auto _shp_channel = ::grpc::CreateCustomChannel(peer_addr, ::grpc::InsecureChannelCredentials(), _channel_args);
            this->m_channel_pool->emplace_back(_shp_channel);
        }
    }

    this->m_peer_addr = peer_addr;
}

ChannelPool::~ChannelPool() noexcept{}

std::shared_ptr<::grpc::Channel> ChannelPool::GetOneChannel() noexcept {
    uint32_t _random_idx = this->m_idx.fetch_add(1,std::memory_order_relaxed) % this->m_channel_pool->size();
    return this->m_channel_pool->operator[](_random_idx);
}

void ChannelPool::HeartBeat(uint32_t term,const std::string &my_addr) noexcept{

    auto _shp_channel = this->GetOneChannel();
    HeartbeatSyncClient  _heartbeat_client(_shp_channel);

    auto _setter = [&](std::shared_ptr<::raft::HeartBeatRequest>& req) {
        req->mutable_base()->set_addr(my_addr);
        req->mutable_base()->set_term(term);
    };

    auto _rpc = std::bind(&::raft::RaftService::Stub::HeartBeat, _heartbeat_client.GetStub().get(),
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

    ::grpc::Status  _status;
    auto &_rsp = _heartbeat_client.DoRPC(_setter, _rpc,
        ::RaftCore::Config::FLAGS_leader_heartbeat_rpc_timeo_ms, _status);

    if (!_status.ok()) {
        LOG(ERROR) << "heart to follower:" << this->m_peer_addr << " rpc fail"
            << ",err code:" << _status.error_code() << ",err msg:" << _status.error_message();
        return;
    }

    if (_rsp.result()!=::raft::ErrorCode::SUCCESS) {
        LOG(ERROR) << "heart to follower:" << this->m_peer_addr << " svr return fail," << ",msg:" << _rsp.err_msg();
        return;
    }

    VLOG(99) << "follower " << this->m_peer_addr << " checking heartbeat success!";
}

}

