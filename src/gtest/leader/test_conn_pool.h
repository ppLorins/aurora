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

#ifndef __GTEST_CONNECTION_POOL_H__
#define __GTEST_CONNECTION_POOL_H__

#include <list>
#include <memory>
#include <chrono>

#include "gtest/test_base.h"
#include "client/client_impl.h"
#include "leader/channel_pool.h"
#include "leader/client_pool.h"

using ::raft::HeartBeatRequest;
using ::RaftCore::Leader::ChannelPool;
using ::RaftCore::Leader::ClientPool;
using ::RaftCore::Client::AppendEntriesAsyncClient;

class TestConnPool : public TestSingleBackendFollower {

    public:

        TestConnPool() {}

    protected:

        virtual void SetUp() override {

            this->m_shp_cq.reset(new CompletionQueue());

            this->m_shp_channel_pool.reset(new ChannelPool(this->m_follower_svc_addr,::RaftCore::Config::FLAGS_channel_pool_size));
            auto _channel = this->m_shp_channel_pool->GetOneChannel();

            for (int i = 0; i < this->m_cpu_cores; ++i) {
                std::shared_ptr<AppendEntriesAsyncClient>  _shp_client( new AppendEntriesAsyncClient(_channel, this->m_shp_cq, false));
                this->m_obj_pool.Back(_shp_client);
            }
        }

        virtual void TearDown() override { }

        std::shared_ptr<ChannelPool>    m_shp_channel_pool;

        std::shared_ptr<CompletionQueue>    m_shp_cq;

        ClientPool<AppendEntriesAsyncClient>    m_obj_pool;
};

TEST_F(TestConnPool, GeneralOperation) {

    std::cout << "start.." << std::endl;

    auto _shp_client = m_obj_pool.Fetch();
    m_obj_pool.Back(_shp_client);

    auto _shp_channel = this->m_shp_channel_pool->GetOneChannel();

    //Test 0 term is okay.
    this->m_shp_channel_pool->HeartBeat(0,this->m_leader_addr);

    ASSERT_EQ(m_obj_pool.GetParentFollower(),nullptr);

    std::cout << "end.." << std::endl;
}

TEST_F(TestConnPool, ConcurrentOperation) {

    uint32_t _threads = ::RaftCore::Config::FLAGS_launch_threads_num;

    uint32_t _op_count = ::RaftCore::Config::FLAGS_conn_op_count;

    auto _op = [&](int thread_idx) {

        for (std::size_t i = 0; i < _op_count; ++i) {
            auto _shp_client = m_obj_pool.Fetch();
            ASSERT_TRUE(_shp_client);
            _shp_client->PushCallBackArgs(nullptr);
            _shp_client->PushCallBackArgs(nullptr);
            _shp_client->Reset();

            m_obj_pool.Back(_shp_client);

            this->m_shp_channel_pool->HeartBeat(0,this->m_leader_addr);
        }
    };

    this->LaunchMultipleThread(_op, _threads);

    uint32_t _total = _op_count * 2 * _threads;

    uint64_t _time_cost_us = this->GetTimeCost();
    std::cout << "(M)operations per second:" << _total / float(_time_cost_us) << std::endl;
}


#endif
