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

#ifndef __GTEST_CLIENT_H__
#define __GTEST_CLIENT_H__

#include "gtest/test_base.h"
#include "client/client_impl.h"

using ::RaftCore::Client::AppendEntriesAsyncClient;

class TestClient : public TestBase {

    public:

        TestClient() {}

        virtual void SetUp() override {
            auto _channel_args = ::grpc::ChannelArguments();
            this->m_shp_channel = ::grpc::CreateCustomChannel(this->m_leader_addr, ::grpc::InsecureChannelCredentials(), _channel_args);
        }

        virtual void TearDown() override {
        }

    protected:

        std::shared_ptr<::grpc::Channel>    m_shp_channel;

};

TEST_F(TestClient, GeneralOperation) {

    std::shared_ptr<CompletionQueue> _shp_cq;

    for (std::size_t i = 0; i < ::RaftCore::Config::FLAGS_client_count; ++i)
        auto *_obj_client = new AppendEntriesAsyncClient(this->m_shp_channel, _shp_cq);

    std::this_thread::sleep_for(std::chrono::seconds(30));
}



#endif
