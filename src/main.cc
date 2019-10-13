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

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "global/global_env.h"

int main(int argc,char** argv) {

    google::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    FLAGS_log_dir = ".";
    FLAGS_logbuflevel = -1;

    //Start the whole thing.
    ::RaftCore::Global::GlobalEnv::InitialEnv();
    ::RaftCore::Global::GlobalEnv::RunServer();

    return 0;
}

