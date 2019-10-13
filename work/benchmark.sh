#!/bin/bash

working_dir='C:/Users/95/Documents/Visual Studio 2015/Projects/apollo/raft/work'

client_exe='C:/Users/95/Documents/Visual Studio 2015/Projects/apollo/Release/grpc_1.8.x_client.exe'
svr_exe='C:/Users/95/Documents/Visual Studio 2015/Projects/apollo/Debug/grpc_1.8.x_svr.exe'

if [ "$1" == "release" ];then
    svr_exe="C:/Users/95/Documents/Visual Studio 2015/Projects/apollo/Release/grpc_1.8.x_svr.exe"
fi


cd "${working_dir}"

for cq_pair in {1,2,4}
do
    for thread_pair in {1,2,4,8,16}
    do
        for pool in {100,200,400}
        do
            "${svr_exe}" --thread_pair=1 --cq_pair=1 --pool=100 > /dev/null 2>&1 &
            pid=$!
            output=`"${client_exe}" --count_per_thread=20000 --thread_per_cq=2 --cq=2 --addr=localhost:50051 --conn=1`
            tp=`echo "${output}" | grep 'final throughput' | awk -F: '{print $2}'`
            echo "cq_pair:${cq_pair},thread_pair:${thread_pair},pool:${pool} tp:${tp}"
            kill -9 ${pid}
        done
    done
done



