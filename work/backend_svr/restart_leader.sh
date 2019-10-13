#!/bin/bash

log_pattern=*aurora.VM_16_7_centos.root*
bin=../../bin/release/aurora
glog_para=''
process_name=`basename $bin`

if [[ "$OSTYPE" == "cygwin" ]]; then
    log_pattern=*raft_svr.exe.ARTHUR-PC.arthur.log*
    bin=../../../Release/raft_svr.exe
    process_name=raft_svr
elif [[ "$OSTYPE" == "darwin"* ]]; then
	log_pattern=*aurora.bogon.arthur*
	glog_para='--log_dir=. --logbuflevel=-1'
fi

pid=`ps -ef | grep $process_name | grep -v grep | awk '{print $2}'`
kill -9 $pid

rm -f $log_pattern
rm -f raft.binlog.leader

$bin --do_heartbeat=false --iterating_wait_timeo_us=2000000 --port=10010 --leader_append_entries_rpc_timeo_ms=5000 --leader_commit_entries_rpc_timeo_ms=5000 --client_cq_num=2 --client_thread_num=2 --notify_cq_num=2 --notify_cq_threads=4 --call_cq_num=2 --call_cq_threads=2 --iterating_threads=2 --client_pool_size=50000 $glog_para > std.txt 2>&1 &
