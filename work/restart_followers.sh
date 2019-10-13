#!/bin/bash

log_pattern=*aurora.VM_16_7_centos.root*
bin=../../bin/release/aurora
glog_para=''
process_name=`basename $bin`

if [[ "$OSTYPE" == "cygwin" ]]; then
    log_pattern=*raft_svr.exe.ARTHUR-PC.arthur.log*
    bin=../../../Release/raft_svr.exe
    process_name=raft_svr

    #kill them in batch under windows
    ps -ef | grep $process_name | grep -v grep | awk '{print $2}' | xargs kill -9
elif [[ "$OSTYPE" == "darwin"* ]]; then
	log_pattern=*aurora.bogon.arthur*
	glog_para='--log_dir=. --logbuflevel=-1'
fi

restart_one(){
    idx=$1
    cd follower_${idx}
    port=1002${idx}

    if [[ "$OSTYPE" != "cygwin" ]]; then
        pid=`ps -ef | grep $process_name | grep -v grep | grep $port | awk '{print $2}'`
        kill -9 $pid
    fi

    rm -f raft.binlog.follower
    rm -f $log_pattern
    $bin  --checking_heartbeat=false --iterating_wait_timeo_us=50000 --disorder_msg_timeo_ms=100000 --port=${port} --notify_cq_num=1 --notify_cq_threads=4 --call_cq_num=1 --call_cq_threads=4 --iterating_threads=2  $glog_para > std.txt 2>&1 &
    cd ..
}


restart_one 0
restart_one 1
restart_one 2


