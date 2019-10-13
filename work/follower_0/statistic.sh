#!/bin/bash


grep "process disorder done" raft_svr.exe.ARTHUR-PC.arthur.log.INFO.* | awk '{print $13}' | awk -F\: 'BEGIN{sum=0;}{x=($2/1000);sum+=x;}END{printf "avg diroder latency(ms):%d",sum/NR;}'


