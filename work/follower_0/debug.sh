#!/bin/bash


grep "insert a disorder msg" raft_svr.exe.ARTHUR-PC.arthur.log.INFO.* | awk '{print $12}' | awk -F: '{print $3}' > tmp.txt
grep "process disorder done" raft_svr.exe.ARTHUR-PC.arthur.log.INFO.* | awk '{print $15}' | awk -F: '{print $3}' >> tmp.txt
sort tmp.txt > tmp2.txt

cur_base=''
start=0

while read line;do
	if [ $start -eq 0 ];then
		cur_base="$line"
		start=1
		continue
	fi
	
	if [ "$cur_base" == "$line" ];then
		start=0
		continue
	fi
	
	echo "error line:$cur_base"
	cur_base="$line"
	start=1
	
done < tmp2.txt