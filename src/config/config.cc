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

#include  "config/config.h"

namespace RaftCore::Config {

    DEFINE_uint32(notify_cq_num, 2, "#notify CQ the server use.");
    DEFINE_uint32(notify_cq_threads, 4, "#threads polling on each notify CQ.");
    DEFINE_uint32(call_cq_num, 2, "#call CQ the server use.");
    DEFINE_uint32(call_cq_threads, 2, "#threads polling on each call CQ.");
    DEFINE_uint32(client_cq_num, 2, "#completion queues dedicated for process backend RPCs in the leader.");
    DEFINE_uint32(client_thread_num, 2, "#threads for each client CQ.");
    DEFINE_uint32(request_pool_size, 100, "#call data instance each thread hold.");

    DEFINE_uint32(binlog_append_file_timeo_us, 1500, "append binlog cv wait timeout in microseconds .");
    DEFINE_uint32(binlog_meta_hash_buf_size, 4, "binlog's meta data hash buf size in MB, each server instance has only one of such buffer.");

    DEFINE_uint32(binlog_max_size, 1024 * 1024 * 128, "max size(meta data not included) in bytes for each individual binlog file,used together with binlog_max_log_num.");
    DEFINE_uint32(binlog_max_log_num, 1024 * 1024 * 4, "max #logs for each individual binlog file,used together with binlog_max_size.");
    DEFINE_uint32(binlog_parse_buf_size, 64, "buf size(in MB) used for parsing binlog on server startup.");

    DEFINE_string(ip, "0.0.0.0", "svr listening ipv4 address.");
    DEFINE_uint32(port, 10010, "svr listening port.");
    DEFINE_uint32(guid_step_len, 200, "guid increase step length.");
    DEFINE_uint32(guid_disk_random_write_hint_ms, 20, "non SSD disk random write latency hint in milliseconds");

    DEFINE_uint32(leader_heartbeat_rpc_timeo_ms, 50, "heartbeat timeout in millisecond.");
    DEFINE_uint32(leader_append_entries_rpc_timeo_ms, 100, "AppendEntries rpc timeout in millisecond.");
    DEFINE_uint32(leader_commit_entries_rpc_timeo_ms, 100, "CommitEntries rpc timeout in millisecond.");
    DEFINE_uint32(leader_resync_log_rpc_timeo_ms, 3000, "resync log rpc timeout in millisecond.");

    DEFINE_uint32(leader_heartbeat_interval_ms, 500, "leader dedicated thread sending heartbeat intervals in ms.");
    DEFINE_uint32(leader_last_log_resolve_additional_wait_ms, 100, "additional wait ms when resolving the last log.");

    DEFINE_uint32(lockfree_queue_resync_log_elements,2 * 1024,"#elements in the resync log queue,round up to nearest 2^n.");
    DEFINE_uint32(lockfree_queue_resync_data_elements, 1 * 1024, "#elements in the resync data queue,round up to nearest 2^n.");
    DEFINE_uint32(lockfree_queue_client_react_elements, 1024 * 1024, "#elements in the client react queue,round up to nearest 2^n.");

    DEFINE_uint32(lockfree_queue_consumer_wait_ms, 800, "the waiting time in us that waiting on the queue's CV.");
    DEFINE_uint32(lockfree_queue_consumer_threads_num, 16, "#consuemr thread,0 means spawninig them by #CPU cores.");

    DEFINE_uint32(list_op_tracker_hash_slot_num, 64, "#slots a operation tracker will use.");

    DEFINE_uint32(timer_precision_ms, 10, "timer check intervals in milliseconds.");
    DEFINE_uint32(thread_stop_waiting_us, 3, "the interval in us to check if the thread if stopped as intended.");
    DEFINE_uint32(gc_interval_ms, 30, "the interval in ms for garbage collecting.");
    DEFINE_uint32(garbage_deque_retain_num, 5000, "the #(garbage deque node) retaining  when doing GC in deque.");

    DEFINE_uint32(conn_per_link, 64, "#tcp connections between each leader<-->follower link.");
    DEFINE_uint32(channel_pool_size, 10, "#channels for each tcp connection.");
    DEFINE_uint32(client_pool_size, 10000, "#clients the client pool maintained for each follower.");

    DEFINE_uint32(resync_log_reverse_step_len, 20, "the reversing step len when the leader try to find the lastest consistent log entry with a follower.");

    DEFINE_uint32(resync_data_item_num_each_rpc, 1024, "the #data items in a single sending of a stream RPC call.");
    DEFINE_uint32(resync_data_log_num_each_rpc, 1024, "the #replicated logs in a single sending of a stream RPC call.");
    DEFINE_uint32(resync_data_task_max_time_ms, 200, "the max time in millisecond a resync data task can hold in a single execution.");
    DEFINE_uint32(binlog_reserve_log_num, 100, "the #log reserved before the ID-LCL when rotating binlog file.");

    DEFINE_uint32(group_commit_count, 500, "#previous appending requests that a commit request at least represents.");
    DEFINE_uint32(cut_empty_timeos_ms, 500, "In leader, if a replicated msg cannot be successfully processed within this time, an error will be returned.");

    DEFINE_uint32(iterating_threads, 4, "#threads for iterating the unfinished requests.");
    DEFINE_uint32(iterating_wait_timeo_us, 50 * 1000, "follower disorder threads wait on CV timeout in microseconds.");

    DEFINE_uint32(follower_check_heartbeat_interval_ms, 10, "interval in milliseconds of follower's checking leader's heartbea behavior,must be.");
    DEFINE_uint32(disorder_msg_timeo_ms, 1000, "In follower, if a disorder msg cannot be successfully processed within this time, an error will be returned.");

    DEFINE_uint32(cgg_wait_for_last_released_guid_finish_us, 50, "there is a time windows one thread can still generating guids even server status already been \
                            set to HALT.This is the us waiting to it elapses.");

    DEFINE_uint32(election_heartbeat_timeo_ms, 3000, "this is the duraion after it elapsed the follower will start to turn into candidate role.");
    DEFINE_uint32(election_term_interval_min_ms, 150, "the lower bound of sleeping interval before incease term and start a new election.");
    DEFINE_uint32(election_term_interval_max_ms, 300, "the upper bound of sleeping interval before incease term and start a new election.");
    DEFINE_uint32(election_vote_rpc_timeo_ms, 2000, "vote rpc timeout in millisecond.");
    DEFINE_uint32(election_non_op_timeo_ms, 500, "timeo value in ms of the submitting non-op log entry operation after new leader elected.");
    DEFINE_uint32(election_wait_non_op_finish_ms, 200, "time in ms waiting for non-op finished.");

    DEFINE_uint32(memchg_sync_data_wait_seconds, 1, "leader will wait for the newly joined nodes to finish sync all the data,this is how long it will wait during each round of waiting.");
    DEFINE_uint32(memchg_rpc_timeo_ms, 50, "membership change RPC timeout in milliseconds.");

    DEFINE_uint32(memory_table_max_item, 1024 * 1024 * 2, "max #records a memory can hold.");
    DEFINE_uint32(memory_table_hash_slot_num, 10 * 1000, "#slots a memory table object's inner hash object can hold.");
    DEFINE_uint32(sstable_table_hash_slot_num, 10 * 1000, "#slots a sstable table object's inner hash object can hold.");
    DEFINE_uint32(sstable_purge_interval_second, 10, "Interval in seconds of merging and purging sstabls.");

    DEFINE_uint32(child_glog_v, 90, "the GLOG_v environment variable used for child processes in gtest.");
    DEFINE_uint32(election_thread_wait_us, 1000, "the waiting time between each check of election thread exiting.");
    DEFINE_bool(do_heartbeat, true, "whether leader sending heartbeat message to followers or not.");
    DEFINE_bool(heartbeat_oneshot, false, "sending heartbeat message just once.");
    DEFINE_bool(member_leader_gone, false, "whether the old leader will exist in the new cluster or not.");
    DEFINE_uint32(concurrent_client_thread_num, 0, "#thread client using when doing benchmark.");
    DEFINE_bool(enable_sstable_gc, true, "whether enable sstable purging or not.");
    DEFINE_bool(checking_heartbeat, true, "whether follower checking heartbeat or not.");
    DEFINE_uint32(append_entries_start_idx, 8057, "#log start index for the AppendEntries interface.");
    DEFINE_bool(clear_existing_sstable_files, true, "whether delete all existing sstable files or not.");
    DEFINE_uint32(hash_slot_num, 500, "#slots in lockfree hash.");
    DEFINE_uint32(resync_log_start_idx, 8057, "#log start index for the LeaderView::ResyncLog interface.");
    DEFINE_uint32(deque_push_count, 100000, "#elements pushed before testing.");
    DEFINE_uint32(meta_count, 80000, "#meta items for testing memory useage.");
    DEFINE_uint32(follower_svc_benchmark_req_round, 10000, "#rounds(phaseI+phaseII) of requests sent during follower service benchmarking.");
    DEFINE_uint32(leader_svc_benchmark_req_count, 10000, "#requests of requests sent during leader service benchmarking.");
    DEFINE_uint32(benchmark_client_cq_num, 2, "#CQ client used to trigger the requests.");
    DEFINE_uint32(benchmark_client_thread_num_per_cq, 4, "#threads client per CQ used to trigger the requests.");
    DEFINE_uint32(client_write_timo_ms, 50, "timeout value(ms) for client writing.");
    DEFINE_bool(benchmark_client_split_entrusting, true, "whether to split the benchmark client entrusing process.");
    DEFINE_string(target_ip, "default_none", "the target ip for a new benchmark server.");
    DEFINE_string(my_ip, "default_none", "the ip addr to indicate myself in client req.");
    DEFINE_uint32(storage_get_slice_count, 10, "#elements get from get_slice().");
    DEFINE_uint32(retain_num_unordered_single_list, 100, "retain num for unordered_single_list unit test.");
    DEFINE_bool(do_commit, false, "whether issue the commit request or not after appenedEntries.");
    DEFINE_uint32(value_len, 2, "value length in unite test.");
}
