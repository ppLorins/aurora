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


#ifndef _AURORA_CONFIG_H_
#define _AURORA_CONFIG_H_

#include "gflags/gflags.h"

namespace RaftCore::Config {

    //Service config.
    DECLARE_uint32(notify_cq_num);
    DECLARE_uint32(notify_cq_threads);
    DECLARE_uint32(call_cq_num);
    DECLARE_uint32(call_cq_threads);
    DECLARE_uint32(client_cq_num);
    DECLARE_uint32(client_thread_num);
    DECLARE_uint32(request_pool_size);

    //Common config
    DECLARE_string(ip);
    DECLARE_uint32(port);

    DECLARE_uint32(lockfree_queue_resync_log_elements);
    DECLARE_uint32(lockfree_queue_resync_data_elements);
    DECLARE_uint32(lockfree_queue_client_react_elements);
    DECLARE_uint32(lockfree_queue_consumer_wait_ms);
    DECLARE_uint32(lockfree_queue_consumer_threads_num);

    DECLARE_uint32(list_op_tracker_hash_slot_num);

    DECLARE_uint32(timer_precision_ms);
    DECLARE_uint32(thread_stop_waiting_us);
    DECLARE_uint32(gc_interval_ms);
    DECLARE_uint32(garbage_deque_retain_num);

    DECLARE_uint32(iterating_threads);
    DECLARE_uint32(iterating_wait_timeo_us);

    //Guid
    DECLARE_uint32(guid_step_len);
    DECLARE_uint32(guid_disk_random_write_hint_ms);

    //Binlog config
    DECLARE_uint32(binlog_meta_hash_buf_size);
    DECLARE_uint32(binlog_max_size);
    DECLARE_uint32(binlog_max_log_num);
    DECLARE_uint32(binlog_parse_buf_size);
    DECLARE_uint32(binlog_append_file_timeo_us);
    DECLARE_uint32(binlog_reserve_log_num);

    //leader config --> follower entity
    DECLARE_uint32(leader_heartbeat_rpc_timeo_ms);
    DECLARE_uint32(leader_append_entries_rpc_timeo_ms);
    DECLARE_uint32(leader_commit_entries_rpc_timeo_ms);
    DECLARE_uint32(leader_resync_log_rpc_timeo_ms);
    DECLARE_uint32(leader_heartbeat_interval_ms);
    DECLARE_uint32(leader_last_log_resolve_additional_wait_ms);

    //leader config --> connection pool
    DECLARE_uint32(conn_per_link);
    DECLARE_uint32(channel_pool_size);
    DECLARE_uint32(client_pool_size);

    //leader config --> resync log
    DECLARE_uint32(resync_log_reverse_step_len);

    //leader config --> resync data
    DECLARE_uint32(resync_data_item_num_each_rpc);
    DECLARE_uint32(resync_data_log_num_each_rpc);
    DECLARE_uint32(resync_data_task_max_time_ms);

    //leader config --> optimization
    DECLARE_uint32(group_commit_count);

    //leader config --> cutempty
    DECLARE_uint32(cut_empty_timeos_ms);

    //follower config
    DECLARE_uint32(follower_check_heartbeat_interval_ms);
    DECLARE_uint32(disorder_msg_timeo_ms);

    //For CGG problem
    DECLARE_uint32(cgg_wait_for_last_released_guid_finish_us);

    //For election.
    DECLARE_uint32(election_heartbeat_timeo_ms);
    DECLARE_uint32(election_term_interval_min_ms);
    DECLARE_uint32(election_term_interval_max_ms);
    DECLARE_uint32(election_vote_rpc_timeo_ms);
    DECLARE_uint32(election_non_op_timeo_ms);
    DECLARE_uint32(election_wait_non_op_finish_ms);

    //For membership change.
    DECLARE_uint32(memchg_sync_data_wait_seconds);
    DECLARE_uint32(memchg_rpc_timeo_ms);

    //For Storage.
    DECLARE_uint32(memory_table_max_item);
    DECLARE_uint32(memory_table_hash_slot_num);
    DECLARE_uint32(sstable_table_hash_slot_num);
    DECLARE_uint32(sstable_purge_interval_second);

    //For unit test.
    DECLARE_uint32(child_glog_v);
    DECLARE_uint32(election_thread_wait_us);
    DECLARE_bool(do_heartbeat);
    DECLARE_bool(heartbeat_oneshot);
    DECLARE_bool(member_leader_gone);
    DECLARE_uint32(concurrent_client_thread_num);
    DECLARE_bool(enable_sstable_gc);
    DECLARE_bool(checking_heartbeat);
    DECLARE_uint32(append_entries_start_idx);
    DECLARE_bool(clear_existing_sstable_files);
    DECLARE_uint32(hash_slot_num);
    DECLARE_uint32(resync_log_start_idx);
    DECLARE_uint32(deque_push_count);
    DECLARE_uint32(meta_count);
    DECLARE_uint32(follower_svc_benchmark_req_round);
    DECLARE_uint32(leader_svc_benchmark_req_count);
    DECLARE_uint32(benchmark_client_cq_num);
    DECLARE_uint32(benchmark_client_thread_num_per_cq);
    DECLARE_uint32(client_write_timo_ms);
    DECLARE_bool(benchmark_client_split_entrusting);
    DECLARE_string(target_ip);
    DECLARE_string(my_ip);
    DECLARE_uint32(storage_get_slice_count);
    DECLARE_uint32(retain_num_unordered_single_list);
    DECLARE_bool(do_commit);
    DECLARE_uint32(value_len);

}

#endif
