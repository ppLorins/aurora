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

#include "common/comm_defs.h"
#include "common/error_code.h"
#include "tools/timer.h"
#include "tools/lock_free_priority_queue.h"
#include "binlog/binlog_singleton.h"
#include "storage/storage_singleton.h"
#include "state/state_mgr.h"
#include "election/election.h"
#include "member/member_manager.h"
#include "leader/follower_entity.h"
#include "client/client_framework.h"
#include "service/service.h"
#include "leader/leader_view.h"

namespace RaftCore::Leader {

using ::raft::AppendEntriesRequest;
using ::raft::AppendEntriesResponse;
using ::raft::CommitEntryRequest;
using ::raft::CommitEntryResponse;
using ::RaftCore::DataStructure::LockFreePriotityQueue;
using ::RaftCore::Member::MemberMgr;
using ::RaftCore::Member::EJointStatus;
using ::RaftCore::Client::UnarySyncClient;
using ::RaftCore::Service::Write;

std::unordered_map<std::string,TypePtrFollowerEntity>    LeaderView::m_hash_followers;

TrivialLockDoubleList<MemoryLogItemLeader>      LeaderView::m_entity_pending_list( std::shared_ptr<MemoryLogItemLeader>(new MemoryLogItemLeader(0x0, 0x0)),
                                                                             std::shared_ptr<MemoryLogItemLeader>(new MemoryLogItemLeader(_MAX_UINT32_, _MAX_UINT64_)));

std::condition_variable     LeaderView::m_cv;

std::mutex   LeaderView::m_cv_mutex;

const char*   LeaderView::m_invoker_macro_names[] = { "CLIENT_RPC","BACKGROUP_THREAD" };

std::shared_timed_mutex    LeaderView::m_hash_followers_mutex;

LockFreeUnorderedSingleList<DoubleListNode<MemoryLogItemLeader>>     LeaderView::m_garbage;

std::atomic<LogIdentifier>      LeaderView::m_last_cut_log;

LeaderView::ServerStatus    LeaderView::m_status = LeaderView::ServerStatus::NORMAL;

TrivialLockSingleList<CutEmptyContext>   LeaderView::m_cut_empty_list(std::shared_ptr<CutEmptyContext>(new CutEmptyContext(-1)),
    std::shared_ptr<CutEmptyContext>(new CutEmptyContext(1)));

LockFreeUnorderedSingleList<SingleListNode<CutEmptyContext>>     LeaderView::m_cut_empty_garbage;

std::atomic<uint32_t>      LeaderView::m_last_log_waiting_num;

std::unordered_map<std::thread::id,uint32_t>     LeaderView::m_notify_thread_mapping;

//To avoid issues caused by including header files mutually.
using ::raft::ErrorCode;
using ::RaftCore::BinLog::BinLogGlobal;
using ::RaftCore::BinLog::FileMetaData;
using ::RaftCore::DataStructure::LockFreeQueue;
using ::RaftCore::DataStructure::LockFreeQueueBase;
using ::RaftCore::Storage::StorageMgr;
using ::RaftCore::Storage::StorageGlobal;
using ::RaftCore::Timer::GlobalTimer;
using ::RaftCore::State::StateMgr;
using ::RaftCore::State::RaftRole;
using ::RaftCore::Election::ElectionMgr;
using ::RaftCore::Leader::FollowerStatus;
using ::RaftCore::Common::ReadLock;
using ::RaftCore::Tools::TypeSysTimePoint;

void LeaderView::Initialize(const ::RaftCore::Topology& _topo) noexcept {

    CommonView::Initialize();

    //Follower initialization must being after threads-mapping initialization.
    for (const auto & _follower_addr : _topo.m_followers)
        m_hash_followers[_follower_addr] = std::shared_ptr<FollowerEntity>(new FollowerEntity(_follower_addr));

    int consumer_threads_num = ::RaftCore::Config::FLAGS_lockfree_queue_consumer_threads_num;
    if (consumer_threads_num == 0)
        consumer_threads_num = ::RaftCore::Common::CommonView::m_cpu_cores * 2;

    auto _heartbeat = [&]()->bool {

        if (!::RaftCore::Config::FLAGS_do_heartbeat)
            return true;

        BroadcastHeatBeat();

        //Unit test need a switch.
        return !::RaftCore::Config::FLAGS_heartbeat_oneshot;
    };
    GlobalTimer::AddTask(::RaftCore::Config::FLAGS_leader_heartbeat_interval_ms,_heartbeat);

    //Register connection pool GC.
    auto _conn_pool_deque_gc = []() ->bool {
        LockFreeDeque<AppendEntriesAsyncClient>::GC();
        LockFreeDeque<CommitEntriesAsyncClient>::GC();
        return true;
    };
    GlobalTimer::AddTask(::RaftCore::Config::FLAGS_gc_interval_ms ,_conn_pool_deque_gc);

    //Register GC task to the global timer.
    CommonView::InstallGC<TrivialLockDoubleList,DoubleListNode,MemoryLogItemLeader>(&m_garbage);
    CommonView::InstallGC<TrivialLockSingleList,SingleListNode,CutEmptyContext>(&m_cut_empty_garbage);

    //Add task & callbacks by tasks' priority,highest priority add first.
    auto *_p_client_reacting_queue = new LockFreeQueue<BackGroundTask::ClientReactContext>();
    _p_client_reacting_queue->Initilize(ClientReactCB, ::RaftCore::Config::FLAGS_lockfree_queue_client_react_elements);
    m_priority_queue.AddTask(LockFreePriotityQueue::TaskType::CLIENT_REACTING,(LockFreeQueueBase*)_p_client_reacting_queue);

    auto *_p_fresher_sync_data_queue = new LockFreeQueue<BackGroundTask::SyncDataContenxt>();
    _p_fresher_sync_data_queue->Initilize(SyncDataCB,::RaftCore::Config::FLAGS_lockfree_queue_resync_data_elements);
    m_priority_queue.AddTask(LockFreePriotityQueue::TaskType::RESYNC_DATA,(LockFreeQueueBase*)_p_fresher_sync_data_queue);

    auto *_p_resync_log_queue = new LockFreeQueue<BackGroundTask::ReSyncLogContext>();
    _p_resync_log_queue->Initilize(ReSyncLogCB,::RaftCore::Config::FLAGS_lockfree_queue_resync_log_elements);
    m_priority_queue.AddTask(LockFreePriotityQueue::TaskType::RESYNC_LOG,(LockFreeQueueBase*)_p_resync_log_queue);

    //Launching the background threads for processing that queue.
    m_priority_queue.Launch();

    m_last_log_waiting_num.store(0);

    //Must be initialized before 'CutEmptyRoutine' started.
    CommonView::m_running_flag = true;

    //Start Leader routine thread.
    for (std::size_t i = 0; i < ::RaftCore::Config::FLAGS_iterating_threads; ++i)
        CommonView::m_vec_routine.emplace_back(new std::thread(Write::CutEmptyRoutine));
}

void LeaderView::UnInitialize() noexcept {

    //Waiting for routine thread exit.
    CommonView::m_running_flag = false;

    for (auto* p_thread : CommonView::m_vec_routine) {
        p_thread->join();
        delete p_thread;
    }

    m_hash_followers.clear();
    m_entity_pending_list.Clear();
    m_cut_empty_list.Clear();

    CommonView::UnInitialize();
}

void LeaderView::UpdateThreadMapping() noexcept {
    std::vector<std::thread::id>    _notify_thread_ids;
    for (auto &_one_group : GlobalEnv::m_vec_notify_cq_workgroup)
        _one_group.GetThreadId(_notify_thread_ids);

    for (std::size_t n = 0; n < _notify_thread_ids.size(); ++n)
        m_notify_thread_mapping[_notify_thread_ids[n]] = n;
}

void LeaderView::BroadcastHeatBeat() noexcept {

    auto  *_p_ref = &m_hash_followers;
    auto  *_p_ref_mutex = &m_hash_followers_mutex;

    auto  *_p_ref_joint = &MemberMgr::m_joint_summary;
    auto  *_p_ref_joing_mutex = &MemberMgr::m_mutex;

    //Sending heartbeat to the nodes in the current cluster.
    {
        ReadLock _r_lock(*_p_ref_mutex);
        for (auto &_pair_kv : *_p_ref) {
            VLOG(89) << "heartbeat sending, follower:" << _pair_kv.second->my_addr;
            _pair_kv.second->m_shp_channel_pool->HeartBeat(ElectionMgr::m_cur_term.load(),StateMgr::GetMyAddr());
        }
    }

    //Sending heartbeat to the nodes in the new cluster if there are any.
    do{
        ReadLock _r_lock(*_p_ref_joing_mutex);
        if (_p_ref_joint->m_joint_status != EJointStatus::JOINT_CONSENSUS)
            break;
        for (auto &_pair_kv : _p_ref_joint->m_joint_topology.m_added_nodes)
            _pair_kv.second->m_shp_channel_pool->HeartBeat(ElectionMgr::m_cur_term.load(),StateMgr::GetMyAddr());
    } while (false);
}

auto LeaderView::PrepareAppendEntriesRequest(std::shared_ptr<BackGroundTask::ReSyncLogContext> &shp_context) {
    auto _null_shp = std::shared_ptr<AppendEntriesRequest>();

    //Need to get a new handler of binlog file.
    std::string _binlog_file_name = BinLogGlobal::m_instance.GetBinlogFileName();
    std::FILE* _f_handler = std::fopen(_binlog_file_name.c_str(),_AURORA_BINLOG_READ_MODE_);
    if (_f_handler == nullptr) {
        LOG(ERROR) << "ReSyncLogCB open binlog file " << _binlog_file_name << "fail..,errno:"
                    << errno << ",follower:" << shp_context->m_follower->my_addr;
        return _null_shp;
    }

    std::list<std::shared_ptr<FileMetaData::IdxPair>> _file_meta;
    BinLogGlobal::m_instance.GetOrderedMeta(_file_meta);

    //Find the earlier X entries
    auto _reverse_iter = _file_meta.crbegin();
    if (_reverse_iter == _file_meta.crend()) {
        LOG(WARNING) << "binlog is empty";
        return _null_shp;
    }

    //_reverse_bound is the first element of _file_meta.
    auto _reverse_bound = _file_meta.crend();
    _reverse_bound--;

    auto _log_id_lcl  = StorageGlobal::m_instance.GetLastCommitted();
    uint32_t _precede_lcl_counter = 0; //Count for #log entries that preceding the LCL.

    /*Since _reverse_iter is a reserve iterator , and we are getting the non-reserve iterator
    based on it , so there is one more place(the '<=' in the for loop statement below) to advance.*/
    for (std::size_t n = 0; n <= ::RaftCore::Config::FLAGS_resync_log_reverse_step_len; ) {
        if ((*_reverse_iter)->operator<(_log_id_lcl))
            _precede_lcl_counter++;

        if ((*_reverse_iter)->operator<(shp_context->m_last_sync_point))
            n++;

        _reverse_iter++;

        //Should stopping at the first element of _file_meta.
        if (_reverse_iter == _reverse_bound)
            break;
    }

    /*_cur_iter will points to _reverse_iter-1, aka, the second element of _file_meta after the
      following line, because every log entry need its previous log info when doing resyncing, so we
      cannot start at the first one.*/
    auto _cur_iter = _reverse_iter.base();

    /*The start point log's ID must be greater than (ID-LCL - FLAGS_binlog_reserve_log_num), otherwise the further ahead log entries may be absent.*/
    if (_precede_lcl_counter > ::RaftCore::Config::FLAGS_binlog_reserve_log_num) {
        shp_context->m_hold_pre_lcl = true;
        BinLogGlobal::m_instance.AddPreLRLUseCount();
    }

    /*Note: _reverse_iter is now points to the previous entry of 'STEP_LEN' or the boundary which at least >= (ID-LCL - FLAGS_binlog_reserve_log_num).
     In both cases we just need to begin iterating at _reverse_iter-1 .*/

    //Appending new entries
    std::shared_ptr<AppendEntriesRequest> _shp_req(new AppendEntriesRequest());
    _shp_req->mutable_base()->set_addr(StateMgr::GetMyAddr());
    _shp_req->mutable_base()->set_term(ElectionMgr::m_cur_term.load());

    //Update last sync point to the first entry that will be sent in the next steps.
    shp_context->m_last_sync_point = *(*_cur_iter);

    /*There will not much log entries between [ (ID-LCL - FLAGS_binlog_reserve_log_num) , ID-LRL ], so resync all the logs in one RPC is acceptable. */

    //_cur_iter unchanged ,_pre_iter points to the previous position of _cur_iter.
    auto _pre_iter = (--_cur_iter)++;
    unsigned char* _p_buf = nullptr;
    for (; _cur_iter!=_file_meta.cend(); ++_pre_iter,++_cur_iter) {

        auto _p_entry = _shp_req->add_replicate_entity();
        auto _p_entity_id = _p_entry->mutable_entity_id();
        _p_entity_id->set_term((*_cur_iter)->m_term);
        _p_entity_id->set_idx((*_cur_iter)->m_index);

        auto _p_pre_entity_id = _p_entry->mutable_pre_log_id();
        _p_pre_entity_id->set_term((*_pre_iter)->m_term);
        _p_pre_entity_id->set_idx((*_pre_iter)->m_index);

        //Seek to position
        if (std::fseek(_f_handler, (*_cur_iter)->m_offset, SEEK_SET) != 0) {
            LOG(ERROR) << "ReSyncLogCB seek binlog file " << _binlog_file_name << "fail..,errno:"
                        << errno << ",follower:" << shp_context->m_follower->my_addr;

            std::fclose(_f_handler);
            return _null_shp;
        }

        //Read protobuf buf length
        uint32_t _buf_len = 0;
        if (std::fread(&_buf_len,1,_FOUR_BYTES_,_f_handler) != _FOUR_BYTES_) {
            LOG(ERROR) << "ReSyncLogCB read binlog file " << _binlog_file_name << "fail..,errno:"
                        << errno << ",follower:" << shp_context->m_follower->my_addr;
            std::fclose(_f_handler);
            return _null_shp;
        }
        ::RaftCore::Tools::ConvertBigEndianToLocal<uint32_t>(_buf_len, &_buf_len);

        //Read protobuf buf
        _p_buf = (_p_buf) ? (unsigned char*)std::realloc(_p_buf,_buf_len): (unsigned char*)malloc(_buf_len);
        if ( std::fread(_p_buf,1,_buf_len, _f_handler) != _buf_len) {
            LOG(ERROR) << "ReSyncLogCB read binlog file " << _binlog_file_name << " fail..,errno:"
                        << errno <<  ",follower:" << shp_context->m_follower->my_addr;
            std::free(_p_buf);
            std::fclose(_f_handler);
            return _null_shp;
        }

        ::raft::BinlogItem _binlog_item;
        if (!_binlog_item.ParseFromArray(_p_buf,_buf_len)) {
            LOG(ERROR) << "ReSyncLogCB parse protobuf buffer fail " << _binlog_file_name << ",follower:"
                        << shp_context->m_follower->my_addr;
            std::free(_p_buf);
            std::fclose(_f_handler);
            return _null_shp;
        }

        auto _p_wop = _p_entry->mutable_write_op();
        _p_wop->set_key(_binlog_item.entity().write_op().key());
        _p_wop->set_value(_binlog_item.entity().write_op().value());
    }

    if (_p_buf)
        std::free(_p_buf);

    //VLOG(89) << "debug pos2" << ",leader sent resync log:" << _shp_req->DebugString();

    std::fclose(_f_handler);
    return _shp_req;
}

void LeaderView::AddRescynDataTask(std::shared_ptr<BackGroundTask::ReSyncLogContext> &shp_context) noexcept {
    //Prevent from duplicated task being executed.
    if (shp_context->m_follower->m_status == FollowerStatus::RESYNC_DATA) {
        LOG(INFO) << "a RESYNC_DATA task already in progress for follower:" << shp_context->m_follower->my_addr
            << ", no need to generate a new one, just return";
        return;
    }

    shp_context->m_follower->m_status = FollowerStatus::RESYNC_DATA;

    /*Here is synonymous to that , the leader is talking to the follower , and says : "Currently I don't have enough log entries to heal your
      log falling behind issue , you have to resync all the whole data , namely , starting the resync data procedure all over again. " */

    std::shared_ptr<BackGroundTask::SyncDataContenxt>   _shp_sync_data_ctx(new BackGroundTask::SyncDataContenxt(shp_context->m_follower));

    //Pass the callback function down through since SyncData will eventually need to all that ,too.
    _shp_sync_data_ctx->m_on_success_cb = shp_context->m_on_success_cb;

    int _ret_code = m_priority_queue.Push(LockFreePriotityQueue::TaskType::RESYNC_DATA, &_shp_sync_data_ctx);
    LOG(INFO) << "Add SYNC-DATA task bool ret:" << _ret_code << ",logID:" << shp_context->m_last_sync_point
        << ",follower:" << shp_context->m_follower->my_addr;
}

bool LeaderView::ReSyncLogCB(std::shared_ptr<BackGroundTask::ReSyncLogContext> &shp_context) noexcept{

    LOG(INFO) << "resync log background task received, peer:" << shp_context->m_follower->my_addr
            << ",last synced point:" << shp_context->m_last_sync_point;

    //Follower must in RESYNC_LOG state
    if (shp_context->m_follower->m_status != FollowerStatus::RESYNC_LOG) {
        LOG(WARNING) << "ReSyncLogCB follower " << shp_context->m_follower->my_addr << " is under "
                    << FollowerEntity::MacroToString(shp_context->m_follower->m_status) << " status,won't resync log to it";
        return false;
    }

    auto _shp_req = PrepareAppendEntriesRequest(shp_context);
    if (!_shp_req) {
        LOG(ERROR) << "PrepareAppendEntriesRequest got an empty result,probably due to a resync-data event happened,check it.";
        return false;
    }

    auto _shp_channel = shp_context->m_follower->m_shp_channel_pool->GetOneChannel();
    UnarySyncClient<AppendEntriesRequest, AppendEntriesResponse>  _sync_log_client(_shp_channel);

    auto _rpc = std::bind(&::raft::RaftService::Stub::AppendEntries, _sync_log_client.GetStub().get(),
        std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

    ::grpc::Status  _status;
    auto &_rsp = _sync_log_client.DoRPC([&](std::shared_ptr<AppendEntriesRequest>& req) {
        req = _shp_req; }, _rpc ,::RaftCore::Config::FLAGS_leader_resync_log_rpc_timeo_ms, _status);

    if (!_status.ok()) {
        LOG(ERROR) << "ReSyncLogCB AppendEntries fail,error code:" << _status.error_code()
            << ",err msg: " << _status.error_message() ;
        return false;
    }

    //const auto &_last_entity = _shp_req->replicate_entity(_shp_req->replicate_entity_size() - 1);

    ErrorCode  _error_code = _rsp.comm_rsp().result();
    if (_error_code!=ErrorCode::SUCCESS && _error_code!=ErrorCode::SUCCESS_MERGED) {
        if (_error_code != ErrorCode::APPEND_ENTRY_CONFLICT && _error_code != ErrorCode::WAITING_TIMEOUT
            && _error_code != ErrorCode::OVERSTEP_LCL) {
            LOG(ERROR) << "ReSyncLogCB AppendEntries fail,detail:" << _rsp.DebugString();
            return false;
        }

        if (_error_code == ErrorCode::OVERSTEP_LCL) {
            AddRescynDataTask(shp_context);
            return true;
        }

        //If still conflict, the task should be re-queued , no task could hold a thread for a long time.
        int _ret_code = m_priority_queue.Push(LockFreePriotityQueue::TaskType::RESYNC_LOG, &shp_context);
        LOG(INFO) << "Add RESYNC-LOG task ret:" << _ret_code << ",last synced point:"
            << shp_context->m_last_sync_point << ", remote peer:" << shp_context->m_follower->my_addr;

        return true;
    }

    //Reduce the use count for pre-lrl if currently holding one.
    if (shp_context->m_hold_pre_lcl) {
        shp_context->m_hold_pre_lcl = false;
        BinLogGlobal::m_instance.SubPreLRLUseCount();
    }

    //Reset follower status to NORMAL,allowing user threads to do normal AppendEntries RPC again.
    shp_context->m_follower->m_status = FollowerStatus::NORMAL;

    if (shp_context->m_on_success_cb)
        shp_context->m_on_success_cb(shp_context->m_follower);

    return true;
}

bool LeaderView::SyncLogAfterLCL(std::shared_ptr<BackGroundTask::SyncDataContenxt> &shp_context) {
    /*Note : If start syncing logs , never turn back , do it until finished. Two reasons :
            1. #logs which larger then ID-LCL is quite small.
            2. If turn back , could incur follower committing a log which already been synced in the data zone,
               this potential break the version sequence of the committed data.  */

    //Prepare sync log.
    std::list<std::shared_ptr<FileMetaData::IdxPair>> _file_meta;
    BinLogGlobal::m_instance.GetOrderedMeta(_file_meta);

    //Find the start syncing point.
    int _precede_lcl_counter = 0;   //count for num of exceeding the LCL.

    auto _iter_begin = _file_meta.cend();
    for (auto _iter = _file_meta.crbegin(); _iter != _file_meta.crend();++_iter) {

        if ((*_iter)->operator<(shp_context->m_last_sync))
            _precede_lcl_counter++;

        if ((*_iter)->operator>(shp_context->m_last_sync))
            continue;

        _iter_begin = _iter.base();
        break;
    }

    if (_iter_begin == _file_meta.cend()) {
        LOG(ERROR) << "SyncDataCB cannot find the sync log starting point";
        return false;
    }

    int _reserve_before_lcl = ::RaftCore::Config::FLAGS_binlog_reserve_log_num;
    CHECK(_precede_lcl_counter < _reserve_before_lcl) << "SyncDBAfter LCL fail,_precede_lcl_counter :"
               << _precede_lcl_counter << ",exceeds limit:" << _reserve_before_lcl;

    TypePtrFollowerEntity _shp_follower = shp_context->m_follower;
    auto _follower_addr = _shp_follower->my_addr;

    //Start syncing log.
    std::string _binlog_file_name = BinLogGlobal::m_instance.GetBinlogFileName();
    std::FILE* _f_handler = std::fopen(_binlog_file_name.c_str(),_AURORA_BINLOG_READ_MODE_);
    if (_f_handler == nullptr) {
        LOG(ERROR) << "ReSyncLogCB open binlog file " << _binlog_file_name << "fail..,errno:"
                    << errno << ",follower:" << _follower_addr;
        return false;
    }

    if (std::fseek(_f_handler, (*_iter_begin)->m_offset, SEEK_SET) != 0) {
        LOG(ERROR) << "ReSyncLogCB seek binlog file " << _binlog_file_name << "fail..,errno:"
                    << errno << ",follower:" << _follower_addr;
        std::fclose(_f_handler);
        return false;
    }

    int _rpc_counter = 0;
    unsigned char* _p_buf = nullptr;

    auto &_shp_client = shp_context->m_shp_client;
    auto _shp_stream = _shp_client->GetReaderWriter();
    auto* _rsp = _shp_client->GetResponse();

    auto _shp_req = _shp_client->GetInstantiatedReq();
    _shp_req->mutable_base()->set_term(ElectionMgr::m_cur_term.load());
    _shp_req->mutable_base()->set_addr(StateMgr::GetMyAddr());

    _shp_req->clear_entity();
    _shp_req->set_msg_type(::raft::SyncDataMsgType::SYNC_LOG);

    bool _sync_log_result = true;
    while (true) {

        _shp_req->clear_entity();
        bool _read_end = false;
        for (std::size_t i = 0; i < ::RaftCore::Config::FLAGS_resync_data_log_num_each_rpc; ++i) {

            uint32_t _buf_len = 0;
            if (std::fread(&_buf_len, _FOUR_BYTES_, 1, _f_handler) != 1) {
                LOG(ERROR) << "ReSyncLogCB read binlog file " << _binlog_file_name << "fail..,errno:"
                            << errno << ",follower:" << shp_context->m_follower->my_addr;
                _sync_log_result = false;
                break;
            }
            ::RaftCore::Tools::ConvertBigEndianToLocal<uint32_t>(_buf_len, &_buf_len);

            //Read protobuf buf
            _p_buf = (_p_buf) ? (unsigned char*)std::realloc(_p_buf,_buf_len): (unsigned char*)malloc(_buf_len);
            if (std::fread(_p_buf, _buf_len, 1, _f_handler) != 1) {
                LOG(ERROR) << "ReSyncLogCB read binlog file " << _binlog_file_name << "fail..,errno:"
                            << errno << ",follower:" << shp_context->m_follower->my_addr;
                _sync_log_result = false;
                break;
            }

            ::raft::BinlogItem _binlog_item;
            if (!_binlog_item.ParseFromArray(_p_buf,_buf_len)) {
                LOG(ERROR) << "ReSyncLogCB parse protobuf buffer fail " << _binlog_file_name << ",follower:"
                            << shp_context->m_follower->my_addr;
                _sync_log_result = false;
                break;
            }

            auto *_p_entity = _shp_req->add_entity();

            //TODO:figure out why this could resulting in a coredump in ~BinlogItem().
            //_p_entity->Swap(_binlog_item.mutable_entity());
            //_binlog_item.clear_entity();

            _p_entity->CopyFrom(_binlog_item.entity());

            if (!EntityIDSmaller(_p_entity->entity_id(), BinLogGlobal::m_instance.GetLastReplicated())) {
                _read_end = true;
                break;
            }
        }

        if (!_sync_log_result)
            break;

        if (!_shp_stream->Write(*_shp_req)) {
            LOG(ERROR) << "SyncDataCB send log fail,follower:" << shp_context->m_follower->my_addr << ",logID:" << shp_context->m_last_sync;
            _sync_log_result = false;
            break;
        }

        if (!_shp_stream->Read(_rsp)) {
            LOG(ERROR) << "SyncDataCB get prepare result fail,follower:" << shp_context->m_follower->my_addr
                        << ",logID:" << shp_context->m_last_sync;
            break;
        }

        if (_rsp->comm_rsp().result() != ErrorCode::SYNC_LOG_CONFRIMED) {
            LOG(ERROR) << "SyncDataCB prepare fail,follower:" << shp_context->m_follower->my_addr << ",logID:" << shp_context->m_last_sync;
            break;
        }

        if (_read_end)
            break;
    }

    std::fclose(_f_handler);
    if (_p_buf)
        std::free(_p_buf);

    if (!_shp_stream->WritesDone()) {
        LOG(ERROR) << "SyncDataCB send log WritesDone fail,follower:" << shp_context->m_follower->my_addr
                    << ",logID:" << shp_context->m_last_sync;
        return false;
    }

    shp_context->m_final_status = _shp_stream->Finish();

    if (!shp_context->m_final_status.ok()) {
        LOG(ERROR) << "SyncDataCB send log final status fail,follower:" << shp_context->m_follower->my_addr
            << ",logID:" << shp_context->m_last_sync << ",error_code:"
            << shp_context->m_final_status.error_code() << ",error_status:"
            << shp_context->m_final_status.error_message();
        return false;
    }

    if (!_sync_log_result)
        return false;

    return true;
}

bool LeaderView::ClientReactCB(std::shared_ptr<BackGroundTask::ClientReactContext> &shp_context) noexcept {
    void* _tag = shp_context->m_react_info.m_tag;
    ::RaftCore::Common::ReactBase* _p_ins = static_cast<::RaftCore::Common::ReactBase*>(_tag);
    _p_ins->React(shp_context->m_react_info.m_cq_result);

    return true;
}

bool LeaderView::SyncDataCB(std::shared_ptr<BackGroundTask::SyncDataContenxt> &shp_context) noexcept{

    TypePtrFollowerEntity _shp_follower = shp_context->m_follower;
    auto _follower_addr = _shp_follower->my_addr;

    LOG(INFO) << "sync data background task received,peer:" << _follower_addr;

    //Iterating over the storage , sync data to the follower in a batch manner.
    auto GetCurrentMS = []()->uint64_t{
        return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    };

    uint64_t _start_ts = GetCurrentMS();

    auto ScheduleNext = [&]()->bool{
        uint64_t _now = GetCurrentMS();
        if (_now - _start_ts <= ::RaftCore::Config::FLAGS_resync_data_task_max_time_ms)
            return false;

        int _ret_code = m_priority_queue.Push(LockFreePriotityQueue::TaskType::RESYNC_DATA, &shp_context);
        LOG(INFO) << "Add RESYNC_DATA task result:" << _ret_code << ",follower:" << _follower_addr
                        << ",logID:" << shp_context->m_last_sync;
        if (_ret_code != QUEUE_SUCC)
            return false;

        return true;
    };

    //After stream established , follower gets into its RPC interface and start waiting to read.
    auto &_shp_client = shp_context->m_shp_client;
    auto _shp_req = _shp_client->GetInstantiatedReq();
    _shp_req->mutable_base()->set_term(ElectionMgr::m_cur_term.load());
    _shp_req->mutable_base()->set_addr(StateMgr::GetMyAddr());

    auto _shp_stream = _shp_client->GetReaderWriter();
    auto* _rsp = _shp_client->GetResponse();

    if (shp_context->IsBeginning()) {

        _shp_req->set_msg_type(::raft::SyncDataMsgType::PREPARE);
        if (!_shp_stream->Write(*_shp_req)) {
            LOG(ERROR) << "SyncDataCB send prepare msg fail,follower:" << _follower_addr << ",logID:"
                        << shp_context->m_last_sync;
            return false;
        }

        VLOG(89) << " sync_data_debug PREPARE sent.";

        if (!_shp_stream->Read(_rsp)) {
            LOG(ERROR) << "SyncDataCB get prepare result fail,follower:" << _follower_addr
                        << ",logID:" << shp_context->m_last_sync;
            return false;
        }

        VLOG(89) << " sync_data_debug prepare received.";

        if (_rsp->comm_rsp().result() != ErrorCode::PREPARE_CONFRIMED) {
            LOG(ERROR) << "SyncDataCB prepare fail,follower:" << _follower_addr << ",logID:"
                        << shp_context->m_last_sync << ",result:" << _rsp->DebugString();
            return false;
        }
    }

    while (true) {
        _shp_req->clear_entity();
        _shp_req->set_msg_type(::raft::SyncDataMsgType::SYNC_DATA);

        std::list<StorageMgr::StorageItem>   _list;
        StorageGlobal::m_instance.GetSlice(shp_context->m_last_sync,::RaftCore::Config::FLAGS_resync_data_item_num_each_rpc,_list);

        if (_list.empty()) {
            VLOG(89) << "list empty after GetSlice";
            break;
        }

        for (const auto &_item : _list) {

            auto *_p_entity = _shp_req->add_entity();
            auto *_p_wop    = _p_entity->mutable_write_op();

             //Ownership of the following two can be taken over.
            _p_wop->set_allocated_key(_item.m_key.get());
            _p_wop->set_allocated_value(_item.m_value.get());

            auto _p_entity_id = _p_entity->mutable_entity_id();
            _p_entity_id->set_term(_item.m_log_id.m_term);
            _p_entity_id->set_idx(_item.m_log_id.m_index);
        }

        bool _rst = _shp_stream->Write(*_shp_req);

        //Release the allocated write_op first.
        for (int i = 0; i < _shp_req->entity_size(); ++i) {
            auto *_p_wop = _shp_req->mutable_entity(i)->mutable_write_op();
            _p_wop->release_key();
            _p_wop->release_value();
        }

        if (!_rst) {
            LOG(ERROR) << "SyncDataCB send data fail,follower:" << _follower_addr << ",logID:"
                       << shp_context->m_last_sync;
            return false;
        }

        if (!_shp_stream->Read(_rsp)) {
            LOG(ERROR) << "SyncDataCB get prepare result fail,follower:" << _follower_addr
                        << ",logID:" << shp_context->m_last_sync;
            return false;
        }

        if (_rsp->comm_rsp().result() != ErrorCode::SYNC_DATA_CONFRIMED) {
            LOG(ERROR) << "SyncDataCB prepare fail,follower:" << _follower_addr << ",logID:"
                       << shp_context->m_last_sync;
            return false;
        }

        //Update last synced storage data item.
        /*TODO: Prevent from losing data(a rare case) when using the 'm_last_sync' as the task restart
            point since the order is not strictly guaranteed among sstables.*/
        shp_context->m_last_sync.Set(_list.back().m_log_id);

        //Return if successfully push the task again to the queue.
        if (ScheduleNext())
            return true;

        //Means there are no more data items to be synced due to 'GetSlice'.
        if (int(_list.size()) < ::RaftCore::Config::FLAGS_resync_data_item_num_each_rpc) {
            VLOG(89) << "list num less than required after GetSlice.";
            break;
        }
    }

    //Re-check if current thread timed out.
    if (ScheduleNext())
        return true;

    LOG(INFO) << "SYNC_DATA end, start sync log after lrl.";

    bool _resync_log_rst = SyncLogAfterLCL(shp_context);
    if (_resync_log_rst && shp_context->m_on_success_cb)
        shp_context->m_on_success_cb(_shp_follower);

    //Reset follower status to NORMAL,allow user threads to do normal AppendEntries RPC.
    shp_context->m_follower->m_status = FollowerStatus::NORMAL;

    return true;
}

void LeaderView::ClientThreadReacting(const ReactInfo &info) noexcept {

    std::shared_ptr<ReactInfo> _shp_task(new ReactInfo(info));

    int _ret_code = LeaderView::m_priority_queue.Push(LockFreePriotityQueue::TaskType::CLIENT_REACTING, &_shp_task);
    if (_ret_code != QUEUE_SUCC)
        LOG(ERROR) << "Add CLIENT_REACTING task fail,ret:" << _ret_code;
}


}
