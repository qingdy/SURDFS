#include <vector>

#include "block_replicate.h"
#include "layout_manager.h"
#include "blade_meta_manager.h"
#include "blade_nameserver_param.h"
#include "blade_net_util.h"
#include "blade_common_data.h"
#include "stat_manager.h"

namespace bladestore
{
namespace nameserver
{
//ReplicateExecutor part
ReplicateExecutor::ReplicateExecutor(MetaManager & meta_manager) : meta_manager_(meta_manager)
{
	replicate_strategy_ = NULL;
}

ReplicateExecutor::~ReplicateExecutor()
{
	clear_all();
	if (NULL != replicate_strategy_)
	{
		delete replicate_strategy_;
	}
	replicate_strategy_ = NULL;
}

void ReplicateExecutor::clear_all()
{
	CWLockGuard write_lock_guard(rw_mutex_);
	REPL_BLOCK_MAP_ITER iter = replicating_map_.begin();
	for (; iter != replicating_map_.end(); iter++)
	{
		delete iter->second;	
	}
	replicating_map_.clear();
}

void ReplicateExecutor::clear_replicating_info()
{
	CWLockGuard write_lock_guard(rw_mutex_, true);
	replicating_map_.clear();
	source_counter_.clear();
	dest_counter_.clear();	
}

int ReplicateExecutor::Complete(uint64_t dest_id, int64_t block_id, bool success)
{
	int ret = BLADE_ERROR;
	
	BladeLayoutManager & layout_manager = meta_manager_.get_layout_manager();
	if (success)
	{
		DataServerInfo * dest_dataserver_info = layout_manager.get_dataserver_info(dest_id);	
		if (NULL != dest_dataserver_info)
			layout_manager.build_block_ds_relation(block_id, dest_id);
		ret = BLADE_SUCCESS;
	}
	rw_mutex_.wlock()->lock();
	check(block_id, BladeNameServerParam::replicate_max_time_, true, false);
	rw_mutex_.wlock()->unlock();
	return ret;
}

int ReplicateExecutor::Execute(int64_t block_id, void * args)
{
	BladeLayoutManager & layout_manager = meta_manager_.get_layout_manager();
	layout_manager.blocks_mutex_.rlock()->lock();
	BladeBlockCollect * block_collect = layout_manager.get_block_collect(block_id);

	LOGV(LL_DEBUG, "IN REPLICATE CHECK DS : %ld ", block_id);
	if (NULL == block_collect)
	{
		LOGV(LL_ERROR, "block:%d not found", block_id);
		layout_manager.blocks_mutex_.rlock()->unlock();
        AtomicInc64(&Singleton<StatManager>::Instance().copy_error_times_);
		return BLADE_ERROR;
	}

	if (meta_manager_.has_valid_lease(block_id))
	{
		LOGV(LL_ERROR, "block:%d has valid write lease", block_id);
		layout_manager.blocks_mutex_.rlock()->unlock();
        AtomicInc64(&Singleton<StatManager>::Instance().copy_error_times_); 
		return BLADE_ERROR;
	}

	std::set<uint64_t> ds_list = block_collect->dataserver_set_;
	if (ds_list.size() < 1)
	{
		LOGV(LL_DEBUG, "DS SIZE IS NOT ENOUGH");
		layout_manager.blocks_mutex_.rlock()->unlock();
        AtomicInc64(&Singleton<StatManager>::Instance().copy_error_times_);   
		return BLADE_ERROR;
	}

	int32_t block_version = block_collect->version_;
	//unlock
	layout_manager.blocks_mutex_.rlock()->unlock();


	int32_t current_ds_size = static_cast<uint32_t>(ds_list.size());
	if (0 == current_ds_size)
	{
		LOGV(LL_ERROR, "no replicate, all data lost , this should not happen");
        AtomicInc64(&Singleton<StatManager>::Instance().copy_error_times_);   
		return BLADE_ERROR;
	}
	
	if (current_ds_size >= layout_manager.get_ds_size())
	{
		LOGV(LL_WARN, "current_ds_size: %d >= total ds size: %d", current_ds_size, layout_manager.get_ds_size());
        AtomicInc64(&Singleton<StatManager>::Instance().copy_error_times_);   
		return BLADE_SUCCESS;
	}

	rw_mutex_.wlock()->lock();
	int32_t ret = check(block_id, BladeNameServerParam::replicate_max_time_, false, true);
	rw_mutex_.wlock()->unlock();
	
	if (0 == ret)
	{
		LOGV(LL_ERROR, "block : %d, is being replicated", block_id);
		return BLADE_ERROR;
	}

	uint64_t source_id = *ds_list.begin();
	srand(time(NULL));
	int pos = rand() % (ds_list.size());
	std::set<uint64_t>::iterator iter = ds_list.begin();
	for (; iter != ds_list.end() && pos >= 0; pos--, iter++)
	{
		source_id = *iter;
	}
	
	rw_mutex_.rlock()->lock();
	int32_t replicating_count = source_counter_[source_id];
	rw_mutex_.rlock()->unlock(); 
	
	if (replicating_count > BladeNameServerParam::replicate_max_count_per_server_)
	{
		LOGV(LL_ERROR, "data server : %s replicate as source count : %d > BladeNameserverParam::replicate_max_count_per_server", BladeNetUtil::AddrToString(source_id).c_str() , replicating_count);				
		AtomicInc64(&Singleton<StatManager>::Instance().copy_error_times_);   
        return BLADE_ERROR;
	}

	//select replicate dest ds
	if (NULL == replicate_strategy_)
	{
		replicate_strategy_ = new ReplicateStrategy(meta_manager_.get_layout_manager());
	}

	std::vector<uint64_t> result_ds;
	std::vector<uint64_t> exclude_ds;
	for (iter = ds_list.begin(); iter != ds_list.end(); iter++)
	{
		exclude_ds.push_back(*iter);
	}
	replicate_strategy_->choose_target(1, source_id, exclude_ds, result_ds);
	
	if (result_ds.size() < 1)
	{
        AtomicInc64(&Singleton<StatManager>::Instance().copy_error_times_);
		return BLADE_ERROR;
	}

	if (dest_counter_[result_ds[0]] > BladeNameServerParam::replicate_max_count_per_server_)
	{
		LOGV(LL_ERROR, "data server : %s replicate as destcount : %d > BladeNameserverParam::replicate_max_count_per_server", BladeNetUtil::AddrToString(result_ds[0]).c_str() , replicating_count);				
	    AtomicInc64(&Singleton<StatManager>::Instance().copy_error_times_);
        return BLADE_ERROR;
    }
	//inert into replicate map
	rw_mutex_.wlock()->lock();
	ReplicateTask * replicate_task = new ReplicateTask();
	replicate_task->block_id_ = block_id;
	replicate_task->source_id_ = source_id;
	replicate_task->dest_id_ = result_ds[0];
	replicate_task->start_time_ = time(NULL);
	source_counter_[source_id]++;
	dest_counter_[result_ds[0]]++;
	replicating_map_[block_id] = replicate_task;	
	rw_mutex_.wlock()->unlock();

	LOGV(LL_DEBUG, "add to replicate block_id : %ld, src ip: %s, dest ip : %s", block_id, BladeNetUtil::AddrToString(source_id).c_str(), BladeNetUtil::AddrToString(result_ds[0]).c_str());
	meta_manager_.add_replicate_info(block_id, block_version, source_id, result_ds[0]);
    AtomicInc64(&Singleton<StatManager>::Instance().copy_times_);
	return BLADE_SUCCESS;
}

//ReplicateLauncher part
ReplicateLauncher::ReplicateLauncher(MetaManager & meta_manager) : meta_manager_(meta_manager), replicate_executor_(meta_manager)
{
	replicate_thread_.set_thread_parameter(&replicate_executor_, 1, this);
	replicate_thread_.Start();
}

ReplicateLauncher::~ReplicateLauncher()
{

}

int ReplicateLauncher::Check(int64_t block_id)
{
	meta_manager_.get_layout_manager().blocks_mutex_.rlock()->lock();
	BLOCKS_MAP_ITER blocks_map_iter = meta_manager_.get_layout_manager().blocks_map_.find(block_id);
	if (meta_manager_.get_layout_manager().blocks_map_.end() == blocks_map_iter)
	{
		meta_manager_.get_layout_manager().blocks_mutex_.rlock()->unlock();
		return SCANNER_NEXT;
	}

	BladeBlockCollect * block_collect = blocks_map_iter->second;	
	uint32_t ds_size = static_cast<uint32_t>(block_collect->dataserver_set_.size());
	int16_t replicas_num = block_collect->replicas_number_;	
	meta_manager_.get_layout_manager().blocks_mutex_.rlock()->unlock();

	if (ds_size < replicas_num)
	{
        LOGV(LL_DEBUG, "blockid :%d , expect replicas : %d , but real replicas :%d ! ", block_id, replicas_num, ds_size);
		replicate_thread_.Push(block_id);
		return SCANNER_CONTINUE;
	}
	else if (ds_size == replicas_num)
	{
		return SCANNER_CONTINUE;
	}

	return SCANNER_NEXT;
}

int ReplicateLauncher::build_blocks(const std::set<int64_t> & lose_block_set)
{
	return 0;	
}

int ReplicateLauncher::handle_replicate_complete(uint64_t ds_id, int64_t block_id)
{
	int ret = BLADE_ERROR;
	ret = replicate_executor_.Complete(ds_id, block_id, true);
	return ret;
}

}//end of namespace nameserver
}//end of namespace bladestore
