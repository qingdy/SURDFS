/*
 *version : 1.0
 *author  : chen yunyun, funing
 *date    : 2012-4-11
 *
 */
#ifndef BLADESTORE_NAMESERVER_REPLICATE_H
#define BLADESTORE_NAMESERVER_REPLICATE_H
#include <ext/hash_map>

#include "block_scanner_manager.h"
#include "blade_nameserver_param.h"
#include "replicate_strategy.h"
#include "proactor_pipe.h"
#include "blade_rwlock.h"
#include "blade_common_define.h"

#ifdef GTEST
#define private public
#define protected public
#endif

using namespace bladestore::common;
using namespace pandora;

namespace bladestore
{
namespace nameserver
{

class MetaManager;

class ReplicateTask
{
public:
	int64_t block_id_;
	uint64_t source_id_;
	uint64_t dest_id_;
	bool is_move_;
	time_t start_time_;
};

//管理block副本,为HeartbeatManager服务
class ReplicateExecutor
{
public:
	typedef __gnu_cxx::hash_map<int64_t, ReplicateTask * > REPL_BLOCK_MAP;
	typedef REPL_BLOCK_MAP::iterator REPL_BLOCK_MAP_ITER;
	typedef __gnu_cxx::hash_map<uint64_t, int32_t > DS_COUNTER;

public:
	ReplicateExecutor(MetaManager & meta_manager);

	~ReplicateExecutor();

	int Execute(int64_t block_id, void * args);

	int Complete(uint64_t ds_id, int64_t block_id, bool success);


	REPL_BLOCK_MAP & get_replicating_map() 
	{
		return replicating_map_;
	}

	bool is_replicate_block(int64_t block_id)
	{
		CRLockGuard read_lock_guard(rw_mutex_, true);	
		return (check(block_id, BladeNameServerParam::replicate_max_time_, false, false) == 0);
	}

private:
	void clear_all();
	void clear_replicating_info();
	bool is_replicate_block(ReplicateTask * repl_task, int32_t count)
	{
		CWLockGuard write_lock_guard(rw_mutex_, true);
		int ret = check(repl_task->block_id_, BladeNameServerParam::replicate_max_time_, false, true);
		if (-1 == ret)
		{
			LOGV(LL_WARN, "block  : %d not found", repl_task->block_id_);
			return false;
		}
		bool i_ret = (source_counter_[repl_task->block_id_] >= count || dest_counter_[repl_task->block_id_] >= count);
		return i_ret;
	}

	int check(int64_t block_id, const time_t max_time, bool complete, bool timeout)
	{
		bool is_timeout = true;
		REPL_BLOCK_MAP_ITER iter = replicating_map_.find(block_id);
		if (iter != replicating_map_.end())
		{
			is_timeout = iter->second->start_time_ < (time(NULL) - max_time);
			if (complete || (is_timeout && timeout))
			{
				source_counter_[iter->second->source_id_]--;	
				dest_counter_[iter->second->dest_id_]--;
				delete iter->second;
				replicating_map_.erase(iter);
			}
			return is_timeout ? 1 : 0;
		}
		return -1;
	}

private:
	CRWLock rw_mutex_;
	MetaManager & meta_manager_;
	ReplicateStrategy * replicate_strategy_;

	REPL_BLOCK_MAP  replicating_map_;
	//复制源&&目的 负载情况
	DS_COUNTER source_counter_;
	DS_COUNTER dest_counter_;
};

class ReplicateLauncher : public Launcher
{
public:
	ReplicateLauncher(MetaManager & meta_manager);

	~ReplicateLauncher();

	int handle_replicate_complete(uint64_t ds_id, int64_t block_id);

	void set_pause_flag();

	int Check(int64_t block_id);

	int build_blocks(const std::set<int64_t> & block_list);
	
	void Initialize();

	void Destroy();

private:
	MetaManager & meta_manager_;

	ReplicateExecutor replicate_executor_;

	int8_t pause_flag_;

	ProactorDataPipe<std::deque<int64_t>, ReplicateExecutor> replicate_thread_;
};

}//end namespace of nameserver
}//end namespace of bladestore
#endif
