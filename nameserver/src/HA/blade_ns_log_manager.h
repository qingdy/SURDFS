/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_HA_NS_LOG_MGR_H
#define BLADESTORE_HA_NS_LOG_MGR_H
#include <stdint.h>

#include "thread_mutex.h"
#include "nameserver_impl.h"
#include "blade_log_writer.h"
#include "blade_common_define.h"
#include "blade_ns_log_worker.h"

using namespace pandora;
using namespace bladestore::nameserver;
using namespace bladestore::common;

namespace bladestore
{
namespace ha
{

class BladeNsLogManager : public BladeLogWriter
{
public:
	static const int UINT64_MAX_LEN;
	BladeNsLogManager();
	~BladeNsLogManager();
	int Init(NameServerImpl * nameserver, BladeSlaveMgr* slave_mgr);

	/// replay all commit log from replay point
	/// after initialization, invoke this method to replay all commit log
	int ReplayLog();

	int DoAfterRecoverCheckPoint();
	int RecoverCheckpoint(uint64_t ckpt_id);

	int LoadServerStatus();

	int DoCheckPoint(const uint64_t checkpoint_id = 0);

	int AddSlave(const uint64_t & server, uint64_t &new_log_file_id);

	const char* GetLogDirPath() const 
	{ 
		return log_dir_; 
	}
	inline uint64_t GetReplayPoint() 
	{
		return replay_point_;
	}
	inline uint64_t GetCheckPoint() 
	{
		return ckpt_id_;
	}
	BladeNsLogWorker* GetLogWorker() 
	{ 
		return &log_worker_; 
	}
	CThreadMutex* GetLogSyncMutex() 
	{ 
		return &log_sync_mutex_; 
	}

private:
	uint64_t ckpt_id_;
	uint64_t replay_point_;
	uint64_t max_log_id_;
	int rt_server_status_;
	bool is_initialized_;
	bool is_log_dir_empty_;
	char log_dir_[BLADE_MAX_FILENAME_LENGTH];

	NameServerImpl * nameserver_;
	BladeNsLogWorker log_worker_;
	CThreadMutex log_sync_mutex_;
};

}//end of namespace ha
}//end of namespace bladestore

#endif

