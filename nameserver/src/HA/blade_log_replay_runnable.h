/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_HA_LOG_REPLAY_RUNNABLE_H
#define BLADESTORE_HA_LOG_REPLAY_RUNNABLE_H

#include "cthread.h"
#include "blade_role_mgr.h"
#include "blade_log_reader.h"
#include "utility.h"

using namespace pandora;
using namespace bladestore::common;

namespace bladestore
{
namespace ha
{

/// 回放日志的线程代码
/// BladeLogReplayRunnable中读取日志数据, 并且调用replay虚函数进行回放
class BladeLogReplayRunnable : public Runnable
{
public:
	BladeLogReplayRunnable();
	virtual ~BladeLogReplayRunnable();
	virtual int Init(const char * log_dir, const uint64_t log_file_id_start, BladeRoleMgr *role_mgr, int64_t replay_wait_time);

	virtual void Run(CThread * thread, void * arg);
	virtual void SetMaxLogFileId(uint64_t max_log_file_id)
	{
		log_reader_.SetMaxLogFileId(max_log_file_id);
	}
	virtual void SetHasNoMax()
	{
		log_reader_.SetHasNoMax();
	}
	
	void Start();
	void Wait();
	void Stop();

protected:
	//nameserver 需要实现的接口
	virtual int Replay(LogCommand cmd, uint64_t seq, char* log_data, const int64_t data_len) = 0;

protected:
	bool stop_;
	CThread * thread_;
	int64_t replay_wait_time_;
	BladeRoleMgr *role_mgr_;
	BladeLogReader log_reader_;
	bool is_initialized_;
};

}//end of namespace ha
}//end of namespace bladestore
#endif
