/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_HA_FETCH_RUNNABLE_H
#define BLADESTORE_HA_FETCH_RUNNABLE_H

#include <stdint.h>
#include <vector>
#include <string>

#include "blade_role_mgr.h"
#include "blade_common_define.h"
#include "blade_log_replay_runnable.h"
#include "cthread.h"
#include "blade_fetch_param.h"

using namespace bladestore::common;
using namespace pandora;
using namespace std;

namespace bladestore
{
namespace ha
{
/// Slave获取Master机器上的日志和checkpoint
/// 用rsync实现相应远程获取功能
/// 需要部署时保证
///     1. Slave进程和Master进程使用相同的账户启动
///     2. Slave进程的工作目录和Master进程工作目录是完全相同的
///     3. Slave机器和Master机器之间建立好信任关系
class BladeFetchRunnable : public Runnable 
{
public:
	static const int64_t DEFAULT_LIMIT_RATE = 1L << 14; // 16 * 1024 KBps
	static const int DEFAULT_FETCH_RETRY_TIMES = 3;
	static const char * DEFAULT_FETCH_OPTION;

public:
	BladeFetchRunnable();
	virtual ~BladeFetchRunnable();
	virtual int Init(std::string master, const char* log_dir, const BladeFetchParam &param, BladeRoleMgr *role_mgr, BladeLogReplayRunnable *replay_thread);

	virtual void Run(CThread* thread, void* arg);
	virtual void Clear();

	virtual int SetFetchParam(const BladeFetchParam& param);

	virtual int AddCkptExt(const char* ext);

	virtual int GotCkpt(uint64_t ckpt_id);
	virtual int GotLog(uint64_t log_id);

	virtual int SetUsrOpt(const char* opt);

	inline void SetLimitRate(const int64_t new_limit)
	{
		limit_rate_ = new_limit;
	}
	inline int64_t GetLimitRate()
	{
		return limit_rate_;
	}

	void Start();	
	void Wait();
	void Stop()
	{
		stop_ = true;
	}

protected:
	int GenFetchCmd(const uint64_t id, const char* fn_ext, char* cmd, const int64_t size);

	bool Exists(const uint64_t id, const char* fn_ext) const;

	bool Remove(const uint64_t id, const char* fn_ext) const;

	int GenFullName(const uint64_t id, const char* fn_ext, char *buf, const int buf_len) const;

	virtual int GetLog();
	virtual int GetCkpt();

protected:
	typedef std::vector<std::string> CkptList;
	typedef CkptList::iterator CkptIter;

	bool stop_;
	CThread * thread_;
	BladeRoleMgr *role_mgr_;
	BladeLogReplayRunnable *replay_thread_;
	int64_t limit_rate_;
	BladeFetchParam param_;
	CkptList ckpt_ext_;
	std::string vip_str_;
	bool is_initialized_;
	char cwd_[BLADE_MAX_FILENAME_LENGTH];
	char log_dir_[BLADE_MAX_FILENAME_LENGTH];
	char *usr_opt_;
};

}//end of namespace ha 
}//end of namespace bladestore

#endif

