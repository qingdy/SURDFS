#include "blade_log_replay_runnable.h"
#include "log.h"

using namespace pandora;

namespace bladestore
{
namespace ha
{

BladeLogReplayRunnable::BladeLogReplayRunnable()
{
	stop_ = false;
	is_initialized_ = false;
	thread_ = new CThread();
}

BladeLogReplayRunnable::~BladeLogReplayRunnable()
{
	if(NULL != thread_)
	{
		delete thread_;
		thread_ = NULL;
	}
}

int BladeLogReplayRunnable::Init(const char* log_dir, const uint64_t log_file_id_start, BladeRoleMgr *role_mgr, int64_t replay_wait_time)
{
	int ret = BLADE_SUCCESS;

	if (is_initialized_)
	{
		LOGV(LL_ERROR, "BladeLogReplayRunnable has been initialized");
		ret = BLADE_INIT_TWICE;
	}

	if (BLADE_SUCCESS == ret)
	{
		if (NULL == role_mgr)
		{
			LOGV(LL_ERROR, "Parameter is invalid[role_mgr=%p]", role_mgr);
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		ret = log_reader_.Init(log_dir, log_file_id_start, true);
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "BladeLogReader init error[ret=%d], BladeLogReplayRunnable init failed", ret);
		}
		else
		{
			role_mgr_ = role_mgr;
			replay_wait_time_ = replay_wait_time;
			SetMaxLogFileId(0);
			is_initialized_ = true;
		}
	}

	return ret;
}

void BladeLogReplayRunnable::Run(CThread* thread, void* arg)
{
	int ret = BLADE_SUCCESS;

	UNUSED(thread);
	UNUSED(arg);

	char* log_data = NULL;
	int64_t data_len = 0;
	LogCommand cmd = BLADE_LOG_UNKNOWN;
	uint64_t seq;

	if (!is_initialized_)
	{
		LOGV(LL_ERROR, "BladeLogReplayRunnable has not been initialized");
		ret = BLADE_NOT_INIT;
	}
	else
	{
		while (!stop_)
		{
			ret = log_reader_.ReadLog(cmd, seq, log_data, data_len);
			if (BLADE_READ_NOTHING == ret)
			{
				if (BladeRoleMgr::MASTER == role_mgr_->GetRole())
				{
					Stop();
				}
				else
				{
					usleep(replay_wait_time_);
				}
				continue;
			}
			else if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "BladeLogReader read_data error[ret=%d]", ret);
				break;
			}
			else
			{
				if (BLADE_LOG_NOP != cmd)
				{
					ret = Replay(cmd, seq, log_data, data_len);
					if (BLADE_SUCCESS != ret)
					{
						LOGV(LL_ERROR, "replay log error[ret=%d]", ret);
						hex_dump(log_data, data_len, false, LL_ERROR);
						break;
					}
				}
			}
		}
	}

	// stop server
	if (NULL != role_mgr_) // double check
	{
		if (BLADE_SUCCESS != ret)
		{
			role_mgr_->SetState(BladeRoleMgr::ERROR);
		}
	}

	LOGV(LL_INFO, "BladeLogReplayRunnable finished[stop=%d ret=%d]", stop_, ret);
}

void BladeLogReplayRunnable::Start()
{
	thread_->Start(this, NULL);
}

void BladeLogReplayRunnable::Wait()
{
	assert(NULL != thread_);
	thread_->Join();
}

void BladeLogReplayRunnable::Stop()
{
	stop_ = true;
}

}//end of namepace ha
}//end of namespace bladestore
