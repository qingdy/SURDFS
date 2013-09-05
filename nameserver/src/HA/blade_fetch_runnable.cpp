#include <errno.h>

#include "blade_fetch_runnable.h"
#include "blade_net_util.h"
#include "log.h"

using namespace pandora;

namespace bladestore
{
namespace ha
{

const char* BladeFetchRunnable::DEFAULT_FETCH_OPTION = "-e \"ssh -oStrictHostKeyChecking=no\" -avz --inplace";


BladeFetchRunnable::BladeFetchRunnable()
{
	limit_rate_ = DEFAULT_LIMIT_RATE;
	param_.min_log_id_ = 0;
	param_.max_log_id_ = 0;
	param_.ckpt_id_ = 0;
	param_.fetch_log_ = 0;
	param_.fetch_ckpt_ = 0;
	is_initialized_ = false;
	stop_ = false;
	cwd_[0] = '\0';
	log_dir_[0] = '\0';
	role_mgr_ = NULL;
	replay_thread_ = NULL;
	thread_ = new CThread();
}

void BladeFetchRunnable::Start()
{
	thread_->Start(this, NULL);
}


void BladeFetchRunnable::Wait()
{
	thread_->Join();
}

BladeFetchRunnable::~BladeFetchRunnable()
{
	if(NULL != thread_)
	{
		delete thread_;
		thread_ = NULL;
	}
}

void BladeFetchRunnable::Run(CThread* thread, void* arg)
{
	int ret = BLADE_SUCCESS;

	UNUSED(thread);
	UNUSED(arg);

	if (!is_initialized_)
	{
		LOGV(LL_ERROR, "BladeFetchRunnable has not been initialized");
		ret = BLADE_NOT_INIT;
	}

	if (BLADE_SUCCESS == ret)
	{
		if (!stop_ && param_.fetch_ckpt_)
		{
			ret = GetCkpt();
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "get_ckpt_ error, ret=%d", ret);
			}
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		if (!stop_ && param_.fetch_log_)
		{
			ret = GetLog();
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "get_log_ error, ret=%d", ret);
			}
		}
	}

	if (BLADE_SUCCESS != ret)
	{
		if (NULL != role_mgr_) // double check
		{
			role_mgr_->SetState(BladeRoleMgr::ERROR);
		}
	}
	else
	{
		if (NULL == replay_thread_)
		{
			ret = BLADE_ERROR;
		}
		else
		{
			replay_thread_->SetHasNoMax();
		}
	}

	LOGV(LL_INFO, "BladeFetchRunnable finished[stop=%d ret=%d]", stop_, ret);
}

void BladeFetchRunnable::Clear()
{
	if(NULL != thread_)
	{
		delete thread_;
		thread_ = NULL;
	}
}

int BladeFetchRunnable::Init(std::string master, const char* log_dir, const BladeFetchParam &param, BladeRoleMgr *role_mgr, BladeLogReplayRunnable *replay_thread)
{
	int ret = 0;

	int log_dir_len = 0;
	if (is_initialized_)
	{
		LOGV(LL_ERROR, "BladeFetchRunnable has been initialized");
		ret = BLADE_INIT_TWICE;
	}
	else
	{
		if (NULL == log_dir || NULL == role_mgr || NULL == replay_thread)
		{
			LOGV(LL_ERROR, "Parameter are invalid[log_dir=%p role_mgr=%p replay_thread=%p]", log_dir, role_mgr, replay_thread);
			ret = BLADE_INVALID_ARGUMENT;
		}
		else
	    {
			log_dir_len = strlen(log_dir);
			if (log_dir_len >= BLADE_MAX_FILENAME_LENGTH)
			{
				LOGV(LL_ERROR, "Parameter is invalid[log_dir_len=%d log_dir=%s]", log_dir_len, log_dir);
				ret = BLADE_INVALID_ARGUMENT;
			}
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		usr_opt_ = (char*)malloc(BLADE_MAX_FETCH_CMD_LENGTH);
		if (NULL == usr_opt_)
		{
			LOGV(LL_ERROR, "malloc for usr_opt_ error, size=%ld", BLADE_MAX_FETCH_CMD_LENGTH);
			ret = BLADE_ERROR;
		}
		else
		{
			ret = SetUsrOpt(DEFAULT_FETCH_OPTION);
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "set default user option error, DEFAULT_FETCH_OPTION=%s ret=%d", DEFAULT_FETCH_OPTION, ret);
			}
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		strncpy(log_dir_, log_dir, log_dir_len);
		log_dir_[log_dir_len] = '\0';
		vip_str_ = master;
		param_ = param;
		role_mgr_ = role_mgr;
		replay_thread_ = replay_thread;

		is_initialized_ = true;
	}

	return ret;
}

int BladeFetchRunnable::SetFetchParam(const BladeFetchParam& param)
{
	int ret = BLADE_SUCCESS;

	param_ = param;

	return ret;
}

int BladeFetchRunnable::AddCkptExt(const char * ext)
{
	int ret = BLADE_SUCCESS;

	if (NULL == ext)
	{
		ret = BLADE_INVALID_ARGUMENT;
		LOGV(LL_ERROR, "Paramter is invalid[ext=NULL]");
	}
	else
	{
		string new_ext;
		new_ext.assign(ext);
		for (CkptIter i = ckpt_ext_.begin(); i != ckpt_ext_.end(); i++)
		{
			if (new_ext == *i)
			{
				ret = BLADE_ERROR;
				LOGV(LL_WARN, "\"%s\" exists", ext);
				break;
			}
		}

		if (BLADE_SUCCESS == ret)
		{
			ckpt_ext_.push_back(new_ext);
		}
	}

	return ret;
}

int BladeFetchRunnable::GotCkpt(uint64_t ckpt_id)
{
	UNUSED(ckpt_id);
	return BLADE_SUCCESS;
}

int BladeFetchRunnable::GotLog(uint64_t log_id)
{
	int ret = BLADE_SUCCESS;

	if (NULL == replay_thread_)
	{
		ret = BLADE_ERROR;
	}
	else
	{
		replay_thread_->SetMaxLogFileId(log_id);
	}

	return ret;
}

int BladeFetchRunnable::SetUsrOpt(const char* opt)
{
	int ret = BLADE_SUCCESS;

	int opt_len = strlen(opt);
	if (opt_len >= BLADE_MAX_FETCH_CMD_LENGTH)
	{
		LOGV(LL_WARN, "usr_option is too long, opt_len=%d maximum_length=%ld", opt_len, BLADE_MAX_FETCH_CMD_LENGTH);
		ret = BLADE_BUF_NOT_ENOUGH;
	}
	else
	{
		strncpy(usr_opt_, opt, opt_len);
		usr_opt_[opt_len] = '\0';
	}

	return ret;
}

int BladeFetchRunnable::GenFetchCmd(const uint64_t id, const char* fn_ext, char* cmd, const int64_t size)
{
	int ret = BLADE_SUCCESS;

	int err = 0;
	const int MAX_FETCH_OPTION_SIZE = 128;
	char fetch_option[MAX_FETCH_OPTION_SIZE];
	char full_log_dir[BLADE_MAX_FILENAME_LENGTH];

	if (NULL == cmd || size <= 0)
	{
		LOGV(LL_ERROR, "Parameters are invalid[id=%lu fn_ext=%s cmd=%p size=%ld", id, fn_ext, cmd, size);
		ret = BLADE_INVALID_ARGUMENT;
	}

	if (BLADE_SUCCESS == ret)
	{
		if ('\0' == cwd_[0])
		{
			if (NULL == getcwd(cwd_, BLADE_MAX_FILENAME_LENGTH))
			{
				LOGV(LL_ERROR, "getcwd error[%s]", strerror(errno));
				ret = BLADE_ERROR;
			}
		}
	}

	// gen log directory
	if (BLADE_SUCCESS == ret)
	{
		if (log_dir_[0] == '/')
		{
			err = snprintf(full_log_dir, BLADE_MAX_FILENAME_LENGTH, "%s", log_dir_);
		}
		else
		{
			err = snprintf(full_log_dir, BLADE_MAX_FILENAME_LENGTH, "%s/%s", cwd_,  log_dir_);
		}

		if (err < 0)
		{
			LOGV(LL_ERROR, "snprintf full_log_dir[BLADE_MAX_FILENAME_LENGTH=%ld err=%d] error[%s]", BLADE_MAX_FILENAME_LENGTH, err, strerror(errno));
			ret = BLADE_ERROR;
		}
		else if (err >= BLADE_MAX_FILENAME_LENGTH)
		{
			LOGV(LL_ERROR, "full_log_dir buffer is not enough[BLADE_MAX_FILENAME_LENGTH=%ld err=%d]", BLADE_MAX_FILENAME_LENGTH, err);
			ret = BLADE_ERROR;
		}
	}

	// get master address and generate fetch option
	if (BLADE_SUCCESS == ret)
	{
		const char* FETCH_OPTION_FORMAT = "%s --bwlimit=%ld";
		err = snprintf(fetch_option, MAX_FETCH_OPTION_SIZE, FETCH_OPTION_FORMAT, usr_opt_, limit_rate_);
		if (err < 0)
		{
			LOGV(LL_ERROR, "snprintf fetch_option[MAX_FETCH_OPTION_SIZE=%d] error[%s]", MAX_FETCH_OPTION_SIZE, strerror(errno));
			ret = BLADE_ERROR;
		}
		else if (err >= MAX_FETCH_OPTION_SIZE)
		{
			LOGV(LL_ERROR, "fetch_option buffer size is not enough[MAX_FETCH_OPTION_SIZE=%d real=%d]", MAX_FETCH_OPTION_SIZE, err);
			ret = BLADE_ERROR;
		}
	}

	// generate fetch command
	if (BLADE_SUCCESS == ret)
	{
		if (NULL == fn_ext || 0 == strlen(fn_ext))
		{
			const char* FETCH_CMD_WITHOUTEXT_FORMAT = "rsync %s %s:%s/%lu %s/";
			err = snprintf(cmd, size, FETCH_CMD_WITHOUTEXT_FORMAT, fetch_option, vip_str_.c_str(), full_log_dir, id, log_dir_);
		}
		else
		{
			const char* FETCH_CMD_WITHEXT_FORMAT = "rsync %s %s:%s/%lu.%s %s/";
			err = snprintf(cmd, size, FETCH_CMD_WITHEXT_FORMAT, fetch_option, vip_str_.c_str(), full_log_dir, id, fn_ext, log_dir_);
		}

		if (err < 0)
		{
			LOGV(LL_ERROR, "snprintf cmd[size=%ld err=%d] error[%s]", size, err, strerror(errno));
			ret = BLADE_ERROR;
		}
		else if (err >= size)
		{
			LOGV(LL_ERROR, "cmd buffer is not enough[size=%ld err=%d]", size, err);
			ret = BLADE_ERROR;
		}
	}

	return ret;
}

bool BladeFetchRunnable::Exists(const uint64_t id, const char* fn_ext) const
{
	bool res = false;

	char fn[BLADE_MAX_FILENAME_LENGTH];
	int ret = GenFullName(id, fn_ext, fn, BLADE_MAX_FILENAME_LENGTH);
	if (BLADE_SUCCESS != ret)
	{
		LOGV(LL_WARN, "gen_full_name_ error[ret=%d id=%lu fn_ext=%s]", ret, id, fn_ext);
	}
	else
	{
		res = FileDirectoryUtils::exists(fn);
	}

	return res;
}

bool BladeFetchRunnable::Remove(const uint64_t id, const char* fn_ext) const
{
	bool res = false;

	char fn[BLADE_MAX_FILENAME_LENGTH];
	int ret = GenFullName(id, fn_ext, fn, BLADE_MAX_FILENAME_LENGTH);
	if (BLADE_SUCCESS != ret)
	{
		LOGV(LL_WARN, "gen_full_name_ error[ret=%d id=%lu fn_ext=%s]", ret, id, fn_ext);
	}
	else
	{
		res = FileDirectoryUtils::delete_file(fn);
	}

	return res;
}

int BladeFetchRunnable::GenFullName(const uint64_t id, const char* fn_ext, char *buf, const int buf_len) const
{
	int ret = BLADE_SUCCESS;

	int fn_ext_len = 0;
	int err = 0;

	if (NULL == buf || buf_len <= 0)
	{
		LOGV(LL_ERROR, "Parameters are invalid[buf=%p buf_len=%d]", buf, buf_len);
		ret = BLADE_ERROR;
	}
	else
	{
		if (NULL != fn_ext)
		{
			fn_ext_len = strlen(fn_ext);
		}

		if (NULL == fn_ext || 0 == fn_ext_len)
		{
			const char* FN_WITHOUTEXT_FORMAT = "%s/%lu";
			err = snprintf(buf, buf_len, FN_WITHOUTEXT_FORMAT, log_dir_, id);
		}
		else
		{
			const char* FN_WITHEXT_FORMAT = "%s/%lu.%s";
			err = snprintf(buf, buf_len, FN_WITHEXT_FORMAT, log_dir_, id, fn_ext);
		}

		if (err < 0)
		{
			LOGV(LL_ERROR, "snprintf error[err=%d]", err);
			ret = BLADE_ERROR;
		}
		else if (err >= buf_len)
		{
			LOGV(LL_ERROR, "buf is not enough[err=%d buf_len=%d]", err, buf_len);
			ret = BLADE_BUF_NOT_ENOUGH;
		}
	}

	return ret;
}

int BladeFetchRunnable::GetLog()
{
	int ret = BLADE_SUCCESS;

	int err = 0;
	char *cmd = NULL;

	if (!is_initialized_)
	{
		LOGV(LL_ERROR, "BladeFetchRunnable has not been initialized");
		ret = BLADE_NOT_INIT;
	}

	if (BLADE_SUCCESS == ret)
	{
		cmd = static_cast<char*>(malloc(BLADE_MAX_FETCH_CMD_LENGTH));
		if (NULL == cmd)
		{
			LOGV(LL_WARN, "malloc error, BLADE_MAX_FETCH_CMD_LENGTH=%ld", BLADE_MAX_FETCH_CMD_LENGTH);
			ret = BLADE_ERROR;
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		if (param_.fetch_log_)
		{
			for (uint64_t i = param_.min_log_id_; !stop_ && i <= param_.max_log_id_; i++)
			{
				ret = GenFetchCmd(i, NULL, cmd, BLADE_MAX_FETCH_CMD_LENGTH);
				if (BLADE_SUCCESS != ret)
				{
					LOGV(LL_ERROR, "gen_fetch_cmd_[id=%lu fn_ext=NULL] error[ret=%d]", i, ret);
					ret = BLADE_ERROR;
				}
				else
				{
					LOGV(LL_INFO, "fetch log[id=%lu]: \"%s\"", i, cmd);
					err = FSU::vsystem(cmd);
					if (0 != err)
					{
						LOGV(LL_ERROR, "fetch[\"%s\"] error[err=%d]", cmd, err);
						ret = BLADE_ERROR;
					}
					else
					{
						ret = GotLog(i);
						if (BLADE_SUCCESS != ret)
						{
							LOGV(LL_ERROR, "got_log error[err=%d]", err);
						}
					}
				}
			}
		}
	}

	if (NULL != cmd)
	{
		free(cmd);
		cmd = NULL;
	}

	return ret;
}

int BladeFetchRunnable::GetCkpt()
{
	int ret = BLADE_SUCCESS;

	int err = 0;
	char *cmd = NULL;

	if (!is_initialized_)
	{
		LOGV(LL_ERROR, "BladeFetchRunnable has not been initialized");
		ret = BLADE_NOT_INIT;
	}

	if (BLADE_SUCCESS == ret)
	{
		cmd = static_cast<char*>(malloc(BLADE_MAX_FETCH_CMD_LENGTH));
		if (NULL == cmd)
		{
			LOGV(LL_WARN, "malloc error, BLADE_MAX_FETCH_CMD_LENGTH=%ld", BLADE_MAX_FETCH_CMD_LENGTH);
			ret = BLADE_ERROR;
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		if (param_.fetch_ckpt_)
		{
			ret = GenFetchCmd(param_.ckpt_id_, DEFAULT_CKPT_EXTENSION, cmd, BLADE_MAX_FETCH_CMD_LENGTH);
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "gen_fetch_cmd_[id=%lu fn_ext=%s] error[ret=%d]", param_.ckpt_id_, DEFAULT_CKPT_EXTENSION, ret);
				ret = BLADE_ERROR;
			}
			else
			{
				if (Exists(param_.ckpt_id_, DEFAULT_CKPT_EXTENSION))
				{
					if (Remove(param_.ckpt_id_, DEFAULT_CKPT_EXTENSION))
					{
						LOGV(LL_INFO, "remove checkpoint file[id=%lu ext=%s]", param_.ckpt_id_, DEFAULT_CKPT_EXTENSION);
					}
					else
					{
						LOGV(LL_WARN, "remove checkpoint file error[id=%lu ext=%s]", param_.ckpt_id_, DEFAULT_CKPT_EXTENSION);
					}
				}

				LOGV(LL_INFO, "fetch checkpoint[id=%lu ext=\"checkpoint\"]: \"%s\"", param_.ckpt_id_, cmd);
				for (int i = 0; i < DEFAULT_FETCH_RETRY_TIMES; i++)
				{
					err = FSU::vsystem(cmd);
					if (0 == err)
					{
						break;
					}
				}
				if (0 != err)
				{
					LOGV(LL_ERROR, "fetch[\"%s\"] error[err=%d]", cmd, err);
					ret = BLADE_ERROR;
				}
			}

			for (CkptIter i = ckpt_ext_.begin(); !stop_ && i != ckpt_ext_.end(); i++)
			{
				ret = GenFetchCmd(param_.ckpt_id_, (*i).c_str(), cmd, BLADE_MAX_FETCH_CMD_LENGTH);

				if (BLADE_SUCCESS != ret)
				{
					LOGV(LL_ERROR, "gen_fetch_cmd_[id=%lu fn_ext=%s] error[ret=%d]", param_.ckpt_id_, (*i).c_str(), ret);
					ret = BLADE_ERROR;
				}
				else
				{
					if (Exists(param_.ckpt_id_, (*i).c_str()))
					{
						if (Remove(param_.ckpt_id_, (*i).c_str()))
						{
							LOGV(LL_INFO, "remove checkpoint file[id=%lu ext=%s]", param_.ckpt_id_, (*i).c_str());
						}
						else
						{
							LOGV(LL_WARN, "remove checkpoint file error[id=%lu ext=%s]", param_.ckpt_id_, (*i).c_str());
						}
					}

					LOGV(LL_INFO, "fetch checkpoint[id=%lu ext=\"%s\"]: \"%s\"", param_.ckpt_id_, (*i).c_str(), cmd);
					for (int i = 0; i < DEFAULT_FETCH_RETRY_TIMES; i++)
					{
						err = FSU::vsystem(cmd);
						if (0 == err)
						{
							break;
						}
					}
					if (0 != err)
					{
						LOGV(LL_ERROR, "fetch[\"%s\"] error[err=%d]", cmd, err);
						ret = BLADE_ERROR;
					}
				}
			}

			if (BLADE_SUCCESS == ret)
			{
				ret = GotCkpt(param_.ckpt_id_);
				if (BLADE_SUCCESS != ret)
				{
					LOGV(LL_ERROR, "GotCkpt error[err=%d]", err);
				}
			}
		}
	}

	if (NULL != cmd)
	{
		free(cmd);
		cmd = NULL;
	}

	return ret;
}

}//end of namespace ha
}//end of namespace bladestore
