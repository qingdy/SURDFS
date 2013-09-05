#include "blade_ns_log_manager.h"
#include "blade_log_dir_scanner.h"
#include "blade_log_reader.h"
#include "singleton.h"
#include "blade_nameserver_param.h"
#include "file_utils.h"
#include "utility.h"
#include "log.h"

namespace
{
  const int64_t DEFAULT_LOG_SIZE_MB = 64; // 64MB
  const int64_t DEFAULT_LOG_SYNC_TYPE = 0;

  const char* STR_NS_SECTION = "nameserver";
  const char* BLADERT_LOG_DIR_PATH = "log_dir_path";
  const char* BLADERT_LOG_SIZE_MB = "log_size_mb";
  const char* BLADERT_LOG_SYNC_TYPE = "log_sync_type";
}

namespace bladestore
{
namespace ha
{


const int BladeNsLogManager::UINT64_MAX_LEN = 32;

BladeNsLogManager::BladeNsLogManager() : ckpt_id_(0), replay_point_(0), max_log_id_(0), rt_server_status_(0), is_initialized_(false), is_log_dir_empty_(false)
{

}

BladeNsLogManager::~BladeNsLogManager()
{

}

int BladeNsLogManager::Init(NameServerImpl * nameserver, BladeSlaveMgr* slave_mgr)
{
	int ret = BLADE_SUCCESS;

	if (is_initialized_)
	{
		LOGV(LL_ERROR, "nameserver's log manager has been initialized");
		ret = BLADE_INIT_TWICE;
	}

	nameserver_ = nameserver;
	log_worker_.SetLogManager(this);
	log_worker_.SetMetaManager(&(nameserver_->meta_manager()));

	const char* log_dir = (Singleton<BladeNameServerParam>::Instance()).get_log_dir().c_str(); 

	if (BLADE_SUCCESS == ret)
	{
		if (NULL == log_dir)
		{
			LOGV(LL_ERROR, "log directory is null, section: %s, name: %s", STR_NS_SECTION, BLADERT_LOG_DIR_PATH);
			ret = BLADE_INVALID_ARGUMENT;
		}
		else
		{
			int log_dir_len = strlen(log_dir);
			if (log_dir_len >= BLADE_MAX_FILENAME_LENGTH)
			{
				LOGV(LL_ERROR, "Argument is invalid[log_dir_len=%d log_dir=%s]", log_dir_len, log_dir);
				ret = BLADE_INVALID_ARGUMENT;
			}
			else
			{
				strncpy(log_dir_, log_dir, log_dir_len);
				log_dir_[log_dir_len] = '\0';
			}
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		BladeLogDirScanner scanner;

		ret = scanner.Init(log_dir_);
		if (BLADE_SUCCESS != ret && BLADE_DISCONTINUOUS_LOG != ret)
		{
			LOGV(LL_ERROR, "BladeLogDirScanner init error");
		}
		else
		{
			if (!scanner.HasCkpt())
			{
				// no check point
				replay_point_ = 1;
				if (!scanner.HasLog())
				{
					max_log_id_ = 1;
					is_log_dir_empty_ = true;
				}
				else
				{
					ret = scanner.GetMaxLogId(max_log_id_);
					if (BLADE_SUCCESS != ret)
					{
						LOGV(LL_ERROR, "BladeLogDirScanner get_max_log_id error[ret=%d]", ret);
					}
				}
				if (BLADE_SUCCESS == ret)
				{
					LOGV(LL_INFO, "check point does not exist, take replay_point[replay_point_=%lu][ckpt_id=%lu]", replay_point_, ckpt_id_);
				}
			}
			else
			{
				// has checkpoint
				uint64_t min_log_id;

				ret = scanner.GetMaxCkptId(ckpt_id_);

				if (ret == BLADE_SUCCESS)
				{
					ret = scanner.GetMinLogId(min_log_id);
				}

				if (BLADE_SUCCESS != ret)
				{
					LOGV(LL_ERROR, "get_min_log_id error[ret=%d]", ret);
					ret = BLADE_ERROR;
				}
				else
				{
					if (min_log_id > ckpt_id_ + 1)
					{
						LOGV(LL_ERROR, "missing log file[min_log_id=%lu ckeck_point=%lu", min_log_id, ckpt_id_);
						ret = BLADE_ERROR;
					}
					else
					{
						replay_point_ = ckpt_id_ + 1; // replay from next log file
					}
				}

				if (BLADE_SUCCESS == ret)
				{
					ret = scanner.GetMaxLogId(max_log_id_);
					if (BLADE_SUCCESS != ret)
					{
						LOGV(LL_ERROR, "get_max_log_id error[ret=%d]", ret);
						ret = BLADE_ERROR;
					}
				}
			}
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		int64_t log_file_max_size = Singleton<BladeNameServerParam>::Instance().get_log_file_max_size();
		int64_t log_sync_type = Singleton<BladeNameServerParam>::Instance().get_log_sync_type();
		ret = BladeLogWriter::Init(log_dir, log_file_max_size, slave_mgr, log_sync_type);
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "BladeLogWriter init failed[ret=%d]", ret);
		}
		else
		{
			is_initialized_ = true;
			LOGV(LL_INFO, "BladeNsLogMgr[this=%p] init succ[replay_point_=%lu max_log_id_=%lu]", this, replay_point_, max_log_id_);
		}
	}

	return ret;
}

int BladeNsLogManager::AddSlave(const uint64_t & server, uint64_t &new_log_file_id)
{
	int ret = BLADE_SUCCESS;

	BladeSlaveMgr* slave_mgr = GetSlaveMgr();
	if (NULL == slave_mgr)
	{
		LOGV(LL_ERROR, "slave manager is NULL");
		ret = BLADE_ERROR;
	}
	else
	{
		ret = slave_mgr->AddServer(server);
		if (ret != BLADE_SUCCESS)
		{
			LOGV(LL_ERROR, "add slave manager failed [ret=%d]", ret);
		}
		else
		{
			ret = SwitchLogFile(new_log_file_id);
			if (ret != BLADE_SUCCESS)
			{
				LOGV(LL_ERROR, "switch log file error [ret=%d]", ret);
			}
		}
	}

	return ret;
}

int BladeNsLogManager::DoAfterRecoverCheckPoint()
{
	LOGV(LL_INFO, "[NOTICE] after recovery checkpointing");
//	nameserver_->start_threads();
//	nameserver_->wait_init_finished();
	LOGV(LL_INFO, "[NOTICE] background threads started and finished init");
	return BLADE_SUCCESS;
}

int BladeNsLogManager::ReplayLog()
{
	int ret = BLADE_SUCCESS;

	BladeLogReader log_reader;

	char* log_data = NULL;
	int64_t log_length = 0;
	LogCommand cmd = BLADE_LOG_UNKNOWN;
	uint64_t seq = 0;

	ret = CheckInnerStat();

	if (ret == BLADE_SUCCESS && !is_log_dir_empty_)
	{
		if (ckpt_id_ > 0)
		{
			ret = LoadServerStatus();
			if (ret == BLADE_SUCCESS)
			{
				ret = nameserver_->recover_from_check_point(rt_server_status_, ckpt_id_);
				if (ret != BLADE_SUCCESS)
				{
					LOGV(LL_ERROR, "recover from check point failed, err=%d", ret);
				}
			}
			else
			{
				LOGV(LL_ERROR, "load server status failed, so recover from check point failed");
			}
		}

		if (ret == BLADE_SUCCESS)
		{
			ret = DoAfterRecoverCheckPoint();
		}

		if (ret == BLADE_SUCCESS)
		{
			ret = log_reader.Init(log_dir_, replay_point_, false);
			if (ret != BLADE_SUCCESS)
			{
				LOGV(LL_ERROR, "BladeLogReader init failed, ret=%d", ret);
			}
			else
			{
				ret = log_reader.ReadLog(cmd, seq, log_data, log_length);
//				hex_dump(log_data, log_length);
				if (BLADE_FILE_NOT_EXIST != ret && BLADE_READ_NOTHING != ret && BLADE_SUCCESS != ret)
				{
					LOGV(LL_ERROR, "BladeLogReader read error[ret=%d]", ret);
				}

				while (ret == BLADE_SUCCESS)
				{
					SetCurLogSeq(seq);

					if (BLADE_LOG_NOP != cmd)
					{
//						LOGV(LL_DEBUG, "BEFORE APPLY");
						ret = log_worker_.Apply(cmd, log_data, log_length);
//						LOGV(LL_DEBUG, "AFTER APPLY");

						if (ret != BLADE_SUCCESS)
						{
							LOGV(LL_ERROR, "apply log failed, command: %d", cmd);
						}

					}

					ret = log_reader.ReadLog(cmd, seq, log_data, log_length);
					if (BLADE_FILE_NOT_EXIST != ret && BLADE_READ_NOTHING != ret && BLADE_SUCCESS != ret)
					{
						LOGV(LL_ERROR, "BladeLogReader read error[ret=%d]", ret);
					}
				}

				// handle exception, when the last log file contain SWITCH_LOG entry
				// but the next log file is missing
				if (BLADE_FILE_NOT_EXIST == ret)
				{
					max_log_id_++;
					ret = BLADE_SUCCESS;
				}

				// reach the end of commit log
				if (BLADE_READ_NOTHING == ret)
				{
					ret = BLADE_SUCCESS;
				}
			}
		}
	}
	else if(ret == BLADE_SUCCESS)
	{
		ret = DoAfterRecoverCheckPoint();
	}

	//if (BLADE_SUCCESS == ret)
	{
		ret = StartLog(max_log_id_, GetCurLogSeq());
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "BladeLogWriter start_log[max_log_id_=%lu get_cur_log_seq()=%lu", max_log_id_, GetCurLogSeq());
		}
		else
		{
			LOGV(LL_INFO, "start log [%d] done", max_log_id_);
		}
	}
	return ret;
}

//对于一个checkpoint_id,可能存在多个后缀，这样就需要对每个checkpoint file做处理,@fix me later
int BladeNsLogManager::RecoverCheckpoint(const uint64_t checkpoint_id)
{
	int ret = BLADE_SUCCESS;

	ckpt_id_ = checkpoint_id;

	if (ckpt_id_ > 0)
	{
		ret = LoadServerStatus();

		if (ret == BLADE_SUCCESS)
		{
			ret = nameserver_->recover_from_check_point(rt_server_status_, ckpt_id_);

			if (ret != BLADE_SUCCESS)
			{
				LOGV(LL_ERROR, "recover from check point [%d] failed, err=%d", ckpt_id_, ret);
			}
			else
			{
				LOGV(LL_INFO, "recover from check point [%d] done", ckpt_id_);
			}

		}
		else
		{
			LOGV(LL_ERROR, "load server status failed, so recover from check point failed");
		}
	}

	return ret;
}

int BladeNsLogManager::DoCheckPoint(const uint64_t checkpoint_id/* = 0 */)
{
	// do check point
	int ret = BLADE_SUCCESS;

	uint64_t ckpt_id = checkpoint_id;

	if (ckpt_id == 0) 
	{
		ret = WriteCheckpointLog(ckpt_id);
	}

	if (ret == BLADE_SUCCESS)
	{
		// write server status into default checkpoint file
		char filename[BLADE_MAX_FILENAME_LENGTH];
		int err = 0;
		FileUtils ckpt_file;
		err = snprintf(filename, BLADE_MAX_FILENAME_LENGTH, "%s/%lu.%s", log_dir_, ckpt_id, DEFAULT_CKPT_EXTENSION);
		if (err < 0 || err >= BLADE_MAX_FILENAME_LENGTH)
		{
			LOGV(LL_ERROR, "generate check point file name failed, error: %s", strerror(errno));
			ret = BLADE_ERROR;
		}
		else
		{
			err = ckpt_file.open(filename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH); 
			if (err < 0)
			{
				LOGV(LL_ERROR, "open check point file[%s] failed: %s", filename, strerror(errno));
				ret = BLADE_ERROR;
			}
			else
			{
				char st_str[UINT64_MAX_LEN];
				int st_str_len = 0;
				int server_status = nameserver_->get_server_status();
				st_str_len = snprintf(st_str, UINT64_MAX_LEN, "%d", server_status);
				if (st_str_len < 0)
				{
					LOGV(LL_ERROR, "snprintf st_str error[%s][server_status=%d]", strerror(errno), server_status);
					ret = BLADE_ERROR;
				}
				else if (st_str_len >= UINT64_MAX_LEN)
				{
					LOGV(LL_ERROR, "st_str is too long[st_str_len=%d UINT64_MAX_LEN=%d", st_str_len, UINT64_MAX_LEN);
					ret = BLADE_ERROR;
				}
				else
				{
					err = ckpt_file.write(st_str, st_str_len);
					if (err < 0)
					{
						LOGV(LL_ERROR, "write error[%s]", strerror(errno));
						ret = BLADE_ERROR;
					}
				}

				ckpt_file.close();
			}
		}
	}

	if (ret == BLADE_SUCCESS)
	{
		ret = nameserver_->do_check_point(ckpt_id);
	}

	if (ret == BLADE_SUCCESS)
	{
		LOGV(LL_INFO, "do check point success, check point id: %lu => %lu", ckpt_id_, ckpt_id);
		ckpt_id_ = ckpt_id;
		replay_point_ = ckpt_id;
	}

	return ret;
}

int BladeNsLogManager::LoadServerStatus()
{
	int ret = BLADE_SUCCESS;

	int err = 0;
	char st_str[UINT64_MAX_LEN];
	int st_str_len = 0;

	char ckpt_fn[BLADE_MAX_FILENAME_LENGTH];
	err = snprintf(ckpt_fn, BLADE_MAX_FILENAME_LENGTH, "%s/%lu.%s", log_dir_, ckpt_id_, DEFAULT_CKPT_EXTENSION);
	if (err < 0 || err >= BLADE_MAX_FILENAME_LENGTH)
	{
		LOGV(LL_ERROR, "generate check point file name [%s] failed, error: %s", ckpt_fn, strerror(errno));
		ret = BLADE_ERROR;
	}

	if (ret == BLADE_SUCCESS && !FileDirectoryUtils::exists(ckpt_fn))
	{
		LOGV(LL_INFO, "check point file[\"%s\"] does not exist", ckpt_fn);
		ret = BLADE_FILE_NOT_EXIST;
	}
	else
	{
		FileUtils ckpt_file;
		err = ckpt_file.open(ckpt_fn, O_RDONLY);
		if (err < 0)
		{
			LOGV(LL_ERROR, "open file[\"%s\"] error[%s]", ckpt_fn, strerror(errno));
			ret = BLADE_ERROR;
		}
		else
		{
			st_str_len = ckpt_file.read(st_str, UINT64_MAX_LEN);
			LOGV(LL_DEBUG, "st_str_len = %d", st_str_len);
			st_str[st_str_len] = '\0';
			if (st_str_len < 0)
			{
				LOGV(LL_ERROR, "read file error[%s]", strerror(errno));
				ret = BLADE_ERROR;
			}
			else if ((st_str_len >= UINT64_MAX_LEN) || (st_str_len == 0))
			{
				LOGV(LL_ERROR, "data contained in ckeck point file is invalid[ckpt_str_len=%d]", st_str_len);
				ret = BLADE_ERROR;
			}
			else
			{
				int rc = sscanf(st_str, "%d", &rt_server_status_);
				if (rc != 1)
				{
					LOGV(LL_ERROR, "server status load failed.");
					ret = BLADE_ERROR;
				}
			}
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		LOGV(LL_INFO, "load server status succ[server_status=%d] from file[\"%s\"]", rt_server_status_, ckpt_fn);
	}

	return ret;
}

}//end of namespace ha
}//end of namespace bladestore

