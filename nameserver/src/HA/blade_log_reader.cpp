#include "blade_log_reader.h"
#include "log.h"

using namespace pandora;

namespace bladestore
{
namespace ha
{

BladeLogReader::BladeLogReader()
{
	is_initialized_ = false;
	cur_log_file_id_ = 0;
	is_wait_ = false;
}

BladeLogReader::~BladeLogReader()
{

}

int BladeLogReader::Init(const char* log_dir, const uint64_t log_file_id_start, bool is_wait)
{
	int ret = BLADE_SUCCESS;

	if (is_initialized_)
	{
		LOGV(LL_ERROR, "BladeLogReader has been initialized before");
		ret = BLADE_INIT_TWICE;
	}
	else
	{
		if (NULL == log_dir)
		{
			LOGV(LL_ERROR, "Parameter is invalid[log_dir=%p]", log_dir);
			ret = BLADE_INVALID_ARGUMENT;
		}
		else
		{
			ret = log_file_reader_.Init(log_dir);
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "log_file_reader_ init[log_dir=%s] error[ret=%d]", log_dir, ret);
			}
			else
			{
				cur_log_file_id_ = log_file_id_start;
				max_log_file_id_ = 0;
				is_wait_ = is_wait;
				is_initialized_ = true;
				has_max_ = false;
			}
		}
	}

	return ret;
}

int BladeLogReader::ReadLog(LogCommand &cmd, uint64_t &seq, char* &log_data, int64_t &data_len)
{
	int ret = BLADE_SUCCESS;

	if (!is_initialized_)
	{
		LOGV(LL_ERROR, "BladeLogReader has not been initialized");
		ret = BLADE_NOT_INIT;
	}
	else
	{
		if (!log_file_reader_.IsOpened())
		{
			ret = OpenLog_(cur_log_file_id_);
		}
		if (BLADE_SUCCESS == ret)
		{
			ret = ReadLog_(cmd, seq, log_data, data_len);
			if (BLADE_SUCCESS == ret)
			{
				while (BLADE_SUCCESS == ret && BLADE_LOG_SWITCH_LOG == cmd)
				{
					LOGV(LL_INFO, "reach the end of log[cur_log_file_id_=%lu]", cur_log_file_id_);
					ret = log_file_reader_.Close();
					if (BLADE_SUCCESS != ret)
					{
						LOGV(LL_ERROR, "log_file_reader_ close error[ret=%d]", ret);
					}
					else
					{
						cur_log_file_id_ ++;
						ret = OpenLog_(cur_log_file_id_, seq);
						if (BLADE_SUCCESS == ret)
						{
							ret = log_file_reader_.ReadLog(cmd, seq, log_data, data_len);
							if (BLADE_SUCCESS != ret && BLADE_READ_NOTHING != ret)
							{
								LOGV(LL_ERROR, "log_file_reader_ read_log error[ret=%d]", ret);
							}
						}
					}
				}
			}
		}
	}

	return ret;
}

int BladeLogReader::OpenLog_(const uint64_t log_file_id, const uint64_t last_log_seq/* = 0*/)
{
	int ret = BLADE_SUCCESS;

	if (is_wait_ && has_max_ && log_file_id > max_log_file_id_)
	{
		ret = BLADE_READ_NOTHING;
	}
	else
	{
		ret = log_file_reader_.Open(log_file_id, last_log_seq);
		if (is_wait_)
		{
			if (BLADE_FILE_NOT_EXIST == ret)
			{
				LOGV(LL_DEBUG, "log file doesnot exist, id=%lu", log_file_id);
				ret = BLADE_READ_NOTHING;
			}
			else if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_WARN, "log_file_reader_ open[id=%lu] error[ret=%d]", cur_log_file_id_, ret);
			}
		}
		else
		{
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_WARN, "log_file_reader_ open[id=%lu] error[ret=%d]", cur_log_file_id_, ret);
			}
		}
	}

	return ret;
}

int BladeLogReader::ReadLog_(LogCommand &cmd, uint64_t &log_seq, char *&log_data, int64_t &data_len)
{
	int ret = BLADE_SUCCESS;

	int err = log_file_reader_.ReadLog(cmd, log_seq, log_data, data_len);
	//while (BLADE_READ_NOTHING == err && is_wait_)
	//{
	//  usleep(WAIT_TIME);
	//  err = log_file_reader_.read_log(cmd, log_seq, log_data, data_len);
	//}
	ret = err;
	if (BLADE_SUCCESS != ret && BLADE_READ_NOTHING != ret)
	{
		LOGV(LL_WARN, "log_file_reader_ read_log error[ret=%d]", ret);
	}

	return ret;
}

}//end of namespace ha
}//end of namespace bladestore

