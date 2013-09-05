#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include "blade_log_writer.h"
#include "blade_common_define.h"
#include "blade_trace_log.h"
#include "time_util.h"
#include "utility.h"
#include "log.h"

using namespace pandora;

namespace bladestore
{
namespace ha
{

BladeLogWriter::BladeLogWriter()
{
	is_initialized_ = false;
	dio_ = true;
	cur_log_size_ = 0;
	log_file_max_size_ = 0;
	log_dir_[0] = '\0';
	cur_log_file_id_ = 0;
	cur_log_seq_ = 0;
	slave_mgr_ = NULL;
	log_sync_type_ = BLADE_LOG_NOSYNC;
	memset(empty_log_, 0x00, sizeof(empty_log_));
}

BladeLogWriter::~BladeLogWriter()
{
	file_.close();

	if (NULL != log_buffer_.get_data())
	{
		free(log_buffer_.get_data());
		log_buffer_.reset();
	}
}

int BladeLogWriter::Init(const char* log_dir, const int64_t log_file_max_size, BladeSlaveMgr *slave_mgr, int64_t log_sync_type)
{
	int ret = BLADE_SUCCESS;

	int log_dir_len = 0;
	if (is_initialized_)
	{
		LOGV(LL_ERROR, "BladeLogWriter has been initialized");
		ret = BLADE_INIT_TWICE;
	}
	else
	{
		if (NULL == log_dir || NULL == slave_mgr)
		{
			LOGV(LL_ERROR, "Parameter are invalid[log_dir=%p slave_mgr=%p]", log_dir, slave_mgr);
			ret = BLADE_INVALID_ARGUMENT;
		}
		else
		{
			log_dir_len = strlen(log_dir);
			if (log_dir_len >= BLADE_MAX_FILENAME_LENGTH)
			{
				LOGV(LL_ERROR, "log_dir is too long[log_dir_len=%d]", log_dir_len);
				ret = BLADE_INVALID_ARGUMENT;
			}
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		strncpy(log_dir_, log_dir, log_dir_len + 1);
		log_file_max_size_ = log_file_max_size;
		slave_mgr_ = slave_mgr;
		log_sync_type_ = log_sync_type;

		void* buf = malloc(LOG_BUFFER_SIZE);
		if (NULL == buf)
		{
			LOGV(LL_ERROR, "ob_malloc[length=%ld] error", LOG_BUFFER_SIZE);
			ret = BLADE_ALLOCATE_MEMORY_FAILED;
		}
		else
		{
			log_buffer_.set_data(static_cast<char*>(buf), LOG_BUFFER_SIZE);
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		is_initialized_ = true;
		LOGV(LL_INFO, "BladeLogWriter initialize successfully[log_dir_=%s log_file_max_size_=%ld cur_log_file_id_=%lu cur_log_seq_=%lu slave_mgr_=%p log_sync_type_=%ld]",log_dir_, log_file_max_size_, cur_log_file_id_, cur_log_seq_, slave_mgr_, log_sync_type_);
	}

	return ret;
}

void BladeLogWriter::ResetLog()
{
	if (file_.is_opened())
	{
		file_.close();
	}

	cur_log_size_ = 0;
	cur_log_file_id_ = 0;
	cur_log_seq_ = 0;
}

int BladeLogWriter::StartLog(const uint64_t log_file_max_id, const uint64_t log_max_seq)
{
	int ret = BLADE_SUCCESS;

	ret = CheckInnerStat();

	if (BLADE_SUCCESS == ret)
	{
		cur_log_file_id_ = log_file_max_id;
		cur_log_seq_ = log_max_seq;

		ret = OpenLogFile(cur_log_file_id_, false);
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "open_log_file_ error[ret=%d]", ret);
		}
		else
		{
			// get current log file size
			cur_log_size_ = file_.get_file_pos();
			if (-1 == cur_log_size_)
			{
				LOGV(LL_ERROR, "get_file_pos error[%s]", strerror(errno));
				ret = BLADE_ERROR;
			}
			else
			{
				LOGV(LL_INFO, "commit log cur_log_file_id_=%lu cur_log_size_=%ld", cur_log_file_id_, cur_log_size_);
			}
		}
	}

	return ret;
}

int BladeLogWriter::StartLog(const uint64_t log_file_max_id)
{
	int ret = BLADE_SUCCESS;

	ret = CheckInnerStat();

	if (BLADE_SUCCESS == ret)
	{
		cur_log_file_id_ = log_file_max_id + 1;

		ret = OpenLogFile(cur_log_file_id_, true);
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "open_log_file_ error[ret=%d]", ret);
		}
	}

	return ret;
}

int BladeLogWriter::WriteLog(const LogCommand cmd, const char* log_data, const int64_t data_len)
{
	int ret = CheckInnerStat();

	if (BLADE_SUCCESS == ret)
	{
		if ((NULL == log_data && data_len != 0) || (NULL != log_data && data_len <= 0))
		{
			LOGV(LL_ERROR, "Parameters are invalid[log_data=%p data_len=%ld]", log_data, data_len);
			ret = BLADE_INVALID_ARGUMENT;
		}
		else
		{
			if (0 == log_buffer_.get_position())
			{
				ret = CheckLogFileSize(LOG_BUFFER_SIZE);
				if (BLADE_SUCCESS != ret)
				{
					LOGV(LL_ERROR, "check_log_file_size_[cur_log_size_=%ld new_length=%ld] error[ret=%d]", cur_log_size_, LOG_BUFFER_SIZE, ret);
				}
			}

			if (BLADE_SUCCESS == ret)
			{
				ret = SerializeLog(cmd, log_data, data_len);
			}
		}
	}

	return ret;
}

int BladeLogWriter::FlushLog()
{
	int ret = CheckInnerStat();

	if (BLADE_SUCCESS == ret)
	{
		if (log_buffer_.get_position() > 0)
		{
			//ret = serialize_nop_log_();
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "serialize_nop_log_ error, ret=%d", ret);
			}
			else
			{
				int64_t send_start_time = TimeUtil::GetTime();
				ret = slave_mgr_->SendData(log_buffer_.get_data(), log_buffer_.get_position());
				last_net_elapse_ = TimeUtil::GetTime() - send_start_time;
				if (BLADE_SUCCESS == ret || BLADE_PARTIAL_FAILED == ret)
				{
					int64_t store_start_time = TimeUtil::GetTime();
					ret = StoreLog(log_buffer_.get_data(), log_buffer_.get_position());
					last_disk_elapse_ = TimeUtil::GetTime() - store_start_time;
					if (BLADE_SUCCESS == ret)
					{
						log_buffer_.get_position() = 0;
					}
				}
			}
		}
	}

	return ret;
}

//单条log力度的同步
int BladeLogWriter::WriteAndFlushLog(const LogCommand cmd, const char* log_data, const int64_t data_len)
{
	int ret = CheckInnerStat();

	if (BLADE_SUCCESS == ret)
	{
		// check whether remaining data
		// if so, clear it
		if (log_buffer_.get_position() > 0)
		{
			log_buffer_.get_position() = 0;
		}
		
		ret = WriteLog(cmd, log_data, data_len);
		if (BLADE_SUCCESS == ret)
		{
			ret = FlushLog();
		}
	}

	return ret;
}

int BladeLogWriter::StoreLog(const char* buf, const int64_t buf_len)
{
	int ret = BLADE_SUCCESS;

	ret = CheckInnerStat();

	if (BLADE_SUCCESS == ret)
	{
		if (NULL == buf || buf_len <=0)
		{
			LOGV(LL_ERROR, "parameters are invalid[buf=%p buf_len=%ld]", buf, buf_len);
			ret = BLADE_INVALID_ARGUMENT;
		}
	}
	

	if (BLADE_SUCCESS == ret)
	{
		hex_dump(buf, buf_len);
		ret = file_.append(buf, buf_len, BLADE_LOG_NOSYNC == log_sync_type_ ? false : true);
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "write data[buf_len=%ld] to commit log error[%s]", buf_len, strerror(errno));
		}
		else
		{
			cur_log_size_ += buf_len;
			LOGV(LL_DEBUG, "write %ld bytes to log[%lu] [cur_log_size_=%ld]", buf_len, cur_log_file_id_, cur_log_size_);
		}
	}

	return ret;
}

int BladeLogWriter::SwitchLogFile(uint64_t &new_log_file_id)
{
	int ret = CheckInnerStat();

	if (BLADE_SUCCESS == ret)
	{
		ret = FlushLog();

		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "flush_log error[ret=%d]", ret);
		}
		else
		{
			const int64_t buf_len = LOG_FILE_ALIGN_SIZE - BladeLogEntry::GetHeaderSize();
			char buf[buf_len];
			int64_t buf_pos = 0;
			ret = serialization::encode_i64(buf, buf_len, buf_pos, cur_log_file_id_);
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "encode_i64[cur_log_file_id_=%lu] error[ret=%d]", cur_log_file_id_, ret);
			}
			else
			{
				ret = SerializeLog(BLADE_LOG_SWITCH_LOG, buf, buf_len);
				if (BLADE_SUCCESS != ret)
				{
					LOGV(LL_ERROR, "serialize_log_ cur_log_file_id_[%lu] error[ret=%d]", cur_log_file_id_, ret);
				}
				else
				{
					ret = FlushLog();
					if (BLADE_SUCCESS != ret)
					{
						LOGV(LL_ERROR, "flush_log error[ret=%d]", ret);
					}
					else
					{
						ret = SwitchToLogFile(cur_log_file_id_ + 1);
						if (BLADE_SUCCESS == ret)
						{
							new_log_file_id = cur_log_file_id_;
						}
					}
				}
			}
		}
	}

	return ret;
}

int BladeLogWriter::SwitchToLogFile(const uint64_t log_file_id)
{
	int ret = CheckInnerStat();

	if (BLADE_SUCCESS == ret)
	{
		if ((cur_log_file_id_ + 1) != log_file_id)
		{
			LOGV(LL_ERROR, "log_file_id is not continous[cur_log_file_id_=%lu log_file_id=%lu]", cur_log_file_id_, log_file_id);
			ret = BLADE_ERROR;
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		file_.close();
	}

	if (BLADE_SUCCESS == ret)
	{
		ret = OpenLogFile(log_file_id, true);
		if (BLADE_SUCCESS == ret)
		{
			LOGV(LL_INFO, "switch_log_file successfully from %lu to %lu", cur_log_file_id_, log_file_id);
			cur_log_file_id_ = log_file_id;
		}
	}

	return ret;
}

int BladeLogWriter::WriteCheckpointLog(uint64_t &log_file_id)
{
	LOGV(LL_DEBUG, "IN WRITE_CHECKPOINT_LOG");
	int ret = CheckInnerStat();

	log_file_id = 0;
	if (BLADE_SUCCESS == ret)
	{
		ret = FlushLog();
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "flush_log error[ret=%d]", ret);
		}
		else
		{
			const int64_t buf_len = sizeof(uint64_t);
			char buf[buf_len];
			int64_t buf_pos = 0;
			ret = serialization::encode_i64(buf, buf_len, buf_pos, cur_log_file_id_);
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "encode_i64[cur_log_file_id_=%lu] error[ret=%d]", cur_log_file_id_, ret);
			}
			else
			{
				ret = SerializeLog(BLADE_LOG_CHECKPOINT, buf, buf_pos);
				if (BLADE_SUCCESS != ret)
				{
					LOGV(LL_ERROR, "serialize_log_ cur_log_file_id_[%lu] error[ret=%d]", cur_log_file_id_, ret);
				}
				else
				{
					ret = FlushLog();
					if (BLADE_SUCCESS != ret)
					{
						LOGV(LL_ERROR, "flush_log error[ret=%d]", ret);
					}
					else
					{
						LOGV(LL_INFO, "write_checkpoint_log successfully[cur_log_file_id_=%lu]", cur_log_file_id_);
						uint64_t new_log_file_id = 0;
						ret = SwitchLogFile(new_log_file_id);
						if (BLADE_SUCCESS == ret)
						{
							log_file_id = cur_log_file_id_ - 1;
						}
					}
				}
			}
		}
	}
	return ret;
}

int BladeLogWriter::OpenLogFile(const uint64_t log_file_id, bool is_trunc)
{
	int ret = BLADE_SUCCESS;

	struct stat file_info;
	int err = stat(log_dir_, &file_info);
	if (err != 0)  // log_dir does not exist
	{
		err = mkdir(log_dir_, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
		if (err != 0)
		{
			LOGV(LL_ERROR, "create \"%s\" directory error[%s]", log_dir_, strerror(errno));
			ret = BLADE_ERROR;
		}
		else
		{
			LOGV(LL_INFO, "create log directory[\"%s\"]", log_dir_);
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		char file_name[BLADE_MAX_FILENAME_LENGTH];
		err = snprintf(file_name, BLADE_MAX_FILENAME_LENGTH, "%s/%lu", log_dir_, log_file_id);
		if (err < 0)
		{
			LOGV(LL_ERROR, "snprintf log filename error[%s]", strerror(errno));
			ret = BLADE_ERROR;
		}
		else if (err >= BLADE_MAX_FILENAME_LENGTH)
		{
			LOGV(LL_ERROR, "generated filename is too long[length=%d]", err);
			ret = BLADE_ERROR;
		}
		else
		{
			std::string filename;
			filename.assign(file_name);
			ret = file_.open(filename , dio_, true, is_trunc);
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "open commit log file[file_name=%s] ret=%d", file_name, ret);
			}
			else
			{
				cur_log_size_ = 0;
			}
		}
	}

	return ret;
}

int BladeLogWriter::SerializeLog(const LogCommand cmd, const char* log_data, const int64_t data_len)
{
	int ret = BLADE_SUCCESS;

	BladeLogEntry entry;
	if ((NULL == log_data && data_len != 0) || (NULL != log_data && data_len <= 0))
	{
		LOGV(LL_ERROR, "Parameters are invalid[log_data=%p data_len=%ld]", log_data, data_len);
		ret = BLADE_INVALID_ARGUMENT;
	}
	else
	{
		uint64_t new_log_seq = 0;
		if (BLADE_LOG_SWITCH_LOG == cmd)
		{
			new_log_seq = cur_log_seq_;
		}
		else
		{
			new_log_seq = cur_log_seq_ + 1;
		}
		entry.SetLogSeq(new_log_seq);
		entry.SetLogCommand(cmd);
		ret = entry.FillHeader(log_data, data_len);
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "BladeLogEntry fill_header error[ret=%d log_data=%p data_len=%ld]", ret, log_data, data_len);
		}
		else
		{
			if ((data_len + entry.GetSerializeSize() + LOG_FILE_ALIGN_SIZE) > log_buffer_.get_remain())
			{
				LOGV(LL_DEBUG, "log_buffer_ remaining length[%ld] is less then \"data_len[%ld] + BladeLogEntry[%ld]\"",	log_buffer_.get_remain(), data_len, entry.GetSerializeSize());
				ret = BLADE_BUF_NOT_ENOUGH;
			}
			else
			{
				ret = entry.Serialize(log_buffer_.get_data(), log_buffer_.get_capacity(), log_buffer_.get_position());
				if (BLADE_SUCCESS != ret)
				{
					LOGV(LL_ERROR, "BladeLogEntry serialize error[ret=%d buffer_remain=%ld]", ret, log_buffer_.get_remain());
				}
				else
				{
					if (NULL != log_data)
					{
						memcpy(log_buffer_.get_data() + log_buffer_.get_position(), log_data, data_len);
						log_buffer_.get_position() += data_len;
					}
					cur_log_seq_ = new_log_seq;
				}
			}
		}
	}

	return ret;
}

int BladeLogWriter::SerializeNopLog()
{
	int ret = BLADE_SUCCESS;

	BladeLogEntry entry;

	if (log_buffer_.get_position() == (log_buffer_.get_position() & LOG_FILE_ALIGN_MASK))
	{
		LOGV(LL_DEBUG, "The log is aligned");
	}
	else
	{
		uint64_t new_log_seq = cur_log_seq_ + 1;
		LOGV(LL_DEBUG, "NO OP LOG SEQ : %d", new_log_seq);
		int64_t data_len = (log_buffer_.get_position() & LOG_FILE_ALIGN_MASK) + LOG_FILE_ALIGN_SIZE - log_buffer_.get_position();
		if (data_len <= entry.GetHeaderSize())
		{
			data_len += LOG_FILE_ALIGN_SIZE;
		}
		data_len -= entry.GetHeaderSize();

		entry.SetLogSeq(new_log_seq);
		entry.SetLogCommand(BLADE_LOG_NOP);
		ret = entry.FillHeader(empty_log_, data_len);
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "BladeLogEntry fill_header error[ret=%d data_len=%ld]", ret, data_len);
		}
		else
		{
			ret = entry.Serialize(log_buffer_.get_data(), log_buffer_.get_capacity(), log_buffer_.get_position());
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "BladeLogEntry serialize error[ret=%d buffer_remain=%ld]", ret, log_buffer_.get_remain());
			}
			else
			{
				memcpy(log_buffer_.get_data() + log_buffer_.get_position(), empty_log_, data_len);
				log_buffer_.get_position() += data_len;
				cur_log_seq_ = new_log_seq;
			}
		}
	}

	return ret;
}

int BladeLogWriter::CheckLogFileSize(const int64_t new_length)
{
	int ret = BLADE_SUCCESS;

	if ((cur_log_size_ + new_length) > log_file_max_size_)
	{
		uint64_t new_log_file_id = 0;
		ret = SwitchLogFile(new_log_file_id);
	}
	return ret;
}

}//end of namespace ha
}//end of namespace bladestore
