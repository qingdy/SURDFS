#include <errno.h>

#include "blade_single_log_reader.h"
#include "utility.h"

namespace bladestore
{
namespace ha
{

const int64_t BladeSingleLogReader::LOG_BUFFER_MAX_LENGTH = 1 << 21;

BladeSingleLogReader::BladeSingleLogReader()
{
	is_initialized_ = false;
	dio_ = true;
	file_name_[0] = '\0';
	file_id_ = 0;
	last_log_seq_ = 0;
	log_buffer_.reset();
}

BladeSingleLogReader::~BladeSingleLogReader()
{
	if (NULL != log_buffer_.get_data())
	{
		free(log_buffer_.get_data());
		log_buffer_.reset();
	}
}

int BladeSingleLogReader::Init(const char* log_dir)
{
	int ret = BLADE_SUCCESS;

	if (is_initialized_)
	{
		LOGV(LL_ERROR, "BladeSingleLogReader has been initialized");
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
			int log_dir_len = strlen(log_dir);
			if (log_dir_len >= BLADE_MAX_FILENAME_LENGTH)
			{
				LOGV(LL_ERROR, "log_dir is too long[len=%d log_dir=%s]", log_dir_len, log_dir);
				ret = BLADE_INVALID_ARGUMENT;
			}
			else
			{
				strncpy(log_dir_, log_dir, log_dir_len + 1);
			}
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		if (NULL == log_buffer_.get_data())
		{
			char* buf = static_cast<char*>(malloc(LOG_BUFFER_MAX_LENGTH));
			if (NULL == buf)
			{
				LOGV(LL_ERROR, "ob_malloc for log_buffer_ failed");
				ret = BLADE_ERROR;
			}
			else
			{
				log_buffer_.set_data(buf, LOG_BUFFER_MAX_LENGTH);
			}
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		LOGV(LL_INFO, "BladeSingleLogReader initialize successfully");
		last_log_seq_ = 0;

		is_initialized_ = true;
	}

	return ret;
}

int BladeSingleLogReader::Open(const uint64_t file_id, const uint64_t last_log_seq/* = 0*/)
{
	int ret = CheckInnerStat();

	if (BLADE_SUCCESS == ret)
	{
		int err = snprintf(file_name_, BLADE_MAX_FILENAME_LENGTH, "%s/%lu", log_dir_, file_id);
		if (err < 0)
		{
			LOGV(LL_ERROR, "snprintf file_name[log_dir_=%s file_id=[%lu] error[%s]", log_dir_, file_id, strerror(errno));
			ret = BLADE_ERROR;
		}
		else if (err >= BLADE_MAX_FILENAME_LENGTH)
		{
			LOGV(LL_ERROR, "snprintf file_name[file_id=%lu] error[%s]", file_id, strerror(errno));
			ret = BLADE_ERROR;
		}
		else
		{
			string filename;
			filename.assign(file_name_);
			ret = file_.open(filename, dio_);
			if (BLADE_SUCCESS == ret)
			{
				file_id_ = file_id;
				last_log_seq_ = last_log_seq;
				pos = 0;
				pread_pos_ = 0;
				log_buffer_.get_position() = 0;
				log_buffer_.get_limit() = 0;
				LOGV(LL_INFO, "open log file(name=%s id=%lu)", file_name_, file_id);
			}
			else if (BLADE_FILE_NOT_EXIST == ret)
			{
				LOGV(LL_DEBUG, "log file(name=%s id=%lu) not found", file_name_, file_id);
			}
			else
			{
				LOGV(LL_WARN, "open file[name=%s] error[%s]", file_name_, strerror(errno));
			}
		}
	}

	return ret;
}

int BladeSingleLogReader::Close()
{
	int ret = CheckInnerStat();

	if (BLADE_SUCCESS == ret)
	{
		file_.close();
		if (0 == last_log_seq_)
		{
			LOGV(LL_INFO, "close file[name=%s], read no data from this log", file_name_);
		}
		else
		{
			LOGV(LL_INFO, "close file[name=%s] successfully, the last log sequence is %lu", file_name_, last_log_seq_);
		}
	}

	return ret;
}

int BladeSingleLogReader::Reset()
{
	int ret = CheckInnerStat();

	if (BLADE_SUCCESS == ret)
	{
		ret = Close();
		if (BLADE_SUCCESS == ret)
		{
			free(log_buffer_.get_data());
			log_buffer_.reset();
			is_initialized_ = false;
		}
	}

	return ret;
}

int BladeSingleLogReader::ReadLog(LogCommand &cmd, uint64_t &log_seq, char *&log_data, int64_t & data_len)
{
	int ret = BLADE_SUCCESS;

	BladeLogEntry entry;
	if (!is_initialized_)
	{
		LOGV(LL_ERROR, "BladeSingleLogReader has not been initialized, please initialize first");
		ret = BLADE_NOT_INIT;
	}
	else
	{
		ret = entry.Deserialize(log_buffer_.get_data(), log_buffer_.get_limit(), log_buffer_.get_position());
		if (BLADE_SUCCESS != ret)
		{
			ret = ReadLog_();
			if (BLADE_READ_NOTHING == ret)
			{
				// comment this log due to too frequent invoke by replay thread
			}
			else if (BLADE_SUCCESS == ret)
			{
				ret = entry.Deserialize(log_buffer_.get_data(), log_buffer_.get_limit(), log_buffer_.get_position());
				if (BLADE_SUCCESS != ret)
				{
					LOGV(LL_DEBUG, "do not get a full entry, when reading BladeLogEntry");
					ret = BLADE_READ_NOTHING;
				}
			}
		}

		if (BLADE_SUCCESS == ret)
		{
			if (IsEntryZeroed(entry))
			{
				log_buffer_.get_position() -= entry.GetSerializeSize();
				pread_pos_ -= log_buffer_.get_limit() - log_buffer_.get_position();
				log_buffer_.get_limit() = log_buffer_.get_position();
				ret = BLADE_READ_NOTHING;
			}
			else if (BLADE_SUCCESS != entry.CheckHeaderIntegrity())
			{
				log_buffer_.get_position() -= entry.GetSerializeSize();
				pread_pos_ -= log_buffer_.get_limit() - log_buffer_.get_position();
				log_buffer_.get_limit() = log_buffer_.get_position();
				LOGV(LL_ERROR, "Log entry header is corrupted, file_id_=%lu pread_pos_=%ld pos=%ld last_log_seq_=%lu",file_id_, pread_pos_, pos, last_log_seq_);
				LOGV(LL_ERROR, "log_buffer_ position_=%ld limit_=%ld capacity_=%ld", log_buffer_.get_position(), log_buffer_.get_limit(), log_buffer_.get_capacity());
				ret = BLADE_ERROR;
			}
			else if (entry.GetLogDataLen() > log_buffer_.get_remain_data_len())
			{
				log_buffer_.get_position() -= entry.GetSerializeSize();

				ret = ReadLog_();
				if (BLADE_READ_NOTHING == ret)
				{
					LOGV(LL_DEBUG, "do not get a full entry, when reading BladeLogEntry");
				}
				else
				{
					ret = entry.Deserialize(log_buffer_.get_data(), log_buffer_.get_limit(), log_buffer_.get_position());
					if (BLADE_SUCCESS != ret)
					{
						LOGV(LL_ERROR, "BladeLogEntry deserialize error[ret=%d]", ret);
					}
					else
					{
						if (entry.GetLogDataLen() > log_buffer_.get_remain_data_len())
						{
							LOGV(LL_DEBUG, "do not get a full entry, when checking log data");
							LOGV(LL_DEBUG, "log_data_len=%ld remaining=%ld", entry.GetLogDataLen(), log_buffer_.get_remain_data_len());
							LOGV(LL_DEBUG, "limit=%ld pos=%ld", log_buffer_.get_limit(), log_buffer_.get_position());
							hex_dump(log_buffer_.get_data(), log_buffer_.get_limit());
							log_buffer_.get_position() -= entry.GetSerializeSize();
							ret = BLADE_READ_NOTHING;
						}
					}
				}
			}
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		if (BLADE_SUCCESS == ret)
		{
			ret = entry.CheckDataIntegrity(log_buffer_.get_data() + log_buffer_.get_position());
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "data corrupt, when check_data_integrity, file_id_=%lu pread_pos_=%ld pos=%ld last_log_seq_=%lu", file_id_, pread_pos_, pos, last_log_seq_);
				LOGV(LL_ERROR, "log_buffer_ position_=%ld limit_=%ld capacity_=%ld", log_buffer_.get_position(), log_buffer_.get_limit(), log_buffer_.get_capacity());
				hex_dump(log_buffer_.get_data(), log_buffer_.get_limit(), true, LL_ERROR);
				ret = BLADE_ERROR;
			}
		}

		if (BLADE_SUCCESS == ret)
		{
			if (BLADE_LOG_SWITCH_LOG != entry.cmd_ && last_log_seq_ != 0 && (last_log_seq_ + 1) != entry.seq_)
			{
				LOGV(LL_ERROR, "the log sequence is not continuous[%lu => %lu]", last_log_seq_, entry.seq_);
				ret = BLADE_ERROR;
			}
		}

		if (BLADE_SUCCESS == ret)
		{
			if (BLADE_LOG_SWITCH_LOG != entry.cmd_)
			{
				last_log_seq_ = entry.seq_;
			}
			cmd = static_cast<LogCommand>(entry.cmd_);
			log_seq = entry.seq_;
			log_data = log_buffer_.get_data() + log_buffer_.get_position();
			data_len = entry.GetLogDataLen();
			log_buffer_.get_position() += data_len;
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		LOGV(LL_DEBUG, "LOG ENTRY: SEQ[%lu] CMD[%d] DATA_LEN[%ld] POS[%ld]", entry.seq_, cmd, data_len, pos);
		pos += entry.header_.header_length_ + entry.header_.data_length_;
	}

	return ret;
}

int BladeSingleLogReader::ReadLog_()
{
	int ret = BLADE_SUCCESS;

	if (log_buffer_.get_remain_data_len() > 0)
	{
		if (all_zero(log_buffer_.get_data() + log_buffer_.get_position(), log_buffer_.get_limit() - log_buffer_.get_position()))
		{
			pread_pos_ -= log_buffer_.get_limit() - log_buffer_.get_position();
			log_buffer_.get_limit() = log_buffer_.get_position();
		}
		else
		{
			memmove(log_buffer_.get_data(), log_buffer_.get_data() + log_buffer_.get_position(), log_buffer_.get_remain_data_len());
			log_buffer_.get_limit() = log_buffer_.get_remain_data_len();
			log_buffer_.get_position() = 0;
		}
	}
	else
	{
		log_buffer_.get_limit() = log_buffer_.get_position() = 0;
	}

	int64_t read_size = 0;
	ret = file_.pread(log_buffer_.get_data() + log_buffer_.get_limit(), log_buffer_.get_capacity() - log_buffer_.get_limit(), pread_pos_, read_size);
//	LOGV(LL_DEBUG, "pread:: pread_pos=%ld read_size=%ld buf_pos=%ld buf_limit=%ld", pread_pos_, read_size, log_buffer_.get_position(), log_buffer_.get_limit());
	if (BLADE_SUCCESS != ret)
	{
		LOGV(LL_ERROR, "read log file[file_id=%lu] ret=%d", file_id_, ret);
	}
	else
	{
		// comment this log due to too frequent invoke by replay thread
		//LOGV(LL_DEBUG, "read %dB amount data from log file[file_id=%lu fd=%d]", err, file_id_, log_fd_);
		log_buffer_.get_limit() += read_size;
		pread_pos_ += read_size;

		if (0 == read_size)
		{
			// comment this log due to too frequent invoke by replay thread
			//LOGV(LL_DEBUG, "reach end of log file[file_id=%d]", file_id_);
			ret = BLADE_READ_NOTHING;
		}
	}

	return ret;
}

bool BladeSingleLogReader::IsEntryZeroed(const BladeLogEntry &entry)
{
	return     entry.header_.magic_ == 0 
			&& entry.header_.header_length_ == 0
			&& entry.header_.version_ == 0
			&& entry.header_.header_checksum_ == 0
			&& entry.header_.reserved_ == 0
			&& entry.header_.data_length_ == 0
			&& entry.header_.data_zlength_ == 0
			&& entry.header_.data_checksum_ == 0
			&& entry.seq_ == 0
			&& entry.cmd_ == 0;
}

}//end of namespace ha
}//end of namespace bladestore
