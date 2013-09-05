#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "blade_log_entry.h"
#include "blade_log_dir_scanner.h"

namespace bladestore
{
namespace ha
{

int BladeLogFile::Assign(const char* filename, FileType &type)
{
	int ret = BLADE_SUCCESS;
	if (NULL == filename)
	{
	    LOGV(LL_ERROR, "parameter filename = NULL. should not reach here");
	    ret = BLADE_ERROR;
	}
	else
	{
		char basename[BLADE_MAX_FILENAME_LENGTH];
		int f_len = strlen(filename);
		if (f_len >= BLADE_MAX_FILENAME_LENGTH)
		{
			type = UNKNOWN;
		}
		else
		{
			int ckpt_len = strlen(DEFAULT_CKPT_EXTENSION);
			if (f_len > ckpt_len + 1 && 0 == strcmp(filename + f_len - ckpt_len, DEFAULT_CKPT_EXTENSION) && '.' == *(filename + f_len - ckpt_len - 1))
			{
				f_len -= ckpt_len + 1;
				strncpy(basename, filename, f_len);
				basename[f_len] = '\0';
				type = CKPT;
			}
			else
			{
				strncpy(basename, filename, f_len);
				basename[f_len] = '\0';
				type = LOG;
			}

			if (!IsLogId(basename))
			{
				type = UNKNOWN;
			}
			else
			{
				strcpy(name, filename);
				id = StrToUint64(basename);
			}
		}
	}

	return ret;
}

bool BladeLogFile::IsLogId(const char* str) const
{
	bool ret = true;

	if (NULL == str || '\0' == (*str) || '0' == (*str))
	{
		ret = false;
	}

	while('\0' != (*str))
	{
		if ((*str) < '0' || (*str) > '9')
		{
			ret = false;
			break;
		}
		str ++;
	}

	return ret;
}

uint64_t BladeLogFile::StrToUint64(const char* str) const
{
	if (NULL == str)
	{
		return 0U;
	}
	else
	{
		return atoll(str);
	}
}

bool BladeLogFile::operator< (const BladeLogFile& r) const
{
	return id < r.id;
}

BladeLogDirScanner::BladeLogDirScanner()
{
	min_log_id_ = 0;
	max_log_id_ = 0;
	max_ckpt_id_ = 0;
	has_log_ = false;
	has_ckpt_ = false;
	is_initialized_ = false;
}

BladeLogDirScanner::~BladeLogDirScanner()
{

}

int BladeLogDirScanner::Init(const char* log_dir)
{
	int ret = BLADE_SUCCESS;

	if (is_initialized_)
	{
		LOGV(LL_ERROR, "BladeLogDirScanner has been initialized.");
		ret = BLADE_INIT_TWICE;
	}
	else
	{
		ret = SearchLogDir(log_dir);
		if (BLADE_SUCCESS == ret)
		{
			is_initialized_ = true;
			LOGV(LL_INFO, "BladeLogDirScanner initialize successfully[min_log_id_=%lu max_log_id_=%lu max_ckpt_id_=%lu]", min_log_id_, max_log_id_, max_ckpt_id_);
		}
		else if (BLADE_DISCONTINUOUS_LOG == ret)
		{
			is_initialized_ = true;
			LOGV(LL_INFO, "BladeLogDirScanner initialize successfully[min_log_id_=%lu max_log_id_=%lu max_ckpt_id_=%lu], but log files in \"%s\" directory is not continuous", min_log_id_, max_log_id_, max_ckpt_id_, log_dir);
		}
		else
		{
			LOGV(LL_INFO, "search_log_dir_[log_dir=%s] error[ret=%d], BladeLogDirScanner initialize failed", log_dir, ret);
		}
	}

	return ret;
}

int BladeLogDirScanner::GetMinLogId(uint64_t &log_id) const
{
	int ret = CheckInnerStat();

	if (BLADE_SUCCESS == ret)
	{
		if (HasLog())
		{
			log_id = min_log_id_;
		}
		else
		{
			log_id = 0;
			ret = BLADE_ENTRY_NOT_EXIST;
		}
	}
	else
	{
    	log_id = 0;
	}

	return ret;
}

int BladeLogDirScanner::GetMaxLogId(uint64_t &log_id) const
{
	int ret = CheckInnerStat();

	if (BLADE_SUCCESS == ret)
	{
		if (HasLog())
		{
			log_id = max_log_id_;
		}
		else
		{
			log_id = 0;
			ret = BLADE_ENTRY_NOT_EXIST;
		}
	}
	else
	{
		log_id = 0;
	}

	return ret;
}

int BladeLogDirScanner::GetMaxCkptId(uint64_t &ckpt_id) const
{
	int ret = CheckInnerStat();

	if (BLADE_SUCCESS == ret)
	{
		if (HasCkpt())
		{
			ckpt_id = max_ckpt_id_;
		}
		else
		{
			ckpt_id = 0;
			ret = BLADE_ENTRY_NOT_EXIST;
		}
	}
	else
	{
		ckpt_id = 0;
	}

	return ret;
}

bool BladeLogDirScanner::HasLog() const
{
	return has_log_;
}

bool BladeLogDirScanner::HasCkpt() const
{
	return has_ckpt_;
}

int BladeLogDirScanner::SearchLogDir(const char* log_dir)
{
	int ret = BLADE_SUCCESS;

	int err = 0;

	std::vector<BladeLogFile> log_files;
	DIR* plog_dir = opendir(log_dir);
	if (NULL == plog_dir)
	{
		err = mkdir(log_dir, S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
		if (err != 0)
		{
			LOGV(LL_ERROR, "mkdir[\"%s\"] error[%s]", log_dir, strerror(errno));
			ret = BLADE_ERROR;
		}
		else
		{
			plog_dir = opendir(log_dir);
			if (NULL == plog_dir)
			{
				LOGV(LL_ERROR, "opendir[\"%s\"] error[%s]", log_dir, strerror(errno));
				ret = BLADE_ERROR;
			}
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		struct dirent entry;
		memset(&entry, 0x00, sizeof(struct dirent));
		struct dirent *pentry = &entry;

		BladeLogFile log_file;
		BladeLogFile::FileType file_type;

		err = readdir_r(plog_dir, pentry, &pentry);
		while (0 == err && NULL != pentry)
		{
			ret = log_file.Assign(pentry->d_name, file_type);
			if (ret != BLADE_SUCCESS)
			{
				break;
			}
			else
			{
				if (BladeLogFile::LOG == file_type)
				{
					log_files.push_back(log_file);
					LOGV(LL_INFO, "find a valid log file(\"%s\")", log_file.name);
				}
				else if (BladeLogFile::CKPT == file_type)
				{
					LOGV(LL_INFO, "find a valid checkpoint file(\"%s\")", log_file.name);
					if (max_ckpt_id_ < log_file.id)
					{
						max_ckpt_id_ = log_file.id;
						has_ckpt_ = true;
					}
				}
				else
				{
					LOGV(LL_DEBUG, "ignore file(\"%s\"): \"%s\" is not valid log file", pentry->d_name, pentry->d_name);
				}
				err = readdir_r(plog_dir, pentry, &pentry);
			}
		}

		if (0 != err)
		{
			LOGV(LL_ERROR, "readdir_r error(ret[%d], pentry[%p], plog_dir[%p]", err, pentry, plog_dir);
			ret = BLADE_ERROR;
		}
		err = closedir(plog_dir);
		if (err < 0)
		{
			LOGV(LL_ERROR, "closedir[DIR=%p] error[%s]", plog_dir, strerror(errno));
			ret = BLADE_SUCCESS;
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		std::sort(log_files.begin(), log_files.end());
		ret = CheckContinuity(log_files, min_log_id_, max_log_id_);
	}

	return ret;
}

int BladeLogDirScanner::CheckContinuity(const std::vector<BladeLogFile> &files, uint64_t &min_file_id, uint64_t &max_file_id)
{
	int ret = BLADE_SUCCESS;

	if (0 == files.size())
	{
		min_file_id = max_file_id = 0;
		has_log_ = false;
	}
	else
	{
		has_log_ = true;
		int size = files.size();
		min_file_id = max_file_id = files[size - 1].id;
		typedef std::vector<BladeLogFile>::iterator FileIter;
		//FileIter i = files.begin();
		int i = size - 1;
		uint64_t pre_id = files[i].id;
		i--;
		for (; i >= 0; --i)
		{
			if (files[i].id != (pre_id - 1))
			{
				break;
			}
			else
			{
				min_file_id = files[i].id;
			}
			pre_id = files[i].id;
		}

		if (i >= 0)
		{
			ret = BLADE_DISCONTINUOUS_LOG;
		}
	}

	return ret;
}

}//end of namespace ha
}//end of namespace bladestore
