/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_HA_SINGLE_LOG_READER_H
#define BLADESTORE_HA_SINGLE_LOG_READER_H

#include "blade_common_define.h"
#include "blade_data_buffer.h"
#include "blade_log_entry.h"
#include "blade_file.h"
#include "log.h"

using namespace pandora;
using namespace bladestore::common;

namespace bladestore
{
namespace ha
{

class BladeSingleLogReader
{
public:
	static const int64_t LOG_BUFFER_MAX_LENGTH;

public:

	BladeSingleLogReader();
	virtual ~BladeSingleLogReader();
	int Init(const char* log_dir);

	int Open(const uint64_t file_id, const uint64_t last_log_seq = 0);
	int Close();
	int Reset();

	int ReadLog(LogCommand &cmd, uint64_t &log_seq, char *&log_data, int64_t &data_len);


	inline bool IsOpened() const 
	{
		return file_.is_opened();
	}

protected:
	int ReadLog_();

	bool IsEntryZeroed(const BladeLogEntry &entry);

	inline int CheckInnerStat()
	{
		int ret = BLADE_SUCCESS;
		if (!is_initialized_)
		{
			LOGV(LL_ERROR, "BladeSingleLogReader has not been initialized");
			ret = BLADE_NOT_INIT;
		}
		return ret;
	}

private:
	uint64_t file_id_;  //日志文件id
	uint64_t last_log_seq_;  //上一条日志(Mutator)序号
	BladeDataBuffer log_buffer_;  //读缓冲区
	char file_name_[BLADE_MAX_FILENAME_LENGTH];  //日志文件名
	char log_dir_[BLADE_MAX_FILENAME_LENGTH];  //日志目录
	int64_t pos;
	int64_t pread_pos_;
	BladeFileReader file_;
	bool is_initialized_;  //初始化标记
	bool dio_;
};

}//end of namespace ha
}//end of namespace bladestore

#endif
