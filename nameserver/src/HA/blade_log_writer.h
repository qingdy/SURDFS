/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-8
 *
 */

#ifndef BLADESTORE_HA_LOG_WRITER_H
#define BLADESTORE_HA_LOG_WRITER_H

#include "blade_data_buffer.h"
#include "blade_slave_mgr.h"
#include "blade_lease_common.h"
#include "blade_log_entry.h"
#include "blade_file.h"

using namespace bladestore::common;

namespace bladestore
{
namespace ha
{

class BladeLogWriter
{
public:
	static const int64_t LOG_BUFFER_SIZE = 1966080L;  
	static const int64_t LOG_FILE_ALIGN_BIT = 9;
	static const int64_t LOG_FILE_ALIGN_SIZE = 1 << LOG_FILE_ALIGN_BIT;
	static const int64_t LOG_FILE_ALIGN_MASK = (-1) >> LOG_FILE_ALIGN_BIT << LOG_FILE_ALIGN_BIT;

public:
	BladeLogWriter();
	virtual ~BladeLogWriter();
	int Init(const char* log_dir, const int64_t log_file_max_size, BladeSlaveMgr *slave_mgr, int64_t log_sync_type);

	void ResetLog();
	int StartLog(const uint64_t log_file_max_id, const uint64_t log_max_seq);
	int StartLog(const uint64_t log_file_max_id);
	int WriteLog(const LogCommand cmd, const char* log_data, const int64_t data_len);
	int FlushLog();
	int WriteAndFlushLog(const LogCommand cmd, const char* log_data, const int64_t data_len);
	int StoreLog(const char* buf, const int64_t buf_len);
	int SwitchLogFile(uint64_t &new_log_file_id);
	int SwitchToLogFile(const uint64_t log_file_id);
	int WriteCheckpointLog(uint64_t &log_file_id);
	inline uint64_t GetCurLogFileId() 
	{
		return cur_log_file_id_;
	}
	inline void SetCurLogSeq(uint64_t cur_log_seq) 
	{
		cur_log_seq_ = cur_log_seq;
	}
	inline uint64_t GetCurLogSeq() 
	{
		return cur_log_seq_;
	}
	inline BladeSlaveMgr* GetSlaveMgr() 
	{
		return slave_mgr_;
	}
	inline int64_t GetLastNetElapse() 
	{
		return last_net_elapse_;
	}
	inline int64_t GetLastDiskElapse() 
	{
		return last_disk_elapse_;
	}

protected:

	int OpenLogFile(const uint64_t log_file_id, bool is_trunc);

	/// 将日志内容加上日志头部, 日志序号和LogCommand, 并序列化到缓冲区
	int SerializeLog(LogCommand cmd, const char* log_data, const int64_t data_len);
	int SerializeNopLog();

	/// 检查当前日志还够不够new_length长度的日志, 若不够则切日志
	int CheckLogFileSize(const int64_t new_length);

	inline int CheckInnerStat() const
	{
		int ret = BLADE_SUCCESS;
		if (!is_initialized_)
		{
			LOGV(LL_ERROR, "BladeLogWriter has not been initialized");
			ret = BLADE_NOT_INIT;
		}
		return ret;
	}

private:
	int64_t log_sync_type_;
	int64_t cur_log_size_;  //当前日志文件长度
	int64_t log_file_max_size_;  //日志文件最大长度
	uint64_t cur_log_file_id_;  //当前日志文件编号
	uint64_t cur_log_seq_;  //当前日志序号
	int64_t last_net_elapse_;  //上一次写日志网络同步耗时
	int64_t last_disk_elapse_;  //上一次写日志磁盘耗时
	BladeSlaveMgr *slave_mgr_;
	BladeDataBuffer log_buffer_;
	BladeFileAppender file_;
	char log_dir_[BLADE_MAX_FILENAME_LENGTH];  //日志目录
	char empty_log_[LOG_FILE_ALIGN_SIZE * 2];
	bool is_initialized_;
	bool dio_;
};

}//end of namespace ha
}//end of namespace bladestore

#endif
