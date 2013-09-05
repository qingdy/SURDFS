/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_HA_LOG_READER_H 
#define BLADESTORE_HA_LOG_READER_H 

#include "blade_log_entry.h"
#include "blade_single_log_reader.h"

namespace bladestore
{
namespace ha
{

/**
 * 日志读取类, 从一个指定的日志id开始, 遇到日志文件结束, 则打开下一个日志文件
 * 主要有两种使用方法:
 *   1. Master启动时回放日志, 读到所有日志结束则回放完成
 *   2. Slave的日志回放线程使用: Slave采用一个线程同步日志, 另一个线程回放日志时, 
 *      回放线程不停追赶日志文件, 当读到日志结束时, 应该等待小段时间后, 再次读日志
 */
class BladeLogReader
{
public:
	static const int64_t WAIT_TIME = 1000000; //us
	static const int FAIL_TIMES = 60;

public:
	BladeLogReader();
	virtual ~BladeLogReader();
	int Init(const char* log_dir, const uint64_t log_file_id_start, bool is_wait);

	/**
	 * 读日志
	 * 遇到SWITCH_LOG日志时直接打开下一个日志文件
	 * 在打开下一个日志文件时, 如果遇到文件不存在, 可能是产生日志的地方正在切文件
	 * 等待1ms后重试, 最多重试10次, 如果还不存在则报错
	 * return BLADE_SUCCESS 成功
	 * return BLADE_READ_NOTHING 没有读取到内容, 可能是日志结束了, 
	 *                         也可能是日志正在产生, 读到了中间状态数据
	 *                         由调用者根据自己逻辑做不同处理
	 * return otherwise 失败
	 */
	int ReadLog(LogCommand &cmd, uint64_t &seq, char* &log_data, int64_t &data_len);

	inline void SetMaxLogFileId(uint64_t max_log_file_id)
	{
		__sync_lock_test_and_set(&max_log_file_id_, max_log_file_id);
		has_max_ = true;
	}

	inline uint64_t GetMaxLogFileId() const
	{
		return max_log_file_id_;
	}
	inline bool GetHasMax() const
	{
		return has_max_;
	}
	inline void SetHasNoMax()
	{
		has_max_ = false;
	}

private:
	int OpenLog_(const uint64_t log_file_id, const uint64_t last_log_seq = 0);
	int ReadLog_(LogCommand &cmd, uint64_t &log_seq, char *&log_data, int64_t &data_len);

private:
	uint64_t cur_log_file_id_;
	uint64_t max_log_file_id_;
	BladeSingleLogReader log_file_reader_;
	bool is_initialized_;
	bool is_wait_;
	bool has_max_;
};

} // end of namespace ha
} // end of namespace bladestore

#endif
