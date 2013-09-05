/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_HA_LOG_DIR_SCANNER_H
#define BLADESTORE_HA_LOG_DIR_SCANNER_H
#include <vector>

#include "blade_common_define.h"
#include "log.h"

using namespace bladestore::common;
using namespace pandora;
using namespace std;

namespace bladestore
{
namespace ha 
{

struct BladeLogFile
{
	char name[BLADE_MAX_FILENAME_LENGTH];
	uint64_t id;

	enum FileType
	{
		UNKNOWN = 1,
		LOG = 2,
		CKPT = 3
	};

	int Assign(const char* filename, FileType &type);
	bool IsLogId(const char* str) const ;
	uint64_t StrToUint64(const char* str) const;
	bool operator < (const BladeLogFile & r) const;
};

class BladeLogDirScanner
{
public:
	BladeLogDirScanner();
	virtual ~BladeLogDirScanner();

	/**
	 * 初始化传入日志目录, BladeLogScanner类会扫描整个目录
	 * 对日志文件按照id进行排序, 得到最大日志号和最小日志号
	 * 同时获得最大的checkpoint号
	 *
	 * !!! 扫描过程中有一种异常情况, 即日志目录内的文件不连续
	 * 此时, 最小的连续日志被认为是有效日志
	 * 出现这种情况时, init返回值是BLADE_DISCONTINUOUS_LOG
	 *
	 */
	int Init(const char* log_dir);

	int GetMinLogId(uint64_t &log_id) const;
	int GetMaxLogId(uint64_t &log_id) const;
	int GetMaxCkptId(uint64_t &ckpt_id) const;
	bool HasLog() const;
	bool HasCkpt() const;
private:
	/**
	 * 遍历日志目录下的所有文件, 找到所有日志文件
	 */
	int SearchLogDir(const char* log_dir);

	/**
	 * 检查日志文件是否连续
	 * 同时获取最小和最大日志文件号
	 */
	int CheckContinuity(const std::vector<BladeLogFile> &files, uint64_t &min_file_id, uint64_t &max_file_id);

	inline int CheckInnerStat() const
	{
		int ret = BLADE_SUCCESS;
        if (!is_initialized_)
        {
			LOGV(LL_ERROR, "BladeLogDirScanner has not been initialized.");
			ret = BLADE_NOT_INIT;
        }
        return ret;
	}

private:
	uint64_t min_log_id_;
	uint64_t max_log_id_;
	uint64_t max_ckpt_id_;
	bool has_log_;
	bool has_ckpt_;
	bool is_initialized_;
};

}//end of namespace ha
}//end of namespace bladestore

#endif
