#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>

#include "blade_trace_log.h"
#include "blade_nameserver_param.h"
#include "time_util.h"
#include "blade_common_define.h"

using namespace bladestore::nameserver;
using namespace bladestore::common;

namespace bladestore
{
namespace ha
{

const char *const TraceLog::LOG_LEVEL_ENV_KEY = "_BLADE_TRACE_LOG_LEVEL_";
const char *const TraceLog::level_strs_[] = {"ERROR", "WARN", "INFO", "DEBUG"};
volatile int TraceLog::log_level_ = LL_DEBUG;
bool TraceLog::got_env_ = false;

TraceLog::LogBuffer &TraceLog::GetLogbuffer()
{
	static __thread LogBuffer log_buffer;
	static __thread bool inited = false;
	if (!inited)
	{
		memset(&log_buffer, 0, sizeof(log_buffer));
		inited = true;
	}
	return log_buffer;
}

int TraceLog::DecLogLevel()
{
	int prev_log_level = log_level_;
	int modify_log_level = log_level_ - 1;
	if (LL_DEBUG >= modify_log_level && LL_ERROR <= modify_log_level)
	{
		log_level_ = modify_log_level;
	}
	LOGV(LL_INFO, "dec_log_level prev_log_level=%d cur_log_level=%d", prev_log_level, log_level_);
	return log_level_;
}

int TraceLog::IncLogLevel()
{
	int prev_log_level = log_level_;
	int modify_log_level = log_level_ + 1;
	if (LL_DEBUG >= modify_log_level && LL_ERROR <= modify_log_level)
	{
		log_level_ = modify_log_level;
	}
	LOGV(LL_INFO, "inc_log_level prev_log_level=%d cur_log_level=%d", prev_log_level, log_level_);
	return log_level_;
}

int TraceLog::SetLogLevel(const char *log_level_str)
{
	if (NULL != log_level_str)
	{
		int level_num = sizeof(level_strs_) / sizeof(const char*);
		for (int i = 0; i < level_num; ++i)
		{
			if (0 == strcasecmp(level_strs_[i], log_level_str))
			{
				log_level_ = i;
				break;
			}
		}
		got_env_ = true;
	}
	return log_level_;
}
        
int TraceLog::GetLogLevel()
{
	if (!got_env_)
	{
		const char *log_level_str = getenv(LOG_LEVEL_ENV_KEY);
		SetLogLevel(log_level_str);
	}
	return log_level_;
}

//主要的处理函数
void TraceLog::FillLog(const char *fmt, ...)
{
	//这里的LL_WARN可以根据需要设置
	if (GetLogLevel() > BladeNameServerParam::trace_log_level_)
	{
		LogBuffer &log_buffer = GetLogbuffer();
		if (0 == log_buffer.prev_timestamp)
		{
			log_buffer.start_timestamp = TimeUtil::GetTime();
			log_buffer.prev_timestamp = log_buffer.start_timestamp;
		}
	}
	else
	{
		va_list args;
        va_start(args, fmt);
        LogBuffer &log_buffer = GetLogbuffer();
        int64_t valid_buffer_size = LogBuffer::LOG_BUFFER_SIZE - log_buffer.cur_pos;
        if (0 < valid_buffer_size)
        {
			int64_t ret = 0;
			ret = vsnprintf(log_buffer.buffer + log_buffer.cur_pos, valid_buffer_size, fmt, args);
			log_buffer.cur_pos += ((0 < ret && valid_buffer_size > ret) ? ret : 0);
			if (-1 < ret && valid_buffer_size > ret)
			{
				valid_buffer_size -= ret;
				int64_t cur_time = TimeUtil::GetTime();
				if (0 < valid_buffer_size)
				{
					if (0 != log_buffer.prev_timestamp)
					{
						ret = snprintf(log_buffer.buffer + log_buffer.cur_pos, valid_buffer_size, " timeu=%lu | ", cur_time - log_buffer.prev_timestamp);
					}
					else
					{
						ret = snprintf(log_buffer.buffer + log_buffer.cur_pos, valid_buffer_size, " | ");
						log_buffer.start_timestamp = cur_time;
					}
					log_buffer.cur_pos += ((0 < ret && valid_buffer_size > ret) ? ret : 0);
				}
				log_buffer.prev_timestamp = cur_time;
			}
			log_buffer.buffer[log_buffer.cur_pos] = '\0';
        }
        va_end(args);
	}
}

void TraceLog::ClearLog()
{
	TraceLog::GetLogbuffer().cur_pos = 0;
	TraceLog::GetLogbuffer().prev_timestamp = 0;
	TraceLog::GetLogbuffer().start_timestamp = 0;
}

}//end of namespace ha
}//end of namespace bladestore
