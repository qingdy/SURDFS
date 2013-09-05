/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_HA_TRACE_LOG_H  
#define BLADESTORE_HA_TRACE_LOG_H 

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>

#include "time_util.h"

using namespace pandora;

#define INC_TRACE_LOG_LEVEL() bladestore::ha::TraceLog::inc_log_level()
#define DEC_TRACE_LOG_LEVEL() bladestore::ha::TraceLog::dec_log_level()
#define SET_TRACE_LOG_LEVEL(level) bladestore::ha::TraceLog::set_log_level(level)
#define CLEAR_TRACE_LOG() bladestore::ha::TraceLog::clear_log()
#define FILL_TRACE_LOG(_fmt_, args...) bladestore::ha::TraceLog::fill_log("f=[%s] " _fmt_, __FUNCTION__, ##args)
#define GET_TRACE_TIMEU() (TimeUtil::GetTime() - bladestore::ha::TraceLog::get_logbuffer().start_timestamp)

namespace bladestore 
{
namespace ha
{

class TraceLog
{
public:
	static const char *const LOG_LEVEL_ENV_KEY;

	struct LogBuffer
	{
		static const int64_t LOG_BUFFER_SIZE = 4 * 1024;
		int64_t cur_pos;
		int64_t prev_timestamp;
		int64_t start_timestamp;
		char buffer[LOG_BUFFER_SIZE];
	};

public:
    static void ClearLog();
    static void FillLog(const char *fmt, ...);
    static void PrintLog();
    static LogBuffer &GetLogbuffer();
    static int SetLogLevel(const char *log_level_str);
    static int GetLogLevel();
    static int DecLogLevel();
    static int IncLogLevel();

private:
    static const char *const level_strs_[];
    static volatile int log_level_;
    static bool got_env_;
};

}//end of namespace ha 
}//end of namespace bladestore

#endif 

