#include "blade_ns_log_replay.h"
#include "blade_common_define.h"
#include "blade_ns_log_manager.h"
#include "log.h"

using namespace pandora;
using namespace bladestore::common;

namespace bladestore
{
namespace ha
{

BladeNsLogReplay::BladeNsLogReplay()
{
	is_initialized_ = false;
}

BladeNsLogReplay::~BladeNsLogReplay()
{

}

void BladeNsLogReplay::SetLogManager(BladeNsLogManager * log_manager)
{
	blade_ns_log_manager_ = log_manager;
}

int BladeNsLogReplay::Replay(LogCommand cmd, uint64_t seq, char* log_data, const int64_t data_len)
{
	UNUSED(seq);
	assert(NULL != blade_ns_log_manager_);

	blade_ns_log_manager_->GetLogWorker()->Apply(cmd, log_data, data_len);

//	if (ret != BLADE_SUCCESS)
//	{
//		LOGV(LL_ERROR, "fatal error, replay log failed, err=[%d]. Quit...", ret);
//		exit(120);
//	}

	return BLADE_SUCCESS;
}

}//end of namespace ha
}//end of namespace bladestore

