#include "blade_ns_fetch_thread.h"
#include "blade_ns_log_manager.h"

namespace bladestore
{
namespace ha
{

const int64_t BladeNsFetchThread::WAIT_INTERVAL = 100 * 1000; // 100 ms

BladeNsFetchThread::BladeNsFetchThread() : recover_ret_(BLADE_SUCCESS), is_recover_done_(false), log_manager_(NULL)
{

}

void BladeNsFetchThread::LogManager(BladeNsLogManager* log_manager)
{
	log_manager_ = log_manager;
}

int BladeNsFetchThread::WaitRecoverDone()
{
	int count = 0;
	while (!is_recover_done_)
	{
		count++;
		if (0 == (count % 100))
		{
			LOGV(LL_INFO, "wait recover from check point used %d seconds", (count/10));
		}
		usleep(WAIT_INTERVAL);
	}

	return recover_ret_;
}

int BladeNsFetchThread::GotCkpt(uint64_t ckpt_id)
{

	LOGV(LL_DEBUG, "fetch got checkpoint %d", ckpt_id);

	if (NULL == log_manager_)
	{
		recover_ret_ = BLADE_INVALID_ARGUMENT;
		LOGV(LL_ERROR, "log manager is not set");
	}
	else
	{
		recover_ret_ = log_manager_->RecoverCheckpoint(ckpt_id);
	}

	is_recover_done_ = true;

	LOGV(LL_INFO, "recover from checkpoint %d return %d", ckpt_id, recover_ret_);

	return recover_ret_;
}

}//end of namespace ha
}//end of namespace bladestore

