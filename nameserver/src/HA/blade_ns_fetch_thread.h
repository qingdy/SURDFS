/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_NS_FETCH_THREAD_H
#define BLADESTORE_NS_FETCH_THREAD_H

#include <stdint.h>

#include "blade_fetch_runnable.h"

namespace bladestore
{
namespace ha
{
class BladeNsLogManager;

class BladeNsFetchThread : public BladeFetchRunnable
{
public:
	const static int64_t WAIT_INTERVAL;

public:
	BladeNsFetchThread();

	int WaitRecoverDone();

	int GotCkpt(uint64_t ckpt_id);

	void LogManager(BladeNsLogManager* log_manager);

private:
	int recover_ret_;
	bool is_recover_done_;
	BladeNsLogManager* log_manager_;
};

}//end of namespace ha
}//end of namespace bladestore

#endif 

