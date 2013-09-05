#include <sys/time.h>

#include "blade_lease_common.h"
#include "log.h"

using namespace pandora;

namespace bladestore
{
namespace common
{

//time in usec
bool BladeLease::IsLeaseValid(int64_t redun_time)
{
	bool ret = true;

	timeval time_val;
	gettimeofday(&time_val, NULL);
	int64_t cur_time_us = time_val.tv_sec * 1000 * 1000 + time_val.tv_usec;
	if (lease_time_ + lease_interval_ + redun_time < cur_time_us)
	{
		LOGV(LL_INFO, "Lease expired, lease_time=%ld, lease_interval=%ld, cur_time_us=%ld", lease_time_, lease_interval_, cur_time_us);
		ret = false;
	}

	return ret;
}

}//end of namespace common
}//end of namespace bladestore
