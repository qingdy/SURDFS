/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-8
 *fun     : used by HA
 */
#ifndef BLADESTORE_COMMON_LEASE_COMMON_H
#define BLADESTORE_COMMON_LEASE_COMMON_H

#include <stdint.h>

namespace bladestore
{
namespace common
{
//从NS需要定期向主NS申请租约
struct BladeLease 
{
	int64_t lease_time_;      // lease start time
	int64_t lease_interval_;  // lease interval, lease valid time [lease_time, lease_time + lease_interval]
	int64_t renew_interval_;  // renew interval, slave will renew lease when lease expiration time is close

	BladeLease() : lease_time_(0), lease_interval_(0), renew_interval_(0) 
	{

	}

	bool IsLeaseValid(int64_t redun_time);
};

}//end of namespace common
}//end of namespace bladestore
#endif
