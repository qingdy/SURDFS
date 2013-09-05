/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-21
 *
 */
#ifndef	BLADESTORE_HA_CHECK_RUNNABLE_H 
#define BLADESTORE_HA_CHECK_RUNNABLE_H 

#include "blade_role_mgr.h"
#include "cthread.h"
#include "blade_lease_common.h"
#include "blade_client_manager.h"

using namespace pandora;
using namespace bladestore::nameserver;

namespace bladestore
{
namespace ha
{

//主备nameserver进行vip检测
class BladeCheckRunnable : public Runnable
{
public:
	BladeCheckRunnable();
	virtual ~BladeCheckRunnable();
	int Init(BladeRoleMgr *role_mgr, const uint32_t vip, const int64_t check_period, BladeClientManager * rpc = NULL, uint64_t  master = 0, uint64_t slave_addr = 0);

	virtual void Run(CThread* thread, void* arg);

	int RenewLease(const BladeLease& lease);
	void SetRenewLeaseTimeout(const int64_t renew_lease_timeout);
	// reset vip (for debug only)
	void ResetVip(const int32_t vip);

	void Start();
	void Wait();
	void Stop();

private:
	enum LeaseStatus
	{
		LEASE_NORMAL = 0,
		LEASE_SHOULD_RENEW = 1, // should renew lease
		LEASE_INVALID = 2,
	};

	// get lease status
	int64_t GetLeaseStatus();
	// renew lease
	int RenewLease();

private:
	bool stop_;
	BladeRoleMgr *role_mgr_;
	CThread * thread_;
	int64_t check_period_;
	uint32_t vip_;
	bool lease_on_;
	int64_t lease_interval_;
	int64_t renew_interval_;
	int64_t lease_time_;
	BladeClientManager * rpc_;
	uint64_t server_;
	uint64_t slave_addr_;
	int64_t renew_lease_timeout_;
};

} // end namespace ha
} // end namespace bladestore

#endif

