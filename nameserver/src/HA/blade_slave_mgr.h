/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_HA_SLAVE_MGR_H
#define BLADESTORE_HA_SLAVE_MGR_H

#include <stdint.h>

#include "thread_mutex.h"
#include "blade_link.h"
#include "blade_lease_common.h"
#include "blade_common_define.h"
#include "blade_client_manager.h"

using	namespace bladestore::nameserver;
using	namespace pandora;

namespace bladestore
{
namespace ha
{

class BladeSlaveMgr
{
public:
	static const int DEFAULT_VERSION;
	static const int DEFAULT_LOG_SYNC_TIMEOUT;  // ms
	static const int CHECK_LEASE_VALID_INTERVAL; // us
	static const int GRANT_LEASE_TIMEOUT; // ms
	static const int MASTER_LEASE_CHECK_REDUNDANCE; // ms
	static const int DEFAULT_SEND_LOG_RETRY_TIMES = 3;

	struct ServerNode
	{
		BladeDLink server_list_link;
        uint64_t server_;
        BladeLease lease_;

        bool is_lease_valid(int64_t redun_time)
        {
          return lease_.IsLeaseValid(redun_time);
        }

        void reset()
        {
          lease_.lease_time_ = 0;
          lease_.lease_interval_ = 0;
          lease_.renew_interval_ = 0;
        }
	};

public:
	BladeSlaveMgr();
	virtual ~BladeSlaveMgr();
	int Init(const uint32_t vip, BladeClientManager *rpc_stub, int64_t log_sync_timeout, int64_t lease_interval, int64_t lease_reserved_time, int64_t send_retry_times = DEFAULT_SEND_LOG_RETRY_TIMES);

	/// reset vip (for debug only)
	void ResetVip(const int32_t vip) 
	{
		vip_ = vip;
	}

	int AddServer(const uint64_t  server);
	int DeleteServer(const uint64_t  server);

	int SendData(const char* data, const int64_t length);

	inline int GetNum() const 
	{
		return slave_num_;
	}

	int ExtendLease(const uint64_t  server, BladeLease& lease);
	int CheckLeaseExpiration();
	bool IsLeaseValid(const uint64_t  server) const;

private:

	ServerNode* FindServer(const uint64_t  server);

	inline int CheckInnerStat() const
	{
		int ret = BLADE_SUCCESS;
		if (!is_initialized_)
		{
			LOGV(LL_ERROR, "BladeSlaveMgr has not been initialized.");
			ret = BLADE_NOT_INIT;
		}
		return ret;
	}

private:
	int64_t log_sync_timeout_;
	int64_t lease_interval_;
	int64_t lease_reserved_time_;
	int64_t send_retry_times_;
	int slave_num_;  //Slave个数
	uint32_t vip_;
	ServerNode slave_head_;  //slave链表头
	BladeClientManager * rpc_stub_;
	CThreadMutex slave_info_mutex_;
	bool is_initialized_;
};

} // end of namespace ha
} // end of namespace bladestore

#endif

