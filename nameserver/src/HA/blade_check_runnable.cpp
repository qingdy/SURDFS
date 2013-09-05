#include "blade_check_runnable.h"
#include "blade_common_define.h"
#include "blade_net_util.h"
#include "time_util.h"
#include "blade_ns_lease_packet.h"
#include "log.h"

namespace bladestore
{
namespace ha
{

BladeCheckRunnable::BladeCheckRunnable()
{
	role_mgr_ = NULL;
	vip_ = 0;
	lease_on_ = false;
	stop_ = false;
	lease_interval_ = 0;
	lease_time_ = 0;
	renew_interval_ = 0;
	rpc_ = NULL;
	server_ = 0;
	renew_lease_timeout_ = 0;
	thread_ = new CThread();
}

BladeCheckRunnable::~BladeCheckRunnable()
{
	if(NULL != thread_)
	{
		delete thread_;	
		thread_ = NULL;
	}
}

void BladeCheckRunnable::Start()
{
	thread_->Start(this, NULL);
}

void BladeCheckRunnable::Stop()
{
	stop_ = true;
}

void BladeCheckRunnable::Wait()
{
	thread_->Join();
}

void BladeCheckRunnable::Run(CThread* thread, void* arg)
{
	UNUSED(thread);
	UNUSED(arg);
	int err = BLADE_SUCCESS;

	while (!stop_)
	{
		LOGV(LL_DEBUG, "IN CHECK RUNNABLE THREAD");
		if (BladeNetUtil::IsLocalAddr(vip_))
		{
			if (BladeRoleMgr::SLAVE == role_mgr_->GetRole())
			{
				struct in_addr vip_addr;
				vip_addr.s_addr = vip_;
				LOGV(LL_INFO, "vip found on slave, vip=%s", inet_ntoa(vip_addr));
				int64_t status = LEASE_NORMAL;
				if (lease_on_)
				{
					status = GetLeaseStatus();
				}

				if (LEASE_INVALID != status)
				{
					role_mgr_->SetState(BladeRoleMgr::SWITCHING);
				}
			}
		}
		else
		{
			if (BladeRoleMgr::MASTER == role_mgr_->GetRole())
			{
				struct in_addr vip_addr;
				vip_addr.s_addr = vip_;
				LOGV(LL_ERROR, "Incorrect vip, STOP server, vip=%s", inet_ntoa(vip_addr));
				role_mgr_->SetState(BladeRoleMgr::STOP);
				Stop();
			}
			else if (BladeRoleMgr::SLAVE == role_mgr_->GetRole() && lease_on_)
			{
				if (BladeRoleMgr::ACTIVE == role_mgr_->GetState())
				{
					int64_t status = GetLeaseStatus();
					if (LEASE_INVALID == status)
					{
						LOGV(LL_ERROR, "Invalid lease, set server to STOP state");
						role_mgr_->SetState(BladeRoleMgr::STOP); // set to stop state
					}
					else if (LEASE_SHOULD_RENEW == status)
					{
						err = RenewLease();
						if (BLADE_SUCCESS != err)
						{
							LOGV(LL_WARN, "failed to renew_lease_, err=%d", err);
						}
					}
				}
			}
		}
		usleep(check_period_);
	}
}

int BladeCheckRunnable::Init(BladeRoleMgr *role_mgr, const uint32_t vip, const int64_t check_period, BladeClientManager* rpc, uint64_t server, uint64_t slave_addr)
{
	int ret = BLADE_SUCCESS;

	if (NULL == role_mgr)
	{
		LOGV(LL_WARN, "Parameter is invalid[role_mgr=%p]", role_mgr);
		ret = BLADE_INVALID_ARGUMENT;
	}
	else
	{
		role_mgr_ = role_mgr;
		vip_ = vip;
		check_period_ = check_period;
		rpc_= rpc;
		server_ = server;
		slave_addr_ = slave_addr;
	}

	return ret;
}

void BladeCheckRunnable::ResetVip(const int32_t vip)
{
	vip_ = vip;
}

int64_t BladeCheckRunnable::GetLeaseStatus()
{
	LeaseStatus status = LEASE_NORMAL;
	int64_t cur_time_us = TimeUtil::GetTime();
	if (lease_time_ + lease_interval_ < cur_time_us)
	{
		LOGV(LL_WARN, "Lease expired, lease_time_=%ld, lease_interval_=%ld, cur_time_us=%ld", lease_time_, lease_interval_, cur_time_us);
		status = LEASE_INVALID;
	}
	else if (lease_time_ + lease_interval_ < cur_time_us + renew_interval_)
	{
		LOGV(LL_DEBUG, "Lease will expire, lease_time_=%ld, lease_interval_=%ld, cur_time_us=%ld, renew_interval_=%ld", lease_time_, lease_interval_, cur_time_us, renew_interval_);
		status = LEASE_SHOULD_RENEW;
	}

	return status;
}

int BladeCheckRunnable::RenewLease()
{
	int err = BLADE_SUCCESS;

	if (NULL == rpc_ || 0 == server_)
	{
		LOGV(LL_WARN, "invalid status, rpc_stub_=%p, server_=%p", rpc_, server_);
		err = BLADE_ERROR;
	}
	else
	{
		BladePacket * packet = NULL;
		err = rpc_->RenewLease(server_, slave_addr_, renew_lease_timeout_, packet);
		if (BLADE_SUCCESS != err)
		{
			LOGV(LL_WARN, "failed to renew lease, err=%d, timeout=%ld", err, renew_lease_timeout_);
		}
		else
		{
			BladeGrantLeasePacket * grant_lease_packet = static_cast<BladeGrantLeasePacket *>(packet);
			if(NULL == grant_lease_packet)
			{    
				err = BLADE_ERROR;
				LOGV(LL_ERROR, "packet static_cast error");
			}

			if (BLADE_SUCCESS == err) 
			{    
				BladeLease lease = grant_lease_packet->blade_lease();
				err = RenewLease(lease);
				if (BLADE_SUCCESS != err) 
				{
					LOGV(LL_WARN, "failed to extend lease, ret=%d", err);
				}
			}
		}
	}

	return err;
}

int BladeCheckRunnable::RenewLease(const BladeLease& lease)
{
	int err = BLADE_SUCCESS;

	lease_on_ = true;
	lease_time_ = lease.lease_time_;
	lease_interval_ = lease.lease_interval_;
	renew_interval_ = lease.renew_interval_;

	LOGV(LL_DEBUG, "Renew lease, lease_time=%ld lease_interval=%ld renew_interval=%ld", lease_time_, lease_interval_, renew_interval_);

	return err;
}

void BladeCheckRunnable::SetRenewLeaseTimeout(const int64_t renew_lease_timeout)
{
	renew_lease_timeout_ = renew_lease_timeout;
}

}//end of namespace ha
}//end of namespace bladestore
