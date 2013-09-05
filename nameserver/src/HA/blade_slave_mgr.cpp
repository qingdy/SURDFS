#include "blade_slave_mgr.h"
#include "time_util.h"
#include "blade_data_buffer.h"
#include "status_packet.h"
#include "blade_packet.h"
#include "blade_net_util.h"
#include "blade_common_define.h"

using namespace bladestore::message;
using namespace bladestore::common;

namespace bladestore
{
namespace ha
{

const int BladeSlaveMgr::DEFAULT_VERSION = 1;
const int BladeSlaveMgr::DEFAULT_LOG_SYNC_TIMEOUT = 500 * 1000;
const int BladeSlaveMgr::GRANT_LEASE_TIMEOUT = 1000000;
const int BladeSlaveMgr::CHECK_LEASE_VALID_INTERVAL = 10000;
const int BladeSlaveMgr::MASTER_LEASE_CHECK_REDUNDANCE = 1000000;

BladeSlaveMgr::BladeSlaveMgr()
{
	is_initialized_ = false;
	slave_num_ = 0;
	rpc_stub_ = NULL;
	lease_interval_ = 0;
	lease_reserved_time_ = 0;
}

BladeSlaveMgr::~BladeSlaveMgr()
{
	ServerNode* node = NULL;
	BladeDLink* p = slave_head_.server_list_link.Next();
	while (p != &slave_head_.server_list_link)
	{
		node = (ServerNode*)p;
		p = p->Next();
		p->Prev()->Remove();
		free(node);
	}
}

int BladeSlaveMgr::Init(const uint32_t vip, BladeClientManager *rpc_stub, int64_t log_sync_timeout, int64_t lease_interval, int64_t lease_reserved_time, int64_t send_retry_times/* = DEFAULT_SEND_LOG_RETRY_TIMES*/)
{
	int ret = BLADE_SUCCESS;

	if (is_initialized_)
	{
		ret = BLADE_INIT_TWICE;
	}
	else
	{
		if (NULL == rpc_stub)
		{
			LOGV(LL_ERROR, "Parameters is invlid[rpc_stub=%p]", rpc_stub);
			ret = BLADE_INVALID_ARGUMENT;
		}
	}


	if (BLADE_SUCCESS == ret)
	{
		vip_ = vip;
		rpc_stub_ = rpc_stub;
		log_sync_timeout_ = log_sync_timeout;
		lease_interval_ = lease_interval;
		lease_reserved_time_ = lease_reserved_time;
		send_retry_times_ = send_retry_times;
		is_initialized_ = true;
	}

	return ret;
}

int BladeSlaveMgr::AddServer(const uint64_t  server)
{
	int ret = BLADE_SUCCESS;

	slave_info_mutex_.Lock();
	ServerNode* node = FindServer(server);
	if (NULL != node)
	{
		std::string addr_str = BladeNetUtil::AddrToString(node->server_);
		LOGV(LL_INFO, "slave[%s] is already existed", addr_str.c_str());
	}
	else
	{
		ServerNode* item = (ServerNode*)malloc(sizeof(ServerNode));
		if (NULL == item)
		{
			LOGV(LL_ERROR, "slave_list_(Vector) push_back error[%d]", ret);
			ret = BLADE_ERROR;
		}
		else
		{
			item->reset();

			item->server_ = server;

			slave_head_.server_list_link.InsertPrev(item->server_list_link);

			slave_num_ ++;


			std::string addr_str = BladeNetUtil::AddrToString(server);
			LOGV(LL_INFO, "add a slave[%s], remaining slave number[%d]", addr_str.c_str(), slave_num_);
		}
	}
	slave_info_mutex_.Unlock();
	
	return ret;
}

int BladeSlaveMgr::DeleteServer(const uint64_t server)
{
	int ret = BLADE_SUCCESS;

	slave_info_mutex_.Lock();
	ServerNode* node = FindServer(server);


	if (NULL == node)
	{
		std::string addr_str = BladeNetUtil::AddrToString(server);
		LOGV(LL_WARN, "Server[%s] is not found", addr_str.c_str());
		ret = BLADE_ERROR;
	}
	else
	{
		std::string addr_str = BladeNetUtil::AddrToString(node->server_);
		node->server_list_link.Remove();
		free(node);
		slave_num_ --;

		LOGV(LL_INFO, "delete server[%s], remaining slave number[%d]", addr_str.c_str(), slave_num_);
	}
	slave_info_mutex_.Unlock();

	return ret;
}


//向slave server发送log信息, 实现HA非常重要的一步
int BladeSlaveMgr::SendData(const char* data, int64_t length)
{
	int ret = CheckInnerStat();
	BladeDataBuffer send_buf;
	ServerNode failed_head;

	if (BLADE_SUCCESS == ret)
	{
		if (NULL == data || length < 0)
		{
			LOGV(LL_ERROR, "parameters are invalid[data=%p length=%ld]", data, length);
			ret = BLADE_INVALID_ARGUMENT;
		}
		else
		{
			send_buf.set_data(const_cast<char*>(data), length);
			send_buf.get_position() = length;
		}
	}

	slave_info_mutex_.Lock();

	if (BLADE_SUCCESS == ret)
	{
		ServerNode* slave_node = NULL;
		BladeDLink* p = slave_head_.server_list_link.Next();
		while (BLADE_SUCCESS == ret && p != NULL && p != &slave_head_.server_list_link)
		{
			slave_node = (ServerNode*)(p);
			int err = 0;

			for (int64_t i = 0; i < send_retry_times_; i++)
			{
				int64_t send_beg_time = TimeUtil::GetMonotonicTime();
				BladePacket * response_packet = NULL;
				LOGV(LL_DEBUG, "send log to : %s", BladeNetUtil::AddrToString(slave_node->server_).c_str());
				err = rpc_stub_->send_log(slave_node->server_, send_buf, log_sync_timeout_, response_packet);
				StatusPacket * status_packet = static_cast<StatusPacket *>(response_packet);
				if ((BLADE_SUCCESS == err) && (NULL != status_packet) && (status_packet->get_status() == STATUS_OK))
				{
					if(NULL != response_packet)
					{
						delete response_packet;
					}
					response_packet = NULL;
					break;
				}
				else if (i + 1 < send_retry_times_)
				{
					int64_t send_elsp_time = TimeUtil::GetMonotonicTime() - send_beg_time;
					if (send_elsp_time < log_sync_timeout_)
					{
						usleep(log_sync_timeout_ - send_elsp_time);
					}
				}

				if(NULL != response_packet)
				{
					delete response_packet;
				}
				response_packet = NULL;
			}

			if (BLADE_SUCCESS != err)
			{
				if (!BladeNetUtil::IsLocalAddr(vip_))
				{
					LOGV(LL_ERROR, "VIP has gone");
					ret = BLADE_ERROR;
					p = p->Next();
				}
				else
				{
					std::string addr_str = BladeNetUtil::AddrToString(slave_node->server_);
					LOGV(LL_WARN, "send_data to slave[%s] error[err=%d]", addr_str.c_str(), err);

					BladeDLink *to_del = p;
					p = p->Next();
					to_del->Remove();
					slave_num_ --;
					failed_head.server_list_link.InsertPrev(*to_del);
				}
			}
			else
			{
				p = p->Next();
			}

		}// end of loop

		if (NULL == p)
		{
			LOGV(LL_ERROR, "Server list encounter NULL pointer, this should not be reached");
			ret = BLADE_ERROR;
		}
	}

	slave_info_mutex_.Unlock();

	if (BLADE_SUCCESS == ret)
	{
		ServerNode* slave_node = NULL;
		BladeDLink* p = failed_head.server_list_link.Next();
		while (p != NULL && p != &failed_head.server_list_link)
		{
			slave_node = (ServerNode*)(p);
			while (slave_node->is_lease_valid(MASTER_LEASE_CHECK_REDUNDANCE))
			{
				usleep(CHECK_LEASE_VALID_INTERVAL);
			}

			std::string addr_str = BladeNetUtil::AddrToString(slave_node->server_);
			LOGV(LL_WARN, "Slave[%s]'s lease is expired and has been removed", addr_str.c_str());

			p = p->Next();
			p->Prev()->Remove();
			free(slave_node);
			ret = BLADE_PARTIAL_FAILED;
		}

		if (NULL == p)
		{
			LOGV(LL_ERROR, "Server list encounter NULL pointer, this should not be reached");
			ret = BLADE_ERROR;
		}
	}

	return ret;
}

int BladeSlaveMgr::ExtendLease(const uint64_t server, BladeLease& lease)
{
	int ret = BLADE_SUCCESS;

	slave_info_mutex_.Lock();

	ServerNode* node = FindServer(server);
	if (NULL == node)
	{
		slave_info_mutex_.Unlock();

		std::string addr_str = BladeNetUtil::AddrToString(server);
		LOGV(LL_WARN, "Server[%s] is not found", addr_str.c_str());
		ret = BLADE_ERROR;
	}
	else
	{
		node->lease_.lease_time_ = TimeUtil::GetTime();
		node->lease_.lease_interval_ = lease_interval_;
		node->lease_.renew_interval_ = lease_reserved_time_;
		lease = node->lease_;

		slave_info_mutex_.Unlock();

//		ret = rpc_stub_->grant_lease(server, lease, GRANT_LEASE_TIMEOUT);
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_WARN, "grant_lease error, ret=%d", ret);
		}
		else
		{
			std::string addr_str = BladeNetUtil::AddrToString(server);
			LOGV(LL_DEBUG, "grant lease to Slave[%s], lease_time=%ld lease_internval=%ld renew_interval=%ld", addr_str.c_str(), lease.lease_time_, lease.lease_interval_, lease.renew_interval_);
		}
	}

	return ret;
}

int BladeSlaveMgr::CheckLeaseExpiration()
{
	LOGV(LL_DEBUG, "TODO: check_lease_expiration");
	return BLADE_SUCCESS;
}

bool BladeSlaveMgr::IsLeaseValid(const uint64_t  server) const
{
	std::string addr_str = BladeNetUtil::AddrToString(server);
	LOGV(LL_DEBUG, "TODO: is_lease_valid of Slave[%s]", addr_str.c_str());
	return false;
}

BladeSlaveMgr::ServerNode* BladeSlaveMgr::FindServer(const uint64_t  server)
{
	ServerNode* res = NULL;

	ServerNode* node = NULL;
	BladeDLink* p = slave_head_.server_list_link.Next();
	while (p != &slave_head_.server_list_link)
	{
		node = (ServerNode*)p;
		if (node->server_ == server)
		{
			res = node;
			break;
		}

		p = p->Next();
	}
	return res;
}

}//end of namespace ha
}//end of namespace bladestore
