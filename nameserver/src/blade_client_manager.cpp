#include "blade_client_manager.h"
#include "blade_common_define.h"
#include "blade_common_data.h"
#include "blade_wait_object.h"
#include "connetion_mgr_list.h"
#include "blade_net_util.h"
#include "blade_ns_lease_packet.h"
#include "blade_log_packet.h"
#include "blade_slave_register_packet.h"
#include "block_to_get_length_packet.h"

namespace bladestore
{
namespace nameserver
{

BladeClientManager::BladeClientManager() : inited_(false), max_request_timeout_(5000000), connmgr_(NULL), waitmgr_(NULL)
{

}

BladeClientManager::~BladeClientManager()
{
	Destroy();
}

void BladeClientManager::Destroy()
{
	if (NULL != connmgr_) 
	{
		delete connmgr_;
		connmgr_ = NULL;
	}

	if (NULL != waitmgr_) 
	{
		delete waitmgr_;
		waitmgr_ = NULL;
	}
}

int BladeClientManager::Initialize(const int64_t max_request_timeout /*=5000000*/)
{
	int rc = BLADE_SUCCESS;
	if (0 != inited_)
	{
		LOGV(LL_ERROR, "ClientManager already initialized.");
		rc = BLADE_INIT_TWICE;
	}

	if (BLADE_SUCCESS == rc)
	{
		waitmgr_ = new (std::nothrow) WaitObjectManager();
		if (NULL == waitmgr_)
		{
			LOGV(LL_ERROR, "cannot allocate WaitObjectManager object.");
			rc = BLADE_ERROR;
		}

		max_request_timeout_ = max_request_timeout;
		connmgr_ =  new ClientManagerConnectMgr(waitmgr_);
		if (NULL == connmgr_)
		{
			LOGV(LL_ERROR, "cannot allocate ClientManager object.");
			rc = BLADE_INVALID_ARGUMENT;
		}
		else
		{
			//ConnectionManager use time in ms;
			//connmgr_->setDefaultQueueTimeout(0, max_request_timeout_ / 1000);
		}
	}

	inited_ = (BLADE_SUCCESS == rc);
	if (0 == inited_)
	{
		Destroy();
	}

	return rc;
}

/**
 * post_packet is async version of send_packet. donot wait for response packet.
 */
int BladeClientManager::post_packet(const uint64_t server, BladePacket* packet) const
{
	int rc = BLADE_SUCCESS;
	if (NULL == packet) 
	{
		rc = BLADE_INVALID_ARGUMENT;
	}
	else if (0 == inited_) 
	{
		rc = BLADE_NOT_INIT;
		LOGV(LL_ERROR, "cannot post packet, ClientManager not initialized.");
		delete packet;
		packet = NULL;
	}
	else
	{
		bool send_ok = connmgr_->send_packet(server, packet, NULL);
		if (0 == send_ok)
		{
			rc = BLADE_ERROR;
			LOGV(LL_INFO, "cannot post packet, maybe send queue is full or disconnect.");
			delete packet;
			packet = NULL;
		}
	}

	return rc;
}


/*
 * send a packet to server %server and wait response packet
 * @param server send to server
 * @param packet send packet object, must be allocate on heap, 
 * if send_packet failed, packet will be free by send_packet.
 * @param timeout max wait time interval
 * @param [out] response  response packet from remote server, allocated on heap, 
 * must be free by user who call the send_packet. response not NULL when return success.
 * @return BLADE_SUCCESS on success or other on failure.
 */
int BladeClientManager::send_packet(const uint64_t server, BladePacket* packet, const int64_t timeout, BladePacket* &response) const
{
	response = NULL;
	int rc = BLADE_SUCCESS;
	if (NULL == packet) 
	{
		rc = BLADE_INVALID_ARGUMENT;
	}
	else if (0 == inited_) 
	{
		rc = BLADE_NOT_INIT;
		LOGV(LL_ERROR, "cannot send packet, ClientManager not initialized.");
		delete packet;
		packet = NULL;
	}

	WaitObject* wait_object = NULL;
	if (BLADE_SUCCESS == rc)
	{
		wait_object = waitmgr_->create_wait_object();
		if (NULL == wait_object)
		{
			LOGV(LL_ERROR, "cannot send packet, cannot create wait object");
			rc = BLADE_ERROR;
		}
	}

	if (BLADE_SUCCESS == rc) 
	{
		if (timeout > max_request_timeout_)
		{
			max_request_timeout_ = timeout;
		}

		// caution! wait_object set no free, it means response packet
		// not be free by wait_object, must be handled by user who call send_packet.
		// MODIFY: wait_object need free the response packet not handled.
		// wait_object->set_no_free();
		WakeStruct * wake_struct = new WakeStruct(); 
		wake_struct->id_ = wait_object->get_id();
		bool send_ok = connmgr_->send_packet(server, packet, (void *)wake_struct);
		if (0 != send_ok)
		{
			send_ok = wait_object->wait_done(1);
			if (0 == send_ok)
			{
				LOGV(LL_ERROR, "wait packet (%d)'s response timeout.", packet->get_operation());
				rc = BLADE_RESPONSE_TIME_OUT;
			}
			else
			{
				response = dynamic_cast<BladePacket*>(wait_object->get_packet());
				rc = (NULL !=  response) ? BLADE_SUCCESS : BLADE_RESPONSE_TIME_OUT;
			}
		}
		else
		{
			rc = BLADE_ERROR;
			LOGV(LL_ERROR, "cannot send packet, maybe send queue is full or disconnect.");
		}

		// do not free the response packet.
		waitmgr_->destroy_wait_object(wait_object);
		wait_object = NULL;
	}

	return rc;
}

int BladeClientManager::send_get_length_packet(const set<uint64_t> servers, BladePacket* packet, const int64_t timeout, std::vector<BladePacket *> & response) 
{
	response.clear() ;
	int rc = BLADE_SUCCESS;
	if (NULL == packet) 
	{
		rc = BLADE_INVALID_ARGUMENT;
	}
	else if (0 == inited_) 
	{
		rc = BLADE_NOT_INIT;
		LOGV(LL_ERROR, "cannot send packet, ClientManager not initialized.");
		delete packet;
		packet = NULL;
	}

	WaitObject* wait_object = NULL;
	if (BLADE_SUCCESS == rc)
	{
		wait_object = waitmgr_->create_wait_object();
		if (NULL == wait_object)
		{
			LOGV(LL_ERROR, "cannot send packet, cannot create wait object");
			rc = BLADE_ERROR;
		}
	}

	if (BLADE_SUCCESS == rc) 
	{
		if (timeout > max_request_timeout_)
		{
			max_request_timeout_ = timeout;
		}

		set<uint64_t>::iterator iter = servers.begin();
		for (; iter != servers.end(); iter++)
		{
			BlockToGetLengthPacket * get_length_packet = (static_cast<BlockToGetLengthPacket *>(packet))->CopySelf();
			WakeStruct * wake_struct = new WakeStruct(); 
			wake_struct->id_ = wait_object->get_id();
            get_length_packet->Pack(); 
			bool ret = connmgr_->send_packet(*iter, get_length_packet, (void *)wake_struct);
            if (0 != ret)
            {
				LOGV(LL_DEBUG, "send to dataserver OK: %s", BladeNetUtil::AddrToString(*iter).c_str());
            }
            else
            {
				LOGV(LL_DEBUG, "send to dataserver ERROR: %s", BladeNetUtil::AddrToString(*iter).c_str());
            }
		}

		delete packet;
		packet = NULL;

		bool wait_ok = wait_object->wait_done(servers.size());
		if (0 == wait_ok)
		{
			rc = BLADE_RESPONSE_TIME_OUT;
		}

		for (int i = 0; i < servers.size(); i++)
		{
			if (NULL != wait_object->get_packet(i))	
			{
				response.push_back(wait_object->get_packet(i));
			}
			else
			{
				break;
			}
		}

        LOGV(LL_DEBUG, "RECECIVE DS SIZE : %d", response.size());

		// do not free the response packet.
		waitmgr_->destroy_wait_object(wait_object);
		wait_object = NULL;
	}
	return rc;
}

int BladeClientManager::send_log(const uint64_t server, BladeDataBuffer & data_buffer, int64_t log_sync_timeout, BladePacket * & response_packet)
{
	int return_code = BLADE_SUCCESS;
	BladeLogPacket * log_packet = new BladeLogPacket();
	log_packet->set_data(data_buffer.get_data(), data_buffer.get_position());		
	log_packet->Pack();
	
	return_code= send_packet(server, log_packet, log_sync_timeout, response_packet);

	if (BLADE_SUCCESS != return_code)
	{
		return_code = BLADE_ERROR;
		LOGV(LL_INFO, "cannot send log , maybe send queue is full or disconnect.");
		if(NULL != response_packet)
		{
			delete response_packet;
		}
		response_packet = NULL;
	}

	return return_code;
}

int BladeClientManager::slave_register(const uint64_t ns_master, const uint64_t self_addr, BladeFetchParam & fetch_param, int64_t network_timeout)	
{
	int return_code = BLADE_SUCCESS;
	BladeSlaveRegisterPacket * packet = new BladeSlaveRegisterPacket();
	packet->slave_id_= self_addr;	
	packet->Pack();

	BladePacket * response_packet = NULL;
	return_code = send_packet(ns_master, packet, network_timeout, response_packet);

	if (return_code != BLADE_SUCCESS)
	{
		LOGV(LL_INFO, "cannot post packet, maybe send queue is full or disconnect.");
		if (NULL != response_packet)
		{
			delete response_packet;
		}
		response_packet = NULL;
	}
	else
	{
		BladeSlaveRegisterReplyPacket * reply_packet = static_cast<BladeSlaveRegisterReplyPacket *>(response_packet);
		if (NULL == reply_packet)
		{
			return_code = BLADE_ERROR;	
		}
		else
		{
			fetch_param = reply_packet->fetch_param();
			if (NULL != response_packet)
			{
				delete response_packet;
			}
			response_packet = NULL;
		}
	}

	return return_code;
}

int BladeClientManager::RenewLease(uint64_t server, uint64_t slave_addr, int64_t renew_lease_timeout, BladePacket * & response)
{
	int return_code = BLADE_SUCCESS;
	BladeRenewLeasePacket * packet = new BladeRenewLeasePacket();
	packet->ds_id_ = slave_addr;	
	packet->Pack();

	BladePacket * response_packet = NULL;
	return_code = send_packet(server , packet, renew_lease_timeout, response_packet);

	if (return_code != BLADE_SUCCESS)
	{
		LOGV(LL_INFO, "cannot post packet, maybe send queue is full or disconnect.");
		if (NULL != response_packet)
		{
			delete response_packet;
		}
		response_packet = NULL;
	}
	return return_code;
}

}//end of namespace nameserver
}//end of namespace bladestore
