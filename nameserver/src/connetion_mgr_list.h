/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-8
 *
 */

#ifndef BLADESTORE_NAMESERVER_CONN_MGR_LIST_H
#define BLADESTORE_NAMESERVER_CONN_MGR_LIST_H

#include "amframe.h"
#include "blade_common_data.h"
#include "blade_socket_handler.h"
#include "blade_wait_object.h"

using namespace pandora;
using namespace bladestore::message;

namespace bladestore
{
namespace nameserver
{


class ClientManagerSocketHandker : public BladeStreamSocketHandler
{
public:
	ClientManagerSocketHandker(AmFrame & amframe, WaitObjectManager * waitmgr) : BladeStreamSocketHandler(amframe), waitmgr_(waitmgr) 
	{
		assert(NULL != waitmgr_);
		packet_factory_ = new BladePacketFactory();
	}

	~ClientManagerSocketHandker() 
	{
		if (NULL != packet_factory_)
		{
			delete packet_factory_;
		}
		packet_factory_ = NULL;
	}
	
public:
	void OnReceived(const Packet& packet, void * client_data)
	{
		LOGV(LL_DEBUG, "IN ON RECEIVED");
		int16_t operation;
    	unsigned char * data_data = const_cast<unsigned char *>(packet.content());
	   	if (4 == sizeof(size_t))
    	{   
       		uint16_t my_data = (unsigned char)data_data[4];
       		my_data <<= 8;
       		my_data |=(unsigned char) data_data[5];
       		operation = my_data;
    	}
    	else if (8 == sizeof(size_t))
    	{   
       		uint16_t my_data = (unsigned char)data_data[8];
       		my_data <<= 8;
       		my_data |=(unsigned char) data_data[9];
       		operation = my_data;
    	}   
    	else 
    	{
       		LOGV(LL_ERROR, "sizeof(size_t) != 4 && 8");
       		return ;
    	}	   

    	BladePacket * blade_packet =  packet_factory_->create_packet(operation);    
		LOGV(LL_DEBUG, "operation type : %d", operation);
    	blade_packet->get_net_data()->set_read_data(packet.content(), packet.length());
    	blade_packet->Unpack();
    	blade_packet->endpoint_ = BladeStreamSocketHandler::end_point();
    	blade_packet->set_channel_id(packet.channel_id());

		if (NULL != client_data)
		{
			int id =((WakeStruct *)(client_data))->id_;
			waitmgr_->wakeup_wait_object(id, blade_packet);
			delete (WakeStruct *)client_data;
		}
		else
		{
			//when args = NULL, means wo dont care the response
		}
	}
	
	void OnTimeout(void * client_data)
	{
		LOGV(LL_DEBUG, "IN ON TIMEOUT");
		if (NULL != client_data)
		{
			int id =((WakeStruct *)(client_data))->id_;
			LOGV(LL_DEBUG , "****************id : %d*****", id);
			waitmgr_->wakeup_wait_object(id, NULL);
			delete (WakeStruct *)client_data;
		}
		else
		{
			//do nothing now, maybe do something later
		}
	}

private:
	CThreadMutex mutex_;
	BladePacketFactory * packet_factory_;
	WaitObjectManager* waitmgr_;
};

class ClientManagerConnectMgr : public ConnectionManager
{
public:
	ClientManagerConnectMgr(WaitObjectManager * waitmgr) : ConnectionManager(), waitmgr_(waitmgr)
	{
		
	}

	~ClientManagerConnectMgr()
	{

	}

	StreamSocketHandler * create_stream_socket_handler() 	
	{
		return new ClientManagerSocketHandker(BladeCommonData::amframe_, waitmgr_);
	}

private:
	WaitObjectManager * waitmgr_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
