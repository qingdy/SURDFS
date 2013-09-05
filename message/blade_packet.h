/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-3-28
 *
 */
#ifndef BLADESTORE_MESSAGE_PACKET_H
#define BLADESTORE_MESSAGE_PACKET_H
#include "blade_net_processor.h"
#include "blade_common_define.h"
#include "blade_data_buffer.h"
#include "amframe.h"
#include <string>

using namespace bladestore::common;

namespace bladestore
{
namespace message
{

using namespace pandora;

class BladePacket : public Packet
{
public:
	BladePacket();

	virtual ~BladePacket();
	
	virtual int Pack() = 0;

	virtual int Pack(BladeDataBuffer & data_buffer) 
	{
		return BLADE_SUCCESS;	
	}

	virtual int Unpack() = 0;

	virtual int Reply(BladePacket * resp_packet)
	{
		return BLADE_SUCCESS;
	}

    void init();

    virtual size_t GetLocalVariableSize() = 0;

	virtual BladePacket * copy_self() const 
	{
		return NULL;
	}

	void set_endpoint(AmFrame::StreamEndPoint * endpoint);
	void copy_content(Packet * packet);
	void set_net_data(NetDataProcessor * net_data);
	NetDataProcessor * get_net_data();
	virtual bool is_regular_packet();
	int16_t get_cmd_type();
	std::string get_name();
	int16_t get_operation()//TODO need to get rid of get
	{
		return operation_;	
	}
	int16_t operation()
	{
		return operation_;	
	}

	void set_log_forward(bool value)
	{
		log_forward_ = value;
	}
	
	void set_peer_id(uint64_t peer_id)
	{
		peer_id_ = peer_id;
	}
	
	uint64_t peer_id()
	{
		return peer_id_;
	}

public:
	AmFrame::StreamEndPoint  endpoint_;

protected:
	bool log_forward_;//用于NS主从之间

	bool is_regular_packet_;
	int16_t operation_;
	uint64_t peer_id_;
	std::string name_;
	NetDataProcessor * net_data_;
};

}//end of namespace message
}//end of namespace bladestore
#endif
