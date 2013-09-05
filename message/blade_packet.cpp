#include <assert.h>
#include "blade_packet.h"

namespace bladestore
{
namespace message
{

BladePacket::BladePacket() : is_regular_packet_(false), peer_id_(0)
{
    init();
}

BladePacket::~BladePacket()
{
	if(NULL != net_data_)
	{
    	delete net_data_;
		net_data_ = NULL;
	}
}

void BladePacket::set_endpoint(AmFrame::StreamEndPoint * endpoint)
{
	endpoint_ = *endpoint;
}

void BladePacket::copy_content(Packet* packet)
{

}

void BladePacket::set_net_data(NetDataProcessor * net_data)
{
	net_data_ = net_data;
}

NetDataProcessor * BladePacket::get_net_data()
{
	return net_data_;
}

void BladePacket::init()
{
    NetDataProcessor * net_data_tmp = new NetDataProcessor();
    set_net_data(net_data_tmp);
}

bool BladePacket::is_regular_packet()
{
	return is_regular_packet_;
}

int16_t BladePacket::get_cmd_type()
{
	return operation_;
}

std::string BladePacket::get_name()
{
	return name_;
}

}//end of namespace message
}//end of namespace bladestore
