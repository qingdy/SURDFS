#include "blade_socket_handler.h"

namespace bladestore
{
namespace message
{

BladeStreamSocketHandler::BladeStreamSocketHandler(AmFrame & amframe) : StreamSocketHandler(amframe)
{
	packet_factory_ = new BladePacketFactory();
}

BladeStreamSocketHandler::~BladeStreamSocketHandler()
{   
    if (packet_factory_)
    {
        delete packet_factory_;
        packet_factory_ = NULL;
    }
}       

int BladeStreamSocketHandler::DetectPacketSize(const void * data, size_t length)
{
	int ret_size = 0;
	char * buf = (char *)data;
	if (length < (sizeof(size_t) + sizeof(int16_t)))
	{
		return 0;
	}
	else
	{
		if (4 == sizeof(size_t))
		{
			uint32_t size;
			size = (unsigned char)buf[0];
			size <<= 8;
			size |= (unsigned char)buf[1];
			size <<= 8;
			size |= (unsigned char)buf[2];
			size <<= 8;
			size |= (unsigned char)buf[3];
			ret_size = (int)size;
		}
		else if (8 == sizeof(size_t))
		{
			uint64_t size;
			size = (unsigned char)buf[0];
			size <<= 8;
			size |= (unsigned char)buf[1];
			size <<= 8;
			size |= (unsigned char)buf[2];
			size <<= 8;
			size |= (unsigned char)buf[3];
			size <<= 8;
			size |= (unsigned char)buf[4];
			size <<= 8;
			size |= (unsigned char)buf[5];
			size <<= 8;
			size |= (unsigned char)buf[6];
			size <<= 8;
			size |= (unsigned char)buf[7];
			ret_size = (int)size;
		}
	}
	return ret_size;
}

//void BladeStreamSocketHandler::OnReceived(const Packet & packet, void * client_data)
//{
//	int16_t packet_type = 0;
//	packet_type = (packet.content())[4];
//	packet_type <<= 8;
//	packet_type |= (packet.content())[5];
//	BladePacket * blade_packet = packet_factory_->create_packet(packet_type);	
//	blade_packet->SetContent(packet.content(), packet.length());
//	//todo something later
//}

//void BladeBaseStreamSocketHandler::set_blade_packet_queue_thread(BladePacketQueueThread * blade_packet_queue_thread)
//{
//	blade_packet_queue_thread_ = blade_packet_queue_thread;
//}

BladePacketFactory * BladeStreamSocketHandler::packet_factory()
{
    return packet_factory_;
}

void BladeStreamSocketHandler::set_packet_factory(BladePacketFactory *packet_factory)
{
    packet_factory_ = packet_factory;
}

}//end of namespace common
}//end of namespace bladestore
