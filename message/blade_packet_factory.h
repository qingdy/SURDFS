/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-3-30
 *
 */
#ifndef BLADESTORE_MESSAGE_PACKET_FACTORY_H
#define BLADESTORE_MESSAGE_PACKET_FACTORY_H

#include "blade_packet.h"


namespace bladestore
{
namespace message 
{

class BladePacketFactory
{
public:
	BladePacketFactory();
	~BladePacketFactory();

	BladePacket * create_packet(int16_t packet_type);
};

}//end of namespace message
}//end of namespace bladestore
#endif
