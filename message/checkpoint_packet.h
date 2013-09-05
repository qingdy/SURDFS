#ifndef BLADESTORE_MESSAGE_CHECKPOINT_PACKET_H
#define BLADESTORE_MESSAGE_CHECKPOINT_PACKET_H

#include <stdint.h>

#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "blade_packet.h"

using namespace bladestore::common;

namespace bladestore 
{ 
namespace message
{
class CheckPointPacket : public BladePacket
{
public:
    CheckPointPacket();
    ~CheckPointPacket();

    int Pack();

    int Unpack();

	size_t GetLocalVariableSize(); 
	
};//end of class CheckPointPacket

}//end of namespace message 
}//end of namespace bladestore

#endif
