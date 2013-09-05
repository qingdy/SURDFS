/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-6-25
 *
 */
#ifndef BLADESTORE_MESSAGE_LEAVE_DS_PACKET_H
#define BLADESTORE_MESSAGE_LEAVE_DS_PACKET_H

#include <stdint.h>

#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "blade_packet.h"

using namespace bladestore::common;

namespace bladestore 
{ 
namespace message
{

class LeaveDsPacket : public BladePacket
{
public:
    LeaveDsPacket();
    ~LeaveDsPacket();

    int Pack();

    int Unpack();

	void set_ds_id(uint64_t);

	uint64_t get_ds_id();

	size_t GetLocalVariableSize(); 
	
private:
   	uint64_t ds_id_; 
};//end of class StatusPacket

}//end of namespace message 
}//end of namespace bladestore

#endif
