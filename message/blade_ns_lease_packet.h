/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * Version : 0.1
 * Author  : funing
 * Date    : 2012-3-27
 * Revised :
 */
#ifndef BLADE_MESSAGE_NS_LEASE_PACKET_H
#define BLADE_MESSAGE_NS_LEASE_PACKET_H

#include <stdint.h>


#include "bladestore_ops.h"
#include "blade_packet.h"
#include "blade_common_define.h"
#include "blade_common_data.h"
#include "blade_lease_common.h"

using namespace bladestore::common;

namespace bladestore
{
namespace message
{

class BladeRenewLeasePacket : public BladePacket
{
public:
    BladeRenewLeasePacket();
    BladeRenewLeasePacket(uint64_t);
    ~BladeRenewLeasePacket();
   
    virtual int Pack();//sender
    virtual int Unpack();//reciever
    int Reply(BladePacket *reply_packet);
    size_t GetLocalVariableSize();
	uint64_t ds_id()
	{
		return ds_id_;
	}

public:
    uint64_t ds_id_;
};


class BladeGrantLeasePacket : public BladePacket
{
public:    
    BladeGrantLeasePacket();
    ~BladeGrantLeasePacket();
    
    virtual int Pack ();//sender
    virtual int Unpack ();//reciever
    size_t GetLocalVariableSize();
	BladeLease blade_lease()
	{
		return blade_lease_;
	}

public:
    int16_t ret_code_;
    BladeLease blade_lease_;
};


}//end of namespace message
}//end of namespace bladestore
#endif
