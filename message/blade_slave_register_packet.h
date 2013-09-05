/*
 *version  : 1.0
 *author   : chen yunyun
 *date     : 2012-5-21
 *
 */
#ifndef BLADE_MESSAGE_NS_SLAVE_REGISTER_PACKET_H
#define BLADE_MESSAGE_NS_SLAVE_REGISTER_PACKET_H

#include <stdint.h>


#include "bladestore_ops.h"
#include "blade_packet.h"
#include "blade_common_define.h"
#include "blade_common_data.h"
#include "blade_fetch_param.h"

using namespace bladestore::common;

namespace bladestore
{
namespace message
{

class BladeSlaveRegisterPacket : public BladePacket
{
public:
    BladeSlaveRegisterPacket();

    ~BladeSlaveRegisterPacket();
   
    virtual int Pack();//sender

    virtual int Unpack();//reciever

    int Reply(BladePacket *reply_packet);

    size_t GetLocalVariableSize();

	uint64_t slave_id()
	{
		return slave_id_;
	}

public:
    uint64_t slave_id_;
};


class BladeSlaveRegisterReplyPacket : public BladePacket
{
public:    
    BladeSlaveRegisterReplyPacket();

    ~BladeSlaveRegisterReplyPacket();
    
    virtual int Pack ();//sender

    virtual int Unpack ();//reciever

    size_t GetLocalVariableSize();

	BladeFetchParam fetch_param()
	{
		return fetch_param_;
	}

public:

    int16_t ret_code_;

    BladeFetchParam fetch_param_;
};


}//end of namespace message
}//end of namespace bladestore
#endif
