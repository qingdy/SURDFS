/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-4-24
 *
 */
#ifndef BLADESTORE_MESSAGE_LOG_PACKET_H
#define BLADESTORE_MESSAGE_LOG_PACKET_H

#include "blade_data_buffer.h"
#include "blade_packet.h"
#include "blade_common_data.h"

using namespace bladestore::common;

namespace bladestore
{
namespace message
{

class BladeLogPacket : public BladePacket
{
public:
	BladeLogPacket();
	~BladeLogPacket();
	
	int Pack();
	int Unpack();

	int Reply(BladePacket * response);	

	int GetSerializeSize();
	
	size_t GetLocalVariableSize();
	
	void set_data(char * , int32_t );

	char * data_buf()
	{
		return data_buf_;
	}

	int32_t data_length()
	{
		return data_length_;
	}

private:

	bool own_buffer_;
	//用来存放BladeLogEntry和具体的数据
	char * data_buf_;
	int32_t data_length_;
};

//BladeLogPacket的返回包用StatusPacket回应

}//end of namespace message
}//end of namespace bladestore
#endif
