/* copyright (c) 2012, Sohu R&D
 * All rights reserved.                                                                                                                                   *
 * File   name: status_packet.h
 * Description: This file defines Status packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-11
 * Revised :
 */

#ifndef BLADESTORE_MESSAGE_STAUS_PACKET_H
#define BLADESTORE_MESSAGE_STAUS_PACKET_H

#include <stdint.h>

#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "blade_packet.h"

using namespace bladestore::common;

namespace bladestore 
{ 
namespace message
{
enum Status 
{
    STATUS_OK,
    STATUS_ERROR 
};

class StatusPacket : public BladePacket
{
public:
    StatusPacket();
    explicit StatusPacket(Status status);
    ~StatusPacket();

    int Pack();

    int Unpack();

	Status get_status();

	size_t GetLocalVariableSize(); 
	
	void set_status(Status status)
	{
		status_ = status;
	}

private:
    Status status_;
};//end of class StatusPacket

}//end of namespace message 
}//end of namespace bladestore

#endif
