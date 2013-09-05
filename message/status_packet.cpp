/* copyright (c) 2012, Sohu R&D                                                                                                                              
 * All rights reserved.                                                                                                                                   *  
 * File name : status_packet.cpp
 * Description: This file defines Status packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-11
 * Revised :
 */

#include "status_packet.h"

namespace bladestore 
{
namespace message
{
using namespace std;

//StatusPacket
StatusPacket::StatusPacket() 
{
    length_ = sizeof(length_) + sizeof(operation_) + sizeof(int8_t);
    operation_ = OP_STATUS;
}

StatusPacket::StatusPacket(Status status): status_(status) 
{
    length_ = sizeof(length_) + sizeof(operation_) + sizeof(int8_t);
    operation_ = OP_STATUS;
}

StatusPacket::~StatusPacket() 
{

}

int StatusPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if(net_data_) 
	{
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt8(status_);

		SetContentPtr(net_write, length_, true);	
        return BLADE_SUCCESS;
    }
    return BLADE_NETDATA_PACK_ERROR;
}

int StatusPacket::Unpack()
{
    if(net_data_) 
	{
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        int8_t tmp;
        net_data_->GetInt8(tmp);
        if(tmp == 0)
		{
            status_ = STATUS_OK;
		}
        else if(tmp == 1)
		{
            status_ = STATUS_ERROR;
		}

        return BLADE_SUCCESS;
    }

    return BLADE_NETDATA_UNPACK_ERROR;
}

Status StatusPacket::get_status()
{
	return status_;
}
   
size_t StatusPacket::GetLocalVariableSize() 
{
	return length_;
}

} //end of namespace message 
} //end of namespace bladestore 
