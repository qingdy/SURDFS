/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: creat_packet.cpp
 * Description: This file defines Creat packet.
 *
 * Version : 0.1
 * Author  : funing
 * Date    : 2012-4-5
 * Revised :
 */

#include "bladestore_ops.h"
#include "blade_common_data.h"
#include "register_packet.h"
#include "blade_net_util.h"

namespace bladestore
{
namespace message
{
// using namespace std;
RegisterPacket::RegisterPacket()
{
    operation_ = OP_REGISTER;
}

RegisterPacket::RegisterPacket(int32_t rackid, uint64_t ds_id):rack_id_(rackid), ds_id_(ds_id)
{
    operation_ = OP_REGISTER;
}

RegisterPacket::~RegisterPacket() {}

int RegisterPacket::Pack()
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) 
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt32(rack_id_);
        net_data_->WriteUint64(ds_id_); 
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } else 
    {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int RegisterPacket::Unpack()
{
    if (net_data_) 
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt32(rack_id_);
        net_data_->GetUint64(ds_id_);
        return BLADE_SUCCESS;
    }
    else 
    {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int RegisterPacket::Reply(BladePacket * resp_packet)
{
    resp_packet->Pack();
    int error;
    if((0 != peer_id_)&&(BladeNetUtil::GetPeerID(endpoint_.GetFd()) == peer_id_))     
    {   
        error = BladeCommonData::amframe_.SendPacket(endpoint_, resp_packet);
    }   
    else
    {   
        error = -1; 
    }   
    return error;
}

size_t RegisterPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(rack_id_) + sizeof(ds_id_);
    return local_size;
}


RegisterReplyPacket::RegisterReplyPacket(): ret_code_(0)
{
    length_ = 0;
    operation_ = OP_REGISTER_REPLY;
}

RegisterReplyPacket::RegisterReplyPacket(int16_t ret_code):ret_code_(ret_code)
{
    length_ = 0;
    operation_ = OP_REGISTER_REPLY;
}

RegisterReplyPacket::~RegisterReplyPacket()
{
}

int RegisterReplyPacket::Pack()
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_)
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt16(ret_code_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    }
    else 
    {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int RegisterReplyPacket::Unpack()
{
    if (net_data_) 
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt16(ret_code_);
        return BLADE_SUCCESS;
    }
    else 
    {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

size_t RegisterReplyPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(ret_code_);
    return local_size;
}

}  // end of namespace message
}  // end of namespace bladestore
