/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: complete_packet.cpp
 * Description: This file defines Complete packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 * Revised :
 */

#include "complete_packet.h"
#include "blade_net_util.h"

namespace bladestore
{
namespace message
{
// CompletePacket
// =====================================================================
CompletePacket::CompletePacket(): file_id_(0), block_id_(0), block_size_(0)
{
    length_ = 0;
    operation_ = OP_COMPLETE;
}

CompletePacket::CompletePacket(int64_t file_id, int64_t block_id, int32_t block_size)
        :file_id_(file_id), block_id_(block_id), block_size_(block_size)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_COMPLETE;
}

CompletePacket::~CompletePacket() {}

int CompletePacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) 
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt64(file_id_);
        net_data_->WriteInt64(block_id_);
        net_data_->WriteInt32(block_size_);
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int CompletePacket::Unpack()
{
    if (net_data_) 
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt64(file_id_);
        net_data_->GetInt64(block_id_);
        net_data_->GetInt32(block_size_);
        return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int CompletePacket::Reply(BladePacket * resp_packet)
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

size_t CompletePacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(file_id_) + sizeof(block_id_) + sizeof(block_size_);
    return local_size;
}

int64_t CompletePacket::file_id()
{
    return file_id_;
}

int64_t CompletePacket::block_id()
{
    return block_id_;
}

int32_t CompletePacket::block_size()
{
    return block_size_;
}

// CompleteReplyPacket
// =====================================================================
CompleteReplyPacket::CompleteReplyPacket(): ret_code_(0)
{
    length_ = 0;
    operation_ = OP_COMPLETE_REPLY;
}

CompleteReplyPacket::CompleteReplyPacket(int16_t ret_code): ret_code_(ret_code)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_COMPLETE_REPLY;
}

CompleteReplyPacket::~CompleteReplyPacket() {}

int CompleteReplyPacket::Pack()
{
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

int CompleteReplyPacket::Unpack()
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

size_t CompleteReplyPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(ret_code_);
    return local_size;
}

int16_t CompleteReplyPacket::ret_code()
{
    return ret_code_;
}

void CompleteReplyPacket::set_ret_code(int16_t ret_code)
{
    ret_code_ = ret_code;
}
}  // end of namespace message
}  // end of namespace bladestore
