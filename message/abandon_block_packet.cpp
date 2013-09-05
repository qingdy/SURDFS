/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: abandon_block.cpp
 * Description: This file defines AbandonBlock packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 * Revised :
 */

#include "abandon_block_packet.h"
#include "blade_net_util.h"

namespace bladestore
{
namespace message
{
// AbandonBlockPacket
// =====================================================================
AbandonBlockPacket::AbandonBlockPacket(): file_id_(0), block_id_(0)
{
    length_ = 0;
    operation_ = OP_ABANDONBLOCK;
}

AbandonBlockPacket::AbandonBlockPacket(int64_t file_id, int64_t block_id)
    : file_id_(file_id), block_id_(block_id)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_ABANDONBLOCK;
}

AbandonBlockPacket::~AbandonBlockPacket() {}

int  AbandonBlockPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt64(file_id_);
        net_data_->WriteInt64(block_id_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int AbandonBlockPacket::Unpack()
{
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt64(file_id_);
        net_data_->GetInt64(block_id_);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

size_t AbandonBlockPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(file_id_) + sizeof(block_id_);
    return local_size;
}

int64_t AbandonBlockPacket::file_id()
{
    return file_id_;
}

int64_t AbandonBlockPacket::block_id()
{
    return block_id_;
}

int AbandonBlockPacket::Reply(BladePacket * resp_packet)
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

// AbandonBlockReplyPacket
AbandonBlockReplyPacket::AbandonBlockReplyPacket(): ret_code_(0)
{
    length_ = 0;
    operation_ = OP_ABANDONBLOCK_REPLY;
}

AbandonBlockReplyPacket::AbandonBlockReplyPacket(int16_t ret_code):
    ret_code_(ret_code)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_ABANDONBLOCK_REPLY;
}

AbandonBlockReplyPacket::~AbandonBlockReplyPacket() {}

int AbandonBlockReplyPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt16(ret_code_);
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int AbandonBlockReplyPacket::Unpack()
{
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt16(ret_code_);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

size_t AbandonBlockReplyPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(ret_code_);
    return local_size;
}

int16_t AbandonBlockReplyPacket::ret_code()
{
    return ret_code_;
}

void AbandonBlockReplyPacket::set_ret_code(int16_t ret_code)
{
    ret_code_ = ret_code;
}

}  // end of namespace message
}  // end of namespace bladestore
