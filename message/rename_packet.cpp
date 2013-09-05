/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: rename_packet.cpp
 * Description: This file defines Rename packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 * Revised :
 */

#include "rename_packet.h"
#include "blade_net_util.h"


namespace bladestore
{
namespace message
{
// RenamePacket
// =====================================================================
RenamePacket::RenamePacket(): src_file_name_(), dst_file_name_()
{
    length_ = 0;
    operation_ = OP_RENAME;
}

RenamePacket::RenamePacket(string src_file_name, string dst_file_name)
    :src_file_name_(src_file_name), dst_file_name_(dst_file_name)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_RENAME;
}

RenamePacket::~RenamePacket() {}

int RenamePacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteString(src_file_name_);
        net_data_->WriteString(dst_file_name_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int RenamePacket::Unpack()
{
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetString(src_file_name_);
        net_data_->GetString(dst_file_name_);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int RenamePacket::Reply(BladePacket * resp_packet)
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

size_t RenamePacket::GetLocalVariableSize()
{
    size_t local_size = src_file_name_.size() + sizeof(int32_t) + 1 +
                        dst_file_name_.size() + sizeof(int32_t) + 1;
    return local_size;
}

string RenamePacket::get_src_file_name()
{
    return src_file_name_;
}

string RenamePacket::get_dst_file_name()
{
    return dst_file_name_;
}

// RenameReplyPacket
RenameReplyPacket::RenameReplyPacket(): ret_code_(0)
{
    length_ = 0;
    operation_ = OP_RENAME_REPLY;
}

RenameReplyPacket::RenameReplyPacket(int16_t ret_code): ret_code_(ret_code)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_RENAME_REPLY;
}

RenameReplyPacket::~RenameReplyPacket() {}

int RenameReplyPacket::Pack()
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

int RenameReplyPacket::Unpack()
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

size_t RenameReplyPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(ret_code_);
    return local_size;
}

int16_t RenameReplyPacket::get_ret_code()
{
    return ret_code_;
}

void RenameReplyPacket::set_ret_code(int16_t ret_code)
{
    ret_code_ = ret_code;
}

}  // end of namespace message
}  // end of namespace bladestore

