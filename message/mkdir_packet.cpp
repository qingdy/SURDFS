/*
 * File name  : mkdir_packet.cpp
 * Description: This file defines Mkdir packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 * Revised :
 */

#include "mkdir_packet.h"
#include "blade_net_util.h"

namespace bladestore
{
namespace message
{
// MkdirPacket
// =====================================================================
MkdirPacket::MkdirPacket(): parent_id_(0), file_name_()
{
    length_ = 0;
    operation_ = OP_MKDIR;
}

MkdirPacket::MkdirPacket(int64_t parent_id, string file_name): parent_id_(parent_id), file_name_(file_name)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_MKDIR;
}

MkdirPacket::~MkdirPacket() {}

int MkdirPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt64(parent_id_);
        net_data_->WriteString(file_name_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int MkdirPacket::Unpack()
{
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt64(parent_id_);
        net_data_->GetString(file_name_);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int MkdirPacket::Reply(BladePacket * resp_packet)
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

size_t MkdirPacket::GetLocalVariableSize()
{
    size_t local_size = file_name_.size() + sizeof(parent_id_) + sizeof(int32_t) + 1;
    return local_size;
}

int64_t MkdirPacket::parent_id()
{
    return parent_id_;
}

string MkdirPacket::file_name()
{
    return file_name_;
}


// MkdirReplyPacket
// =====================================================================
MkdirReplyPacket::MkdirReplyPacket(): ret_code_(0), file_id_(0)
{
    length_ = 0;
    operation_ = OP_MKDIR_REPLY;
}

MkdirReplyPacket::MkdirReplyPacket(int16_t ret_code, int64_t file_id): ret_code_(ret_code), file_id_(file_id)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_MKDIR_REPLY;
}

MkdirReplyPacket::~MkdirReplyPacket() {}

int MkdirReplyPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt16(ret_code_);
        net_data_->WriteInt64(file_id_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int MkdirReplyPacket::Unpack()
{
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt16(ret_code_);
        net_data_->GetInt64(file_id_);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

size_t MkdirReplyPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(ret_code_) + sizeof(file_id_);
    return local_size;
}

int16_t MkdirReplyPacket::ret_code()
{
    return ret_code_;
}

int64_t MkdirReplyPacket::file_id()
{
    return file_id_;
}

void MkdirReplyPacket::set_ret_code(int16_t ret_code)
{
    ret_code_ = ret_code;
}

void MkdirReplyPacket::set_file_id(int64_t file_id)
{
    file_id_ = file_id;
}

}  // end of namespace message
}  // end of namespace bladestore
