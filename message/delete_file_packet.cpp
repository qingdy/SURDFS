/*
 * File name  : delete_file_packet.cpp
 * Description: This file defines DeleteFile packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 * Revised :
 */

#include "delete_file_packet.h"
#include "blade_net_util.h"

namespace bladestore
{
namespace message
{
// DeleteFilePacket
// =====================================================================
DeleteFilePacket::DeleteFilePacket(): parent_id_(0), file_name_(),path_()
{
    length_ = 0;
    operation_ = OP_DELETEFILE;
}

DeleteFilePacket::DeleteFilePacket(int64_t parent_id, string file_name, string path): parent_id_(parent_id), file_name_(file_name), path_(path)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_DELETEFILE;
}

DeleteFilePacket::~DeleteFilePacket() {}

int DeleteFilePacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt64(parent_id_);
        net_data_->WriteString(file_name_);
        net_data_->WriteString(path_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int DeleteFilePacket::Unpack()
{
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt64(parent_id_);
        net_data_->GetString(file_name_);
        net_data_->GetString(path_);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int DeleteFilePacket::Reply(BladePacket * resp_packet)
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

size_t DeleteFilePacket::GetLocalVariableSize()
{
    size_t local_size = (file_name_.size() + sizeof(int32_t) + 1) + (path_.size() + sizeof(int32_t) + 1) + sizeof(parent_id_) ;
    return local_size;
}

int64_t DeleteFilePacket::parent_id()
{
    return parent_id_;
}

string DeleteFilePacket::file_name()
{
    return file_name_;
}

string DeleteFilePacket::path()
{
    return path_;
}

// DeleteFileReplyPacket
// =====================================================================
DeleteFileReplyPacket::DeleteFileReplyPacket(): ret_code_(0)
{
    length_ = 0;
    operation_ = OP_DELETEFILE_REPLY;
}

DeleteFileReplyPacket::DeleteFileReplyPacket(int16_t ret_code):
                                                ret_code_(ret_code)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_DELETEFILE_REPLY;
}

DeleteFileReplyPacket::~DeleteFileReplyPacket() {}

int DeleteFileReplyPacket::Pack()
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

int DeleteFileReplyPacket::Unpack()
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

size_t DeleteFileReplyPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(ret_code_);
    return local_size;
}

int16_t DeleteFileReplyPacket::ret_code()
{
    return ret_code_;
}

void DeleteFileReplyPacket::set_ret_code(int16_t ret_code)
{
    ret_code_ = ret_code;
}
}  // end of namespace message
}  // end of namespace bladestore
