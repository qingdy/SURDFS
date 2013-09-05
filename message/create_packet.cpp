/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: create_packet.cpp
 * Description: This file defines Create packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-5
 * Revised :
 */

#include "create_packet.h"
#include "blade_net_util.h"

namespace bladestore
{
namespace message
{
// using namespace std;
// CreatePacket
// =====================================================================
CreatePacket::CreatePacket(): parent_id_(0), file_name_(), oflag_(0), replication_(0)
{
    length_ = 0;
    operation_ = OP_CREATE;
}

CreatePacket::CreatePacket(int64_t parent_id, string file_name, int8_t oflag, int16_t replication):
        parent_id_(parent_id), file_name_(file_name), oflag_(oflag), replication_(replication)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_CREATE;
}

CreatePacket::~CreatePacket() {}

int CreatePacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) 
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt64(parent_id_);
        net_data_->WriteString(file_name_);
        net_data_->WriteInt16(replication_);
        net_data_->WriteInt8(oflag_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int CreatePacket::Unpack()
{
    if (net_data_) 
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt64(parent_id_);
        net_data_->GetString(file_name_);
        net_data_->GetInt16(replication_);
        net_data_->GetInt8(oflag_);
        return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int CreatePacket::Reply(BladePacket * resp_packet)
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

size_t CreatePacket::GetLocalVariableSize()
{
    size_t local_size = file_name_.size() + sizeof(parent_id_) + sizeof(int32_t) + 1
           + sizeof(replication_) + sizeof(oflag_);
    return local_size;
}

int64_t CreatePacket::parent_id()
{
    return parent_id_;
}

string CreatePacket::file_name()
{
    return file_name_;
}

int16_t CreatePacket::replication()
{
    return replication_;
}

int8_t CreatePacket::oflag()
{
    return oflag_;
}

// CreateReplyPacket
// =====================================================================
CreateReplyPacket::CreateReplyPacket(): ret_code_(0), file_id_(0)
{
    length_ = 0;
    operation_ = OP_CREATE_REPLY;
}

CreateReplyPacket::CreateReplyPacket(int16_t ret_code, int64_t file_id):
        ret_code_(ret_code), file_id_(file_id)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_CREATE_REPLY;
}

CreateReplyPacket::~CreateReplyPacket() {}

int CreateReplyPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) 
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt16(ret_code_);
        if (ret_code_ == RET_SUCCESS || ret_code_ == RET_FILE_EXIST)
            net_data_->WriteInt64(file_id_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int CreateReplyPacket::Unpack()
{
    if (net_data_) 
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt16(ret_code_);
        if (ret_code_ == RET_SUCCESS || ret_code_ == RET_FILE_EXIST)
            net_data_->GetInt64(file_id_);

         return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

size_t CreateReplyPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(ret_code_) + sizeof(file_id_);
    return local_size;
}

int16_t CreateReplyPacket::ret_code()
{
    return ret_code_;
}

int64_t CreateReplyPacket::file_id()
{
    return file_id_;
}

void CreateReplyPacket::set_ret_code(int16_t ret_code)
{
    ret_code_ = ret_code;
}

void CreateReplyPacket::set_file_id(int64_t file_id)
{
    file_id_ = file_id;
}
}  // end of namespace message
}  // end of namespace bladestore
