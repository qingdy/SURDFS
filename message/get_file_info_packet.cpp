/*
 * File   name: get_file_info_packet.cpp
 * Description: This file defines GetFileInfo packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 * Revised :
 */

#include "get_file_info_packet.h"
#include "blade_net_util.h"

namespace bladestore
{
namespace message
{
// GetFileInfoPacket
// =====================================================================
GetFileInfoPacket::GetFileInfoPacket(): parent_id_(0), file_name_()
{
    length_ = 0;
    operation_ = OP_GETFILEINFO;
}

GetFileInfoPacket::GetFileInfoPacket(int64_t parent_id, string file_name): parent_id_(parent_id), file_name_(file_name)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_GETFILEINFO;
}

GetFileInfoPacket::~GetFileInfoPacket() {}

int GetFileInfoPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) 
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt64(parent_id_);
        net_data_->WriteString(file_name_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int GetFileInfoPacket::Unpack()
{
    if (net_data_) 
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt64(parent_id_);
        net_data_->GetString(file_name_);
        return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int GetFileInfoPacket::Reply(BladePacket * resp_packet)
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

size_t GetFileInfoPacket::GetLocalVariableSize()
{
    size_t local_size = file_name_.size() + sizeof(parent_id_) + sizeof(int32_t) + 1;
    return local_size;
}

int64_t GetFileInfoPacket::parent_id()
{
    return parent_id_;
}

string GetFileInfoPacket::file_name()
{
    return file_name_;
}

// GetFileInfoReplyPacket
GetFileInfoReplyPacket::GetFileInfoReplyPacket(): ret_code_(0), file_info_()
{
    length_ = 0;
    operation_ = OP_GETFILEINFO_REPLY;
}

GetFileInfoReplyPacket::GetFileInfoReplyPacket(
        int16_t ret_code, int8_t t, int64_t id, struct timeval mt,
        struct timeval ct, struct timeval crt, int64_t c, int16_t n,
        off_t file_size):ret_code_(ret_code),
        file_info_(id, t, mt, ct, crt, c, n, file_size)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_GETFILEINFO_REPLY;
}

GetFileInfoReplyPacket::~GetFileInfoReplyPacket() {}

int GetFileInfoReplyPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) 
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt16(ret_code_);
        if (ret_code_ == RET_SUCCESS) 
        {
            net_data_->WriteInt64(file_info_.get_file_id());
            net_data_->WriteInt8(file_info_.get_file_type());
            net_data_->WriteInt16(file_info_.get_num_replicas());
            net_data_->WriteInt64(file_info_.get_mtime().tv_sec);
            net_data_->WriteInt64(file_info_.get_mtime().tv_usec);
            net_data_->WriteInt64(file_info_.get_ctime().tv_sec);
            net_data_->WriteInt64(file_info_.get_ctime().tv_usec);
            net_data_->WriteInt64(file_info_.get_crtime().tv_sec);
            net_data_->WriteInt64(file_info_.get_crtime().tv_usec);
            net_data_->WriteInt64(file_info_.get_block_count());
            net_data_->WriteInt64(file_info_.get_file_size());
        }
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int GetFileInfoReplyPacket::Unpack()
{
    if (net_data_) 
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt16(ret_code_);
        if (ret_code_ == RET_SUCCESS) 
        {
            int64_t file_id;
            net_data_->GetInt64(file_id);
            file_info_.set_file_id(file_id);
            int8_t file_type;
            net_data_->GetInt8(file_type);
            file_info_.set_file_type(file_type);
            int16_t num_replicas;
            net_data_->GetInt16(num_replicas);
            file_info_.set_num_replicas(num_replicas);
            struct timeval mtime;
            net_data_->GetInt64(mtime.tv_sec);
            net_data_->GetInt64(mtime.tv_usec);
            file_info_.set_mtime(mtime);
            struct timeval ctime;
            net_data_->GetInt64(ctime.tv_sec);
            net_data_->GetInt64(ctime.tv_usec);
            file_info_.set_ctime(ctime);
            struct timeval crtime;
            net_data_->GetInt64(crtime.tv_sec);
            net_data_->GetInt64(crtime.tv_usec);
            file_info_.set_crtime(crtime);
            int64_t block_count;
            net_data_->GetInt64(block_count);
            file_info_.set_block_count(block_count);
            int64_t file_size;
            net_data_->GetInt64(file_size);
            file_info_.set_file_size(file_size);
        }
        return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

size_t GetFileInfoReplyPacket::GetLocalVariableSize()
{
    size_t local_size =
        sizeof(ret_code_) + sizeof(file_info_.get_file_type()) +
        sizeof(file_info_.get_num_replicas()) + sizeof(file_info_.get_mtime()) +
        sizeof(file_info_.get_ctime()) + sizeof(file_info_.get_crtime()) +
        sizeof(file_info_.get_block_count()) + sizeof(file_info_.get_file_size()) +
        sizeof(file_info_.get_file_id());
    return local_size;
}

int16_t GetFileInfoReplyPacket::ret_code()
{
    return ret_code_;
}

FileInfo GetFileInfoReplyPacket::file_info()
{
    return file_info_;
}

void GetFileInfoReplyPacket::set_ret_code(int16_t ret_code)
{
    ret_code_ = ret_code;
}

void GetFileInfoReplyPacket::set_file_info(FileInfo &file_info)
{
    file_info_.set_file_type(file_info.get_file_type());
    file_info_.set_file_id(file_info.get_file_id());
    file_info_.set_num_replicas(file_info.get_num_replicas());
    file_info_.set_mtime(file_info.get_mtime());
    file_info_.set_ctime(file_info.get_ctime());
    file_info_.set_crtime(file_info.get_crtime());
    file_info_.set_block_count(file_info.get_block_count());
    file_info_.set_file_size(file_info.get_file_size());
}

}  // end of namespace message
}  // end of namespace bladestore
