/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: get_listing.cpp
 * Description: This file defines GetListing packet.(example to other)
 * 
 * Version : 0.1
 * Author  : carloswjx
 * Date    : 2012-3-30
 *
 * Version : 0.2 
 * Author  : carloswjx
 * Date    : 2012-4-11
 * Revised : add init, modify after first debugging.
 */
#include "get_listing_packet.h"
#include "blade_net_util.h"

namespace bladestore
{
namespace message
{
// GetListingPacket
// =====================================================================
GetListingPacket::GetListingPacket(): parent_id_(0), file_name_()
{
    length_ = 0;
    operation_ = OP_GETLISTING;
}

GetListingPacket::GetListingPacket(int64_t parent_id, string file_name) : parent_id_(parent_id), file_name_(file_name)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_GETLISTING;
}

GetListingPacket::~GetListingPacket() {}

int GetListingPacket::Pack()  // sender fill amframe packet's content
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

int GetListingPacket::Unpack()  // receiver fill local struct
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

int GetListingPacket::Reply(BladePacket * resp_packet)
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

size_t GetListingPacket::GetLocalVariableSize()
{
    size_t local_size = file_name_.size() + sizeof(int32_t) + 1 + sizeof(parent_id_);
    return local_size;
}

int64_t GetListingPacket::parent_id()
{
    return parent_id_;
}

string GetListingPacket::file_name()
{
    return file_name_;
}

// GetListingReplyPacket
// =====================================================================
GetListingReplyPacket::GetListingReplyPacket(): ret_code_(0), file_num_(0)
{
    length_ = 0;
    operation_ = OP_GETLISTING_REPLY;
}

GetListingReplyPacket::GetListingReplyPacket(int16_t ret_code,
    const vector<string>& files): ret_code_(ret_code), file_num_(files.size()),
    file_names_(files)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_GETLISTING_REPLY;
}

GetListingReplyPacket::~GetListingReplyPacket() {}

int GetListingReplyPacket::Pack()  // sender fill amframe packet's content
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) 
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt16(ret_code_);
        net_data_->WriteInt32(file_num_);
        for (vector<string>::const_iterator iter = file_names_.begin();
                iter != file_names_.end(); ++iter) 
        {
            net_data_->WriteString(*iter);
        }
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int GetListingReplyPacket::Unpack()  // receiver fill local struct
{
    if (net_data_) 
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt16(ret_code_);
        if (ret_code_ == RET_SUCCESS) 
        {
            if (net_data_->GetInt32(file_num_)) 
            {
                string file_name_;
                for (int i = 0; i < file_num_; ++i) 
                {
                    net_data_->GetString(file_name_);
                    (file_names_).push_back(file_name_);
                }
            }
        }
        return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

size_t GetListingReplyPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(ret_code_) + sizeof(file_num_);
    for (vector<string>::const_iterator iter = file_names_.begin();
                iter != file_names_.end(); ++iter) 
    {
        local_size += (*iter).size() + sizeof(int32_t) + 1;
    }
    return local_size;
}

int16_t GetListingReplyPacket::ret_code()
{
    return ret_code_;
}

int32_t GetListingReplyPacket::file_num()
{
    return file_num_;
}

vector<string>& GetListingReplyPacket::file_names()
{
    return file_names_;
}

void GetListingReplyPacket::set_ret_code(int16_t ret_code)
{
    ret_code_ = ret_code;
}

void GetListingReplyPacket::set_file_num(int32_t file_num)
{
    file_num_ = file_num;
}

void GetListingReplyPacket::set_file_names(vector<string> &file_names)
{
    file_names_ = file_names;
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
}

}  // end of namespace message
}  // end of namespace bladestore
