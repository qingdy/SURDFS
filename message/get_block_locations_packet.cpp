/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: get_block_locations_packet.cpp
 * Description: This file defines GetBlockLocations packet.
 * 
 * Version : 0.1
 * Author  : guili
 * Date    : 2012-4-05
 *
 * Version : 0.2
 * Author  : guili
 * Date    : 2012-4-09
 * Revised : changed block_version to int32_t,DataServerID to uint64_t
 */
#include "get_block_locations_packet.h"
#include "blade_net_util.h"

namespace bladestore
{
namespace message
{
// GetBlockLocationsPacket
// =====================================================================
GetBlockLocationsPacket::GetBlockLocationsPacket(): parent_id_(0), file_name_(""),
                                        offset_(0), data_length_(0)
{
    length_ = 0;
    operation_ = OP_GETBLOCKLOCATIONS;
}

GetBlockLocationsPacket::GetBlockLocationsPacket(int64_t parent_id, string file_name,
                         int64_t offset, int64_t data_length):
    parent_id_(parent_id), file_name_(file_name), offset_(offset), data_length_(data_length)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_GETBLOCKLOCATIONS;
}

GetBlockLocationsPacket::~GetBlockLocationsPacket() {}

int GetBlockLocationsPacket::Pack()  // sender fill amframe packet's content
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) 
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt64(parent_id_);
        net_data_->WriteString(file_name_);
        net_data_->WriteInt64(offset_);
        net_data_->WriteInt64(data_length_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int GetBlockLocationsPacket::Unpack()  // receiver fill local struct
{
    if (net_data_) 
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt64(parent_id_);
        net_data_->GetString(file_name_);
        net_data_->GetInt64(offset_);
        net_data_->GetInt64(data_length_);
        return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int GetBlockLocationsPacket::Reply(BladePacket * resp_packet)
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

size_t GetBlockLocationsPacket::GetLocalVariableSize()
{
    size_t local_size = file_name_.size() + sizeof(parent_id_) + sizeof(int32_t) + 1
                        + sizeof(offset_) + sizeof(data_length_);
    return local_size;
}

int64_t GetBlockLocationsPacket::parent_id()
{
    return parent_id_;
}

string GetBlockLocationsPacket::file_name()
{
    return file_name_;
}

int64_t GetBlockLocationsPacket::offset()
{
    return offset_;
}

int64_t GetBlockLocationsPacket::data_length()
{
    return data_length_;
}

// GetBlockLocationsReplyPacket
// =====================================================================
GetBlockLocationsReplyPacket::GetBlockLocationsReplyPacket(): ret_code_(0),
    file_id_(0), file_length_(0), located_block_num_(0), v_located_block_()
{
    length_ = 0;
    operation_ = OP_GETBLOCKLOCATIONS_REPLY;
}

GetBlockLocationsReplyPacket::GetBlockLocationsReplyPacket(
                int16_t ret_code,
                int64_t file_id,
                int64_t file_length,
                vector<LocatedBlock> &v_located_block):
    ret_code_(ret_code), file_id_(file_id), file_length_(file_length),
    located_block_num_(v_located_block.size()), v_located_block_(v_located_block)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_GETBLOCKLOCATIONS_REPLY;
}

GetBlockLocationsReplyPacket::~GetBlockLocationsReplyPacket() {}

// sender fill amframe packet's content
int GetBlockLocationsReplyPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) 
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt16(ret_code_);
        net_data_->WriteInt64(file_id_);
        net_data_->WriteInt64(file_length_);

        net_data_->WriteInt16(located_block_num_);
        for (vector<LocatedBlock>::const_iterator it = v_located_block_.begin();
                it != v_located_block_.end(); ++it) 
        {
            net_data_->WriteInt64(it->block_id());
            net_data_->WriteInt32(it->block_version());
            net_data_->WriteInt64(it->offset());
            net_data_->WriteInt64(it->length());

            net_data_->WriteInt16(it->dataserver_num());
            const vector<uint64_t> &vdsid = it->dataserver_id();
            for (vector<uint64_t>::const_iterator it2 = vdsid.begin();
                    it2 != vdsid.end(); ++it2) 
            {
                net_data_->WriteUint64(*it2);
            }
        }
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } 
    else 
    {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int GetBlockLocationsReplyPacket::Unpack()  // receiver fill local struct
{
    if (net_data_) 
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt16(ret_code_);
        if (ret_code_ == RET_SUCCESS) 
        {
            net_data_->GetInt64(file_id_);
            net_data_->GetInt64(file_length_);

            int64_t block_id, offset, block_length;
            uint64_t dataserver_id;
            int32_t block_version;
            int16_t dataserver_num;
            if (net_data_->GetInt16(located_block_num_)) 
            {
                for (int i = 0; i < located_block_num_; ++i) 
                {
                    net_data_->GetInt64(block_id);
                    net_data_->GetInt32(block_version);
                    net_data_->GetInt64(offset);
                    net_data_->GetInt64(block_length);

                    vector<uint64_t> vdsid;
                    if (net_data_->GetInt16(dataserver_num)) 
                    {
                        for (int j = 0; j < dataserver_num; ++j) 
                        {
                            net_data_->GetUint64(dataserver_id);
                            vdsid.push_back(dataserver_id);
                        }
                    }
                    LocatedBlock lb(block_id, block_version, offset,
                                    block_length, vdsid);
                    v_located_block_.push_back(lb);
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

size_t GetBlockLocationsReplyPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(ret_code_) + sizeof(file_id_)
                        + sizeof(file_length_) + sizeof(located_block_num_);
    for (vector<LocatedBlock>::const_iterator iter = v_located_block_.begin();
                  iter != v_located_block_.end(); ++iter) 
    {
        local_size += iter->GetSize();
        local_size += sizeof(int16_t);  // add the space of dataserver_num
    }
    return local_size;
}

int16_t GetBlockLocationsReplyPacket::ret_code()
{
    return ret_code_;
}

int64_t GetBlockLocationsReplyPacket::file_id()
{
    return file_id_;
}
int64_t GetBlockLocationsReplyPacket::file_length()
{
    return file_length_;
}

int16_t GetBlockLocationsReplyPacket::located_block_num()
{
    return located_block_num_;
}
vector<LocatedBlock> &GetBlockLocationsReplyPacket::v_located_block()
{
    return v_located_block_;
}

void GetBlockLocationsReplyPacket::set_ret_code(int16_t ret_code)
{
    ret_code_ = ret_code;
}

void GetBlockLocationsReplyPacket::set_file_id(int64_t file_id)
{
    file_id_ = file_id;
}

void GetBlockLocationsReplyPacket::set_file_length(int64_t file_length)
{
    file_length_ = file_length;
}

void GetBlockLocationsReplyPacket::set_located_block_num(int16_t located_block_num)
{
    located_block_num_ = located_block_num;
}

void GetBlockLocationsReplyPacket::set_v_located_block(vector<LocatedBlock> &v_located_block)
{
    v_located_block_ = v_located_block;

    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
}
}  // end of namespace message
}  // end of namespace bladestore
