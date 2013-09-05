/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: add_block.cpp
 * Description: This file defines AddBlock packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 * Revised :
 */

#include "add_block_packet.h"
#include "blade_net_util.h"

namespace bladestore
{
namespace message
{
// using namespace std;

// AddBlockPacket
AddBlockPacket::AddBlockPacket(): file_id_(0)
{
    length_ = 0;
    operation_ = OP_ADDBLOCK;
}

AddBlockPacket::AddBlockPacket(int64_t file_id, int8_t is_safe_write)
                    : file_id_(file_id), is_safe_write_(is_safe_write)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_ADDBLOCK;
}

AddBlockPacket::~AddBlockPacket() {}

int AddBlockPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt64(file_id_);
        net_data_->WriteInt8(is_safe_write_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int AddBlockPacket::Unpack()
{
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt64(file_id_);
        net_data_->GetInt8(is_safe_write_);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int AddBlockPacket::Reply(BladePacket * resp_packet)
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

size_t AddBlockPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(file_id_) + sizeof(is_safe_write_);
    return local_size;
}

int64_t AddBlockPacket::file_id()
{
    return file_id_;
}

int8_t AddBlockPacket::is_safe_write()
{
    return is_safe_write_;
}

// AddBlockReplyPacket
AddBlockReplyPacket::AddBlockReplyPacket(): ret_code_(0), dataserver_num_(0),
            located_block_()
{
    length_ = 0;
    operation_ = OP_ADDBLOCK_REPLY;
}

AddBlockReplyPacket::AddBlockReplyPacket(int16_t ret_code, int64_t block_id,
                                         int32_t block_version, int64_t offset,
                                         const vector<uint64_t> &dataserver_id):
    ret_code_(ret_code), dataserver_num_(dataserver_id.size()),
    located_block_(block_id, block_version, offset, 0, dataserver_id)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_ADDBLOCK_REPLY;
}

AddBlockReplyPacket::~AddBlockReplyPacket() {}

int AddBlockReplyPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt16(ret_code_);
        if (ret_code_ == RET_SUCCESS) {
            net_data_->WriteInt16(dataserver_num_);
            net_data_->WriteInt64(located_block_.block_id());
            net_data_->WriteInt32(located_block_.block_version());
            net_data_->WriteInt64(located_block_.offset());
            //net_data_->write_int64(located_block_.get_length());
            for (vector<int64_t>::size_type ix = 0;
                    ix != located_block_.dataserver_id().size(); ++ix) {
                net_data_->WriteUint64(
                    (located_block_.dataserver_id())[ix]);
            }
        }
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int AddBlockReplyPacket::Unpack()
{
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt16(ret_code_);
        if (ret_code_ == RET_SUCCESS) {
            net_data_->GetInt16(dataserver_num_);
            int64_t block_id;
            net_data_->GetInt64(block_id);
            int32_t block_version;
            net_data_->GetInt32(block_version);
            int64_t offset;
            net_data_->GetInt64(offset);
            //int64_t length;
            //net_data_->get_int64(length);
            vector<uint64_t> dataserver_id;
            for (int ix = 0; ix != dataserver_num_; ++ix) {
                uint64_t dataserver;
                net_data_->GetUint64(dataserver);
                dataserver_id.push_back(dataserver);
            }
            LocatedBlock lb(block_id, block_version, offset,
                            0, dataserver_id);
            located_block_ = lb;
        }
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

size_t AddBlockReplyPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(ret_code_)
                        + sizeof(dataserver_num_) + located_block_.GetSize();
    return local_size;
}

int16_t AddBlockReplyPacket::ret_code()
{
    return ret_code_;
}

int16_t AddBlockReplyPacket::dataserver_num()
{
    return dataserver_num_;
}

LocatedBlock &AddBlockReplyPacket::located_block()
{
    return located_block_;
}

void AddBlockReplyPacket::set_ret_code(int16_t ret_code)
{
    ret_code_ = ret_code;
}

void AddBlockReplyPacket::set_dataserver_num(int16_t dataserver_num)
{
    dataserver_num_ = dataserver_num;
}

void AddBlockReplyPacket::set_located_block(LocatedBlock &located_block)
{
    located_block_.set_block_id(located_block.block_id());
    located_block_.set_block_version(located_block.block_version());
    located_block_.set_offset(located_block.offset());
    located_block_.set_length(located_block.length());
    located_block_.set_dataserver_id(located_block.dataserver_id());

    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
}

//LogAddBlockPacket
LogAddBlockPacket::LogAddBlockPacket() : file_id_(0), ds_num_(0)
{
    length_ = 0;
    operation_ = OP_NS_ADD_BLOCK;
}

LogAddBlockPacket::LogAddBlockPacket(int64_t file_id, vector<uint64_t> &results,int8_t is_safe_write) : 
    file_id_(file_id), ds_num_(results.size()), ds_vector_(results), is_safe_write_(is_safe_write)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_NS_ADD_BLOCK;
}

LogAddBlockPacket::~LogAddBlockPacket()
{
}

int LogAddBlockPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt64(file_id_);
        net_data_->WriteInt16(ds_num_);
        for(vector<uint64_t>::const_iterator it = ds_vector_.begin(); it != ds_vector_.end(); ++it)
            net_data_->WriteUint64(*it);

        net_data_->WriteInt8(is_safe_write_);
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int LogAddBlockPacket::Unpack()
{
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt64(file_id_);
        net_data_->GetInt16(ds_num_);

        uint64_t ds_id;
        for(int i = 0; i < ds_num_; ++i)
        {
            net_data_->GetUint64(ds_id);
            ds_vector_.push_back(ds_id);
        }
        net_data_->GetInt8(is_safe_write_);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int64_t LogAddBlockPacket::file_id()
{
    return file_id_;
}

int8_t LogAddBlockPacket::is_safe_write()
{
    return is_safe_write_;
}

vector<uint64_t> &LogAddBlockPacket::ds_vector()
{
    return ds_vector_;
}

size_t LogAddBlockPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(file_id_) + sizeof(ds_num_);
    for(vector<uint64_t>::const_iterator it = ds_vector_.begin(); it != ds_vector_.end(); ++it)
        local_size += sizeof(*it);

    local_size += sizeof(is_safe_write_);

    return local_size;
}

}  // end of namespace message
}  // end of namespace bladestore
