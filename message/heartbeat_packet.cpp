/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * Version : 0.1
 * Author  : funing
 * Date    : 2012-3-30
 */
#include "log.h"
#include "blade_common_define.h"
#include "blade_common_data.h"
#include "heartbeat_packet.h"
#include "blade_net_util.h"

namespace bladestore 
{
namespace message
{
//HeartbeatPacket
HeartbeatPacket::HeartbeatPacket(){
    length_ = 0;
    operation_ = OP_HEARTBEAT;
    rack_id_ = 0;
    ds_id_ = 0;
    struct DataServerMetrics temp = {0,0,0,0};
    ds_metrics_ = temp;
} 

HeartbeatPacket::HeartbeatPacket(int32_t rack_id, uint64_t dsID, DataServerMetrics  ds_metrics) {
    length_ = 0;
    operation_ = OP_HEARTBEAT;
    rack_id_ = rack_id;
    ds_id_ = dsID;
    ds_metrics_ = ds_metrics;
}

HeartbeatPacket::~HeartbeatPacket() 
{
}
    
size_t HeartbeatPacket::GetLocalVariableSize()
{
    size_t local_length = 0;
    local_length = sizeof(rack_id_) + sizeof(ds_id_) + sizeof(ds_metrics_);
    return local_length;
}


int HeartbeatPacket::Pack()
{ //sender fill amframe packet's content
    
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize() ;
    unsigned char *net_write = (unsigned char *)malloc(length_);                                                                
    net_data_->set_write_data(net_write, length_);
    if (net_data_)
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt32(rack_id_);
        net_data_->WriteUint64(ds_id_);
        net_data_->WriteInt64(ds_metrics_.total_space_);
        net_data_->WriteInt64(ds_metrics_.used_space_);
        net_data_->WriteInt32(ds_metrics_.num_connection_);
        net_data_->WriteInt32(ds_metrics_.cpu_load_);
        
        SetContentPtr(net_write, length_, true); 
        return BLADE_SUCCESS;
    }
     else 
        return BLADE_NETDATA_PACK_ERROR;
}

int HeartbeatPacket::Unpack()
{ //receiver fill local struct
    if (net_data_) 
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt32(rack_id_);
        net_data_->GetUint64(ds_id_);
        net_data_->GetInt64(ds_metrics_.total_space_);
        net_data_->GetInt64(ds_metrics_.used_space_);
        net_data_->GetInt32(ds_metrics_.num_connection_);
        net_data_->GetInt32(ds_metrics_.cpu_load_);
        return BLADE_SUCCESS;
    }
    else 
    {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int HeartbeatPacket::Reply(BladePacket *resp_packet)
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

//HeartbeatReplyPacket
HeartbeatReplyPacket::HeartbeatReplyPacket(): ret_code_(0) 
{
    length_ = 0;
    operation_ = OP_HEARTBEAT_REPLY;
}

HeartbeatReplyPacket::HeartbeatReplyPacket(int16_t ret_code): ret_code_(ret_code)
{
    length_ = 0;
    operation_ = OP_HEARTBEAT_REPLY;
}

HeartbeatReplyPacket::~HeartbeatReplyPacket() 
{
    ClearAll();
}

size_t HeartbeatReplyPacket::GetLocalVariableSize()
{
    size_t local_length = 0;
    size_t block_to_replicate_size = 0;
    
    for (map<BlockInfo,set<uint64_t> >::iterator iter = blocks_to_replicate_.begin();
        iter != blocks_to_replicate_.end(); ++iter) 
    {
        block_to_replicate_size += sizeof(int64_t) + sizeof(int32_t) + sizeof(int64_t)//class BlockInfo size
            + sizeof(int32_t) + iter->second.size() * sizeof(uint64_t);
    }
    local_length = sizeof(ret_code_) + sizeof(int32_t) + 
        sizeof(int64_t) * blocks_to_remove_.size() + sizeof(int32_t) + block_to_replicate_size;
    return local_length;    
}

void HeartbeatReplyPacket::ClearAll()
{
    blocks_to_remove_.clear();
    blocks_to_replicate_.clear();
}

int HeartbeatReplyPacket::Pack()  //sender fill amframe packet's content 
{
    
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    unsigned char *net_write = (unsigned char *)malloc(length_);                                                                
    net_data_->set_write_data(net_write, length_);
    if (net_data_) 
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt16(ret_code_);
        net_data_->WriteInt32(blocks_to_remove_.size());
        for (set<int64_t>::iterator iter = blocks_to_remove_.begin(); 
             iter != blocks_to_remove_.end(); iter++) 
        {
            net_data_->WriteInt64(*iter);
        }
                        
        net_data_->WriteInt32(blocks_to_replicate_.size());
        map<BlockInfo, set<uint64_t> >::iterator iter;
        for (iter = blocks_to_replicate_.begin(); iter != blocks_to_replicate_.end(); 
             iter++) 
        {
            net_data_->WriteInt64(iter->first.block_id());
            net_data_->WriteInt32(iter->first.block_version());
            net_data_->WriteInt64(iter->first.file_id());
            net_data_->WriteInt32(iter->second.size());
            for (set<uint64_t>::iterator dsiter = iter->second.begin(); 
                 dsiter != iter->second.end(); dsiter++ ) 
            {
                net_data_->WriteUint64(*dsiter);
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

int HeartbeatReplyPacket::Unpack() //receiver fill local struct
{
    if (net_data_) 
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt16(ret_code_);
        int32_t num_blocks = 0;
        int64_t temp_block_id = 0;
        int32_t temp_block_version = 0;
        int64_t temp_file_id = 0;
        BlockInfo temp_block;
        net_data_->GetInt32(num_blocks);
        for (; num_blocks != 0; --num_blocks) 
        {                
            net_data_->GetInt64(temp_block_id);
            blocks_to_remove_.insert(temp_block_id);
        }
        set<int64_t>::iterator iter = blocks_to_remove_.begin();
        net_data_->GetInt32(num_blocks);
        for (; num_blocks != 0; --num_blocks) 
        {
            net_data_->GetInt64(temp_block_id);
            net_data_->GetInt32(temp_block_version);
            net_data_->GetInt64(temp_file_id);
            temp_block.set_block_id(temp_block_id);
            temp_block.set_block_version(temp_block_version);
            temp_block.set_file_id(temp_file_id);
            int32_t num_ds = 0;
            net_data_->GetInt32(num_ds);
            set<uint64_t> temp_ds;
            uint64_t temp_ds_id = 0;
            for (; num_ds != 0; --num_ds) 
            {
                net_data_->GetUint64(temp_ds_id);
                temp_ds.insert(temp_ds_id);
            }
            blocks_to_replicate_.insert(make_pair(temp_block, temp_ds)); 
        }
            
        //map<BlockInfo, set<uint64_t> >::iterator iter1 = blocks_to_replicate_.begin();
        //for (; iter1!=blocks_to_replicate_.end(); iter1++) {
        //    set<uint64_t>::iterator iter2 = iter1->second.begin();
        //} 
        return BLADE_SUCCESS;
    } 
    else
        return BLADE_NETDATA_UNPACK_ERROR;
}

}//end of namespace message
}//end of namespace bladestore
