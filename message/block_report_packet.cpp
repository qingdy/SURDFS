/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * Version : 0.1
 * Author  : funing
 * Date    : 2012-3-30
 */
#include "bladestore_ops.h"
#include "blade_common_data.h"
#include "block_report_packet.h"
#include "blade_net_util.h"

namespace bladestore 
{
namespace message
{
//BlockReportPacket
BlockReportPacket::BlockReportPacket()
{
    operation_ = OP_BLOCK_REPORT;
    ds_id_ = 0;//wait for a really
}

BlockReportPacket::BlockReportPacket(uint64_t ds_id, set<BlockInfo *> &block_info)
{
    operation_ = OP_BLOCK_REPORT;
    ds_id_ = ds_id;
    report_blocks_info_ = block_info ;
}

BlockReportPacket::~BlockReportPacket() 
{
    for(set<BlockInfo* >::iterator iter = report_blocks_info_.begin(); 
            iter != report_blocks_info_.end(); iter++) {
        if (*iter) {
            delete *iter;
        }
    }
    report_blocks_info_.clear();
}
size_t BlockReportPacket::GetLocalVariableSize()
{
    size_t local_length = 0;
    local_length = sizeof(uint64_t) + sizeof(int32_t)//size of set<BlockInfo*>
        + (sizeof(int64_t) + sizeof(int32_t) + sizeof(int64_t)) * report_blocks_info_.size();
    return local_length;
}


int BlockReportPacket::Pack() 
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize() ;
    unsigned char *net_write = (unsigned char *)malloc(length_);                                                                
    if (NULL == net_write)
        return BLADE_NETDATA_PACK_ERROR;

    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteUint64(ds_id_);
        net_data_->WriteInt32(report_blocks_info_.size());
        for(set<BlockInfo* >::iterator iter = report_blocks_info_.begin(); 
                        iter != report_blocks_info_.end(); iter++) {
            net_data_->WriteInt64((*iter)->block_id());       
            net_data_->WriteInt32((*iter)->block_version()); 
            net_data_->WriteInt64((*iter)->file_id());       
        }
        
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int BlockReportPacket::Unpack() 
{
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetUint64(ds_id_);
        
        int32_t numBlocks = 0;
        net_data_->GetInt32(numBlocks);
        for(; numBlocks != 0; --numBlocks) {                
            BlockInfo* temp = new BlockInfo();
            int64_t block_id;
            net_data_->GetInt64(block_id);
            temp->set_block_id(block_id);
            int32_t block_version;
            net_data_->GetInt32(block_version);
            temp->set_block_version(block_version);
            int64_t file_id;
            net_data_->GetInt64(file_id);
            temp->set_file_id(file_id);
            report_blocks_info_.insert(temp);
        }
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int BlockReportPacket::Reply(BladePacket *resp_packet)
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

//BlockReportReplyPacket
BlockReportReplyPacket::BlockReportReplyPacket(): ret_code_(0) 
{
    length_ = 0;
    operation_ = OP_BLOCK_REPORT_REPLY;
}

BlockReportReplyPacket::BlockReportReplyPacket(int16_t ret_code): ret_code_(ret_code)
{
    length_ = 0;
    operation_ = OP_BLOCK_REPORT_REPLY;
}

BlockReportReplyPacket::~BlockReportReplyPacket() 
{
}

size_t BlockReportReplyPacket::GetLocalVariableSize()
{
    size_t local_length = sizeof(ret_code_);
    return local_length;    
}


int BlockReportReplyPacket::Pack()  //sender fill amframe packet's content 
{    
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    unsigned char *net_write = (unsigned char *)malloc(length_);                                                                
    if (NULL == net_write)
        return BLADE_NETDATA_PACK_ERROR;

    net_data_->set_write_data(net_write, length_);
    if(net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt16(ret_code_);
        
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int BlockReportReplyPacket::Unpack() //receiver fill local struct
{
    if(net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt16(ret_code_);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

}//end of namespace message
}//end of namespace bladestore
