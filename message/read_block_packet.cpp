
#include "log.h"
#include "mempool.h"
#include "singleton.h"
#include "bladestore_ops.h"
#include "blade_common_data.h"
#include "blade_net_util.h"
#include "read_block_packet.h"

namespace bladestore
{
namespace message
{

//-----------------beginning of ReadBlockPacket-----------------------//
ReadBlockPacket::ReadBlockPacket() : block_id_(0), block_version_(0),
                                    block_offset_(0), request_data_length_(0), sequence_(0) 
{
    length_ = 0;
    operation_ = OP_READBLOCK;
}
    
ReadBlockPacket::ReadBlockPacket(ReadBlockPacket &read_block)
{
    SetContent(read_block.content_, read_block.length_);
    set_endpoint(&(read_block.endpoint_));
    set_channel_id(read_block.channel_id());
}
    
ReadBlockPacket::ReadBlockPacket(int64_t block_id, int32_t block_version,
                            int64_t block_offset,int64_t request_data_length, int64_t sequence)
                        : block_id_(block_id), block_version_(block_version), 
        block_offset_(block_offset), request_data_length_(request_data_length), sequence_(sequence)
{
    length_=sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_READBLOCK;
}
    
ReadBlockPacket::~ReadBlockPacket()
{
}

int ReadBlockPacket::Pack() 
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    if (NULL == net_write)
    {
        return BLADE_NETDATA_PACK_ERROR;
    }

    net_data_->set_write_data(net_write, length_);
    if (net_data_) 
    {
        net_data_ -> WriteSize(length_);
        net_data_ -> WriteInt16(operation_);
        net_data_ -> WriteInt64(block_id_);
        net_data_ -> WriteInt32(block_version_);
        net_data_ -> WriteInt64(block_offset_);
        net_data_ -> WriteInt64(request_data_length_);
        net_data_ -> WriteInt64(sequence_);
        
        SetContentPtr(net_write, length_ ,true);
        return BLADE_SUCCESS;
    }
    else
    {
        free(net_write);
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int ReadBlockPacket::Unpack() 
{
    if (net_data_) 
    {
        net_data_ -> GetSize(length_);
        net_data_ -> GetInt16(operation_);
        net_data_ -> GetInt64(block_id_);
        net_data_ -> GetInt32(block_version_);
        net_data_ -> GetInt64(block_offset_);
        net_data_ -> GetInt64(request_data_length_);
        net_data_ -> GetInt64(sequence_);
        return BLADE_SUCCESS;
    }
    else 
    {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int ReadBlockPacket::Reply(BladePacket * resp_packet)
{

    ReadBlockReplyPacket *p = static_cast<ReadBlockReplyPacket *>(resp_packet);
    int32_t ret = p->Pack();
    if (BLADE_SUCCESS != ret) 
    {
        delete p;
        LOGV(LL_ERROR, "pack error");
        return BLADE_ERROR;
    }

    if ((0 != peer_id_)&&(BladeNetUtil::GetPeerID(endpoint_.GetFd()) == peer_id_))
    {   
        ret = Singleton<AmFrame>::Instance().SendPacket(endpoint_, p);
        LOGV(LL_DEBUG, "block_id:%ld,block_version:%d,sequece:%d",
             block_id_, block_version_, sequence_);
        if (0 != ret) 
        {
            LOGV(LL_ERROR, "send read reply packet error");
            delete p;
            return BLADE_ERROR;
        }
        else 
        {
            LOGV(LL_INFO, "send read reply packet sucess");
            return BLADE_SUCCESS;
        }
    }
    else 
    {
        LOGV(LL_ERROR, "send read reply packet error");
        delete p;
        return BLADE_ERROR;
    }
}

size_t ReadBlockPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(block_id_) + sizeof(block_version_) + 
                sizeof(block_offset_) + sizeof(request_data_length_) + sizeof(sequence_);
    return local_size;
}

//----------------beginning of ReadBlockReplyPacket-----------------------//
ReadBlockReplyPacket::ReadBlockReplyPacket():ret_code_(RET_READ_INIT),
                block_id_(0),block_offset_(0),sequence_(0),data_(NULL)
{
    length_ = 0;
    operation_ = OP_READBLOCK_REPLY;
}


ReadBlockReplyPacket::ReadBlockReplyPacket(const ReadBlockReplyPacket &read_block_reply)
{
    ret_code_ = read_block_reply.ret_code();
    block_id_ = read_block_reply.block_id();
    block_offset_ = read_block_reply.block_offset();
    sequence_ = read_block_reply.sequence();
    data_length_ = read_block_reply.data_length();
    if (data_length_) 
    {
        char *data_tmp = static_cast<char *>(MemPoolAlloc(data_length_));
        assert(data_tmp);
        memcpy(data_tmp, read_block_reply.data(), data_length_);
        data_ = data_tmp;
    }
    else 
    {
        data_ = NULL;
    }
}
        
ReadBlockReplyPacket::ReadBlockReplyPacket(int16_t ret_code,int64_t block_id,
    int64_t block_offset,int64_t sequence,int64_t data_length,char * data)
    : ret_code_(ret_code),block_id_(block_id),block_offset_(block_offset), 
    sequence_(sequence),data_length_(data_length),data_(data)   
{
    length_=sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_READBLOCK_REPLY;
}
            
ReadBlockReplyPacket::~ReadBlockReplyPacket()
{
    if(NULL != data_)
    {
        MemPoolFree(data_);
        //free data_;
        data_ = NULL;
    }
}

int ReadBlockReplyPacket::Pack()
{ 
    unsigned char *net_write = (unsigned char *)malloc(length_);
    if (NULL == net_write) 
    {
        return BLADE_NETDATA_PACK_ERROR;
    }

    net_data_->set_write_data(net_write, length_);
    if (net_data_) 
    {
        net_data_ -> WriteSize(length_);
        net_data_ -> WriteInt16(operation_);
        net_data_ -> WriteInt16(ret_code_);
        net_data_ -> WriteInt64(block_id_);
        net_data_ -> WriteInt64(sequence_);
        if (ret_code_ == RET_SUCCESS)
        {
            net_data_ -> WriteInt64(block_offset_);
            net_data_ -> WriteInt64(data_length_);
            net_data_ -> WriteChars(data_, data_length_);
        }
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    }
    else 
    {
        free(net_write);
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int ReadBlockReplyPacket::Unpack() 
{
    if (net_data_) 
    {
        net_data_ -> GetSize(length_);
        net_data_ -> GetInt16(operation_);
        net_data_ -> GetInt16(ret_code_);
        net_data_ -> GetInt64(block_id_);
        net_data_ -> GetInt64(sequence_);
        if (ret_code_ == RET_SUCCESS) 
        {
            net_data_ -> GetInt64(block_offset_);
            net_data_ -> GetInt64(data_length_);
            data_ = static_cast<char *>(MemPoolAlloc(data_length_));
            if (!data_)
            {
                return BLADE_NETDATA_UNPACK_ERROR;
            }
            net_data_ -> GetChars(data_, data_length_);
        }
        return BLADE_SUCCESS;
    }
    else 
    {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

size_t ReadBlockReplyPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(ret_code_) + sizeof(block_id_) + 
                        sizeof(block_offset_) + sizeof(sequence_) +
                        sizeof(data_length_) + data_length_;
    return local_size;
}

}//end of namespace message
}//end of namespace bladestore

