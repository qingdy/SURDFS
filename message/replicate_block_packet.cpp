
#include "log.h"
#include "singleton.h"
#include "mempool.h"
#include "bladestore_ops.h"
#include "replicate_block_packet.h"
#include "blade_net_util.h"

namespace bladestore
{
namespace message
{

//-----------------beginning of ReplicateBlockPacket-----------------------//
ReplicateBlockPacket::ReplicateBlockPacket() : block_id_(0), block_length_(0),
                        block_version_(0), file_id_(0), block_offset_(0),
                        sequence_(0), data_length_(0), checksum_length_(0)
{
    data_ = NULL;
    checksum_ = NULL;
    delete_flag_ = false;
    length_ = 0;
    operation_ = OP_REPLICATEBLOCK;
}
    
    
ReplicateBlockPacket::ReplicateBlockPacket(int64_t block_id, 
            int64_t block_length, int32_t block_version, int64_t file_id,
            int64_t block_offset, int32_t sequence, int64_t data_length,
            int64_t checksum_length, char * data, char * checksum):
            block_id_(block_id), block_length_(block_length), 
            block_version_(block_version), file_id_(file_id),
            block_offset_(block_offset), sequence_(sequence),
            data_length_(data_length), checksum_length_(checksum_length),
            data_(data), checksum_(checksum), delete_flag_(false)
{
	length_=sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_REPLICATEBLOCK;
}
    
ReplicateBlockPacket::~ReplicateBlockPacket()
{
    if (delete_flag_){ 
        if (NULL != data_) {
            MemPoolFree(data_);
            data_= NULL;
        }
        if (NULL != checksum_){
            MemPoolFree(checksum_);
            checksum_  = NULL;
        }
    }
}

int ReplicateBlockPacket::Pack() 
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    if (NULL == net_write)
        return BLADE_NETDATA_PACK_ERROR;

    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt64(block_id_);
        net_data_->WriteInt64(block_length_);
        net_data_->WriteInt32(block_version_);
        net_data_->WriteInt64(file_id_);
        net_data_->WriteInt64(block_offset_);
        net_data_->WriteInt32(sequence_);
        net_data_->WriteInt64(data_length_);
        net_data_->WriteInt64(checksum_length_);
        net_data_->WriteChars(data_, data_length_); 
        net_data_->WriteChars(checksum_, checksum_length_); 
        
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    }
    else {
        free(net_write);
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int ReplicateBlockPacket::Unpack() 
{
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt64(block_id_);
        net_data_->GetInt64(block_length_);
        net_data_->GetInt32(block_version_);
        net_data_->GetInt64(file_id_);
        net_data_->GetInt64(block_offset_);
        net_data_->GetInt32(sequence_);
        net_data_->GetInt64(data_length_);
        net_data_->GetInt64(checksum_length_);

        //memory  space  alloc out
        data_ = static_cast<char *>(MemPoolAlloc(data_length_));
        if (!data_)
            return BLADE_NETDATA_UNPACK_ERROR;
        checksum_ = static_cast<char *>(MemPoolAlloc(checksum_length_));
        if (!checksum_) {
            MemPoolFree(data_);
            data_ = NULL;
            return BLADE_NETDATA_UNPACK_ERROR;
        }
        net_data_->GetChars(data_, data_length_); 
        net_data_->GetChars(checksum_, checksum_length_); 
        set_delete_flag(true);
        return BLADE_SUCCESS;
    }
    else 
        return BLADE_NETDATA_UNPACK_ERROR;
}

int ReplicateBlockPacket::Reply(BladePacket * resp_packet)
{
    ReplicateBlockReplyPacket *p = static_cast<ReplicateBlockReplyPacket *>(resp_packet);
    int32_t ret = p->Pack();
    if (BLADE_SUCCESS != ret) {
        delete p;
        LOGV(LL_ERROR, "pack error");
        return BLADE_ERROR;
    }
    if((0 != peer_id_)&&(BladeNetUtil::GetPeerID(endpoint_.GetFd()) == peer_id_))
    {
        ret = Singleton<AmFrame>::Instance().SendPacket(endpoint_, p);
    }
    else 
    {
        ret = -1;
    }
    if (0 != ret) {
        LOGV(LL_ERROR, "sendpacket error", ret);
        return BLADE_ERROR;
    } else {
        LOGV(LL_INFO, "send read reply ok;blk_id:%d;", 
                p->block_id());
        return BLADE_SUCCESS;
    }
}



size_t ReplicateBlockPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(block_id_) + sizeof(block_length_) + 
                        sizeof(block_version_) + sizeof(file_id_) + 
                        sizeof(block_offset_) + sizeof(sequence_) +
                        sizeof(data_length_) + sizeof(checksum_length_) +
                        data_length_ + checksum_length_;
    return local_size;
}

//----------------beginning of ReplicateBlockReplyPacket-----------------------//
ReplicateBlockReplyPacket::ReplicateBlockReplyPacket()
                         : ret_code_(RET_SUCCESS), block_id_(0), sequence_(0)
{
    length_ = 0;
    operation_ = OP_REPLICATEBLOCK_REPLY;
}

		
ReplicateBlockReplyPacket::ReplicateBlockReplyPacket(int16_t ret_code, 
                                          int64_t block_id, int32_t sequence)
            : ret_code_(ret_code), block_id_(block_id), sequence_(sequence)
{
	length_=sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_REPLICATEBLOCK_REPLY;
}
			
ReplicateBlockReplyPacket::~ReplicateBlockReplyPacket()
{
}

int ReplicateBlockReplyPacket::Pack()
{ 
	unsigned char *net_write = (unsigned char *)malloc(length_);
    if (NULL == net_write)
        return BLADE_NETDATA_PACK_ERROR;

    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_ -> WriteSize(length_);
		net_data_ -> WriteInt16(operation_);
		net_data_ -> WriteInt16(ret_code_);
		net_data_ -> WriteInt64(block_id_);
        net_data_ -> WriteInt32(sequence_);

		SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
	}
    else {
        free(net_write);
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int ReplicateBlockReplyPacket::Unpack() 
{
	if (net_data_) {
		net_data_ -> GetSize(length_);
		net_data_ -> GetInt16(operation_);
		net_data_ -> GetInt16(ret_code_);
        net_data_ -> GetInt64(block_id_);
        net_data_ -> GetInt32(sequence_);
		return BLADE_SUCCESS;
	}
	else 
		return BLADE_NETDATA_UNPACK_ERROR;
}

size_t ReplicateBlockReplyPacket::GetLocalVariableSize()
{
	size_t local_size = sizeof(ret_code_) + sizeof(block_id_) +
                                            sizeof(sequence_); 
    return local_size;
}

}//end of namespace message
}//end of namespace bladestore
