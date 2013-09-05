#include "amframe.h"
#include "singleton.h"
#include "log.h"
#include "bladestore_ops.h"
#include "block_to_get_length_packet.h"
#include "blade_net_util.h"

namespace bladestore 
{
namespace message
{
/*---------------------------BlockToGetLengthPacket--------------------------*/
BlockToGetLengthPacket::BlockToGetLengthPacket()
{
    operation_ = OP_BLOCK_TO_GET_LENGTH;
}

BlockToGetLengthPacket::BlockToGetLengthPacket(int64_t block_id,
                                               int32_t block_version)
                 : block_id_(block_id), block_version_(block_version)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_BLOCK_TO_GET_LENGTH;
}

BlockToGetLengthPacket::~BlockToGetLengthPacket() 
{
}

size_t BlockToGetLengthPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(int64_t) + sizeof(int32_t);
    return local_size;
}

int BlockToGetLengthPacket::Pack()
{ //sender fill amframe packet's content
    unsigned char *net_write = (unsigned char *)malloc(length_); 
    if (NULL == net_write)
        return BLADE_NETDATA_PACK_ERROR;

    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt64(block_id_);
        net_data_->WriteInt32(block_version_);
        
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    }
    else {
        free(net_write);
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int BlockToGetLengthPacket::Unpack() { //receiver fill local struct
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt64(block_id_);
        net_data_->GetInt32(block_version_);
        return BLADE_SUCCESS;
    }
    else return BLADE_NETDATA_UNPACK_ERROR;
}

int BlockToGetLengthPacket::Reply(BladePacket *resp_packet)
{

    BlockToGetLengthReplyPacket *p = static_cast<BlockToGetLengthReplyPacket *>(resp_packet);
    p->set_channel_id(channel_id());
    int32_t ret = p->Pack();
    if (BLADE_SUCCESS != ret) {
        delete p;
        LOGV(LL_ERROR, "pack error");
        return BLADE_ERROR;
    }
    if ((0 != peer_id_)&&(BladeNetUtil::GetPeerID(endpoint_.GetFd()) == peer_id_))
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
        LOGV(LL_INFO, "send read reply ok;blk_id:%d; start_offset:%d", 
             p->block_id(), p->block_version());
        return BLADE_SUCCESS;
    }
}

BlockToGetLengthPacket * BlockToGetLengthPacket::CopySelf()
{
	BlockToGetLengthPacket * packet = new BlockToGetLengthPacket();
	packet->block_id_ = block_id_;
	packet->block_version_ = block_version_;
    packet->length_ = length_;
	return packet;
}

/*---------------------BlockToGetLengthReplyPacket----------------------*/
BlockToGetLengthReplyPacket::BlockToGetLengthReplyPacket(): ret_code_(0) 
{
    length_ = 0;
    operation_ = OP_BLOCK_TO_GET_LENGTH_REPLY;
}

BlockToGetLengthReplyPacket::BlockToGetLengthReplyPacket(int16_t ret_code,
                    uint64_t ds_id, int64_t block_id, int32_t block_version,
                    int64_t block_length) : ret_code_(ret_code), ds_id_(ds_id),
                    block_id_(block_id), block_version_(block_version),
                    block_length_(block_length)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_BLOCK_TO_GET_LENGTH_REPLY;
}

BlockToGetLengthReplyPacket::~BlockToGetLengthReplyPacket() {}


size_t BlockToGetLengthReplyPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(ret_code_) + sizeof(ds_id_) + sizeof(block_id_) +
                        sizeof(block_version_) + sizeof(block_length_); 
    return local_size;
}


int BlockToGetLengthReplyPacket::Pack()  //sender fill amframe packet's content 
{
    unsigned char *net_write = (unsigned char *)malloc(length_); 
    if (NULL == net_write)
        return BLADE_NETDATA_PACK_ERROR;

    net_data_->set_write_data(net_write, length_);
    if(net_data_) 
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt16(ret_code_);
        net_data_->WriteUint64(ds_id_);
        net_data_->WriteInt64(block_id_);
        net_data_->WriteInt32(block_version_);
        net_data_->WriteInt64(block_length_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } 
    else {
        free(net_write);
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int BlockToGetLengthReplyPacket::Unpack() //receiver fill local struct
{
    if(net_data_)
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt16(ret_code_);
        net_data_->GetUint64(ds_id_);
        net_data_->GetInt64(block_id_);
        net_data_->GetInt32(block_version_);
        net_data_->GetInt64(block_length_);

        return BLADE_SUCCESS;
    } 
    else
        return BLADE_NETDATA_UNPACK_ERROR;
}

}//end of namespace message
}//end of namespace bladestore
