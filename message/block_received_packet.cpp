#include "blade_common_data.h"
#include "bladestore_ops.h"
#include "block_received_packet.h"

namespace bladestore 
{
namespace message
{
//BlockReceivedPacket
BlockReceivedPacket::BlockReceivedPacket()
{
    operation_ = OP_BLOCK_RECEIVED;
    ds_id_ = 0;//wait for a really
}

BlockReceivedPacket::BlockReceivedPacket(uint64_t ds_id,
                                         BlockInfo & block_info)
{
    operation_ = OP_BLOCK_RECEIVED;
    ds_id_ = ds_id;
    block_info_.set_block_id(block_info.block_id());
    block_info_.set_block_version(block_info.block_version());
}


BlockReceivedPacket::~BlockReceivedPacket() 
{
}

size_t BlockReceivedPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(ds_id_) + sizeof(block_info_.block_id())
                        + sizeof(block_info_.block_version()); 
    return local_size;
}

int BlockReceivedPacket::Pack() 
{ 
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();

    unsigned char *net_write = (unsigned char *)malloc(length_);
    if (NULL == net_write)
    {
        return BLADE_NETDATA_PACK_ERROR;
    }

    net_data_->set_write_data(net_write, length_);
    if (net_data_) 
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteUint64(ds_id_);
        net_data_->WriteInt64(block_info_.block_id());
        net_data_->WriteInt32(block_info_.block_version());

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } 
    else 
    {
        free(net_write);
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int BlockReceivedPacket::Unpack() 
{
    if (net_data_) 
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetUint64(ds_id_);
        int64_t block_id = 0;
        net_data_->GetInt64(block_id);
        block_info_.set_block_id(block_id);
        int32_t block_version = 0;
        net_data_->GetInt32(block_version);
        block_info_.set_block_version(block_version);

        return BLADE_SUCCESS;
    } 
    else
    { 
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

}//end of namespace message
}//end of namespace bladestore
