
#include "mempool.h"
#include "bladestore_ops.h"
#include "write_packet_packet.h"

namespace bladestore 
{
namespace message
{

//-------------------------WritePacketPacket---------------------//
WritePacketPacket::WritePacketPacket(): block_id_(0), block_version_(0), block_offset_(0), sequence_(0),target_num_(0), data_length_(0),data_(NULL), checksum_(NULL)
{
    length_ = 0;
    operation_ = OP_WRITEPACKET;
}

WritePacketPacket::WritePacketPacket(WritePacketPacket &write_packet)
{
    SetContent(write_packet.content_, write_packet.length_);
    set_endpoint(&(write_packet.endpoint_));
    set_channel_id(write_packet.channel_id());
    data_ = NULL;
    checksum_ = NULL;
}

WritePacketPacket::WritePacketPacket(int64_t block_id, int32_t block_version, 
        int64_t block_offset, int64_t sequence, int8_t target_num, 
        int64_t data_length, int64_t checksum_length, char * data,
        char * checksum) : block_id_(block_id), block_version_(block_version),
        block_offset_(block_offset), sequence_(sequence),
        target_num_(target_num),data_length_(data_length),
        checksum_length_(checksum_length), data_(data), checksum_(checksum)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_WRITEPACKET;
}

int WritePacketPacket::Init()
{
    char *data = data_;
    char *checksum = checksum_;
    data_ = static_cast<char *>(MemPoolAlloc(data_length_));
    if (!data_)
        return BLADE_ERROR;
    memcpy(data_, data, data_length_);
    checksum_ = static_cast<char *>(MemPoolAlloc(checksum_length_));
    if (!checksum_) {
        MemPoolFree(data_);
        data_ = NULL;
        return BLADE_ERROR;
    }
    if (checksum) {
        memcpy(checksum_, checksum, checksum_length_);
    }
    return BLADE_SUCCESS;
}

WritePacketPacket::~WritePacketPacket() 
{
    if (NULL != data_)
    {
        MemPoolFree(data_);
        data_= NULL;
    }
    if (NULL != checksum_){
        MemPoolFree(checksum_);
        checksum_  = NULL;
    }
}
int WritePacketPacket::Pack() 
{ 
    //sender fill amframe packet's content
    unsigned char *net_write = (unsigned char *)malloc(length_);
    if (NULL == net_write)
        return BLADE_NETDATA_PACK_ERROR;

    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_); 
        net_data_->WriteInt16(operation_); 
        net_data_->WriteInt64(block_id_); 
        net_data_->WriteInt32(block_version_); 
        net_data_->WriteInt64(block_offset_);
        net_data_->WriteInt64(sequence_);
        net_data_->WriteInt8(target_num_);
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

int WritePacketPacket::Unpack() 
{ 
    //receiver fill local struct
    //FIXME mempoolalloc size should +1 or not 
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt64(block_id_); 
        net_data_->GetInt32(block_version_); 
        net_data_->GetInt64(block_offset_);
        net_data_->GetInt64(sequence_);
        net_data_->GetInt8(target_num_);
        net_data_->GetInt64(data_length_);
        net_data_->GetInt64(checksum_length_);
        data_ = static_cast<char *>(MemPoolAlloc(data_length_));
        if (!data_)
            return BLADE_NETDATA_UNPACK_ERROR;
        checksum_ = static_cast<char *>(MemPoolAlloc(checksum_length_));
        if (!checksum_) {
            MemPoolFree(data_);
            data_ = NULL;
            return BLADE_NETDATA_UNPACK_ERROR;
        }

        net_data_->GetChars(data_,data_length_); 
        net_data_->GetChars(checksum_,checksum_length_); 
        return BLADE_SUCCESS;
    }
    else return BLADE_NETDATA_UNPACK_ERROR;
}


size_t WritePacketPacket::GetLocalVariableSize()
{
    size_t local_size = 0;
    local_size = sizeof(block_id_) + sizeof(block_version_) + 
                 sizeof(block_offset_) + sizeof(sequence_) + 
                 sizeof(target_num_) + sizeof(data_length_) +
                 sizeof(checksum_length_) + checksum_length_ +
                 data_length_;
    return local_size;
}

//-------------------------WritePacketReplyPacket---------------------//
WritePacketReplyPacket::WritePacketReplyPacket(): block_id_(0), sequence_(0),
                                                  ret_code_(0) 
{
    length_ = 0;
    operation_ = OP_WRITEPACKET_REPLY;
}

WritePacketReplyPacket::WritePacketReplyPacket(int64_t block_id, 
                int64_t sequence, int16_t ret_code):block_id_(block_id), 
                sequence_(sequence), ret_code_(ret_code)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_WRITEPACKET_REPLY;
}

WritePacketReplyPacket::~WritePacketReplyPacket()
{
}

//sender fill amframe packet's content
int WritePacketReplyPacket::Pack() 
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    if (NULL == net_write)
        return BLADE_NETDATA_PACK_ERROR;

    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_); 
        net_data_->WriteInt16(operation_); 
        net_data_->WriteInt64(block_id_); 
        net_data_->WriteInt64(sequence_);
        net_data_->WriteInt16(ret_code_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    }
    else {
        free(net_write);
        return BLADE_NETDATA_PACK_ERROR;
    }
}

//receiver fill local struct
int WritePacketReplyPacket::Unpack()
{
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt64(block_id_); 
        net_data_->GetInt64(sequence_);
        net_data_->GetInt16(ret_code_);
        return BLADE_SUCCESS;
    }
    else return BLADE_NETDATA_UNPACK_ERROR;
}

size_t WritePacketReplyPacket::GetLocalVariableSize()
{
    size_t local_size = 0;
    local_size = sizeof(block_id_) + sizeof(sequence_) + sizeof(ret_code_);
    return local_size;
}

}//end of namespace message
}//end of namespace bladestore
