
#include "bladestore_ops.h"
#include "write_pipeline_packet.h"

namespace bladestore 
{
namespace message
{

//--------------------------------app of WritePipelinePacket----------------------------//
WritePipelinePacket::WritePipelinePacket() : file_id_(0), block_id_(0),
                                             block_version_(0),
                                             is_safe_write_(0), target_num_(0)
{
    length_ = 0;
    operation_ = OP_WRITEPIPELINE;
}

WritePipelinePacket::WritePipelinePacket(WritePipelinePacket &write_pipeline)
{
    SetContent(write_pipeline.content_, write_pipeline.length_);
    set_endpoint(&(write_pipeline.endpoint_));
    set_channel_id(write_pipeline.channel_id());
}

WritePipelinePacket::WritePipelinePacket(int64_t file_id, int64_t block_id,
                                         int32_t block_version,
                                         int8_t is_safe_write,
                                         const vector<uint64_t> & ids)
                : file_id_(file_id), block_id_(block_id), block_version_(block_version), 
                is_safe_write_(is_safe_write), target_num_(ids.size()),
                dataserver_ids_(ids)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_WRITEPIPELINE;
}


WritePipelinePacket::~WritePipelinePacket() 
{
}

//invoke by dataserver ; sender fill amframe packet's content
int WritePipelinePacket::Pack()
{
	unsigned char *net_write = (unsigned char *)malloc(length_);
    if (NULL == net_write)
        return BLADE_NETDATA_PACK_ERROR;

    net_data_->set_write_data(net_write, length_);
    if (net_data_){
        net_data_ -> WriteSize(length_);
        net_data_ -> WriteInt16(operation_);
        net_data_ -> WriteInt64(file_id_);
        net_data_ -> WriteInt64(block_id_);
        net_data_ -> WriteInt32(block_version_);
        net_data_ -> WriteInt8(is_safe_write_);
        net_data_ -> WriteInt8(target_num_);
        for(vector<uint64_t>::const_iterator iter = dataserver_ids_.begin();
            iter != dataserver_ids_.end(); ++iter) {
            net_data_ -> WriteUint64((*iter));
        }
		SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    }
    else {
        free(net_write);
        return BLADE_NETDATA_PACK_ERROR;
    }
}


//invoke by dataserver ; receiver fill local struct
int WritePipelinePacket::Unpack()
{
    if (net_data_){
        net_data_ -> GetSize(length_);
        net_data_ -> GetInt16(operation_);
        net_data_ -> GetInt64(file_id_);
        net_data_ -> GetInt64(block_id_);
        net_data_ -> GetInt32(block_version_);
        net_data_ -> GetInt8(is_safe_write_);
        net_data_ -> GetInt8(target_num_);
        for(int8_t i=0; i<(target_num_); ++i) {
            uint64_t dataserver_id;
            net_data_ -> GetUint64(dataserver_id);
            dataserver_ids_.push_back(dataserver_id);
        }
        return BLADE_SUCCESS;
    }
    else
        return BLADE_NETDATA_UNPACK_ERROR;
}

size_t WritePipelinePacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(file_id_) + sizeof(block_id_) +
                        sizeof(block_version_) + sizeof(is_safe_write_) +
                        sizeof(target_num_);
    for (vector<uint64_t>::const_iterator iter = dataserver_ids_.begin();
            iter != dataserver_ids_.end(); ++iter) 
        local_size += sizeof(*iter);
    return local_size;
}

//----------------------------app of WritePipelineReplyPacket--------------------------//
WritePipelineReplyPacket::WritePipelineReplyPacket():ret_code_(0)
{
    length_ = 0;
    block_id_ = 0;
    block_version_ = 0;
    operation_ = OP_WRITEPIPELINE_REPLY;
}

WritePipelineReplyPacket::WritePipelineReplyPacket(int64_t block_id, 
                int32_t block_version, int16_t ret_code) : block_id_(block_id),
                block_version_(block_version), ret_code_(ret_code)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_WRITEPIPELINE_REPLY;
}


WritePipelineReplyPacket::~WritePipelineReplyPacket()
{
}

//invoke by dataserver
int WritePipelineReplyPacket::Pack()
{
	unsigned char *net_write = (unsigned char *)malloc(length_);
    if (NULL == net_write)
        return BLADE_NETDATA_PACK_ERROR;

    net_data_->set_write_data(net_write, length_);
    if (net_data_){
        net_data_ -> WriteSize(length_);
        net_data_ -> WriteInt16(operation_);
        net_data_ -> WriteInt64(block_id_);
        net_data_ -> WriteInt32(block_version_);
        net_data_ -> WriteInt16(ret_code_);

		SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    }
    else {
        free(net_write);
        return BLADE_NETDATA_PACK_ERROR;
    }
}

//invoke by client
int WritePipelineReplyPacket::Unpack()
{
    if (net_data_){
        net_data_ -> GetSize(length_);
        net_data_ -> GetInt16(operation_);
        net_data_ -> GetInt64(block_id_);
        net_data_ -> GetInt32(block_version_);
        net_data_ -> GetInt16(ret_code_);
        return BLADE_SUCCESS;
    }
    else
        return BLADE_NETDATA_UNPACK_ERROR;
}

size_t WritePipelineReplyPacket::GetLocalVariableSize()
{
    size_t local_size = sizeof(block_id_) +sizeof(block_version_) + sizeof(ret_code_);
    return local_size;
}

}//end of namespace message
}//end of namespace bladestore
