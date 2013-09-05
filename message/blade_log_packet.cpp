#include "blade_log_packet.h"
#include "blade_common_define.h"
#include "bladestore_ops.h"
#include "blade_net_util.h"

using namespace bladestore::common;

namespace bladestore
{
namespace message
{

BladeLogPacket::BladeLogPacket() 
{
	operation_ = OP_NS_LOG;
	own_buffer_ = false;
	data_buf_ = NULL;
}

BladeLogPacket::~BladeLogPacket()
{
	if(own_buffer_)
	{
		if(NULL != data_buf_)
		{
			delete data_buf_;
		}
		data_buf_ = NULL;
	}
}

void BladeLogPacket::set_data(char * data , int32_t length)
{
	data_buf_ = data;
	data_length_ = length;	
}

int BladeLogPacket::GetSerializeSize()
{
	return sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
}

size_t BladeLogPacket::GetLocalVariableSize()
{
	return  data_length_ + sizeof(int32_t);
}

int BladeLogPacket::Pack()
{
	length_ = GetSerializeSize();
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_)
	{
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt32(data_length_);
        net_data_->WriteBytes(data_buf_, data_length_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    }
	else 
	{
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int BladeLogPacket::Unpack()
{
    if (net_data_) 
	{
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt32(data_length_);
		data_buf_ = (char *)malloc(data_length_*sizeof(char));
		own_buffer_ = true;
        net_data_->ReadBytes(data_buf_, data_length_);
        return BLADE_SUCCESS;
    }
	else 
	{
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int BladeLogPacket::Reply(BladePacket * resp_packet)
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

}//end of namespace message
}//end of namespace bladestore
