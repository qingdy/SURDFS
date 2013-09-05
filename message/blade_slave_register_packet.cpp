#include <assert.h>

#include "blade_slave_register_packet.h"
#include "blade_net_util.h"

namespace bladestore 
{
namespace message
{

//BladeRenewLeasePacket
BladeSlaveRegisterPacket::BladeSlaveRegisterPacket()
{
    operation_ = OP_NS_SLAVE_REGISTER;
    slave_id_ = 0;//wait for a really
}

BladeSlaveRegisterPacket::~BladeSlaveRegisterPacket() 
{

}

size_t BladeSlaveRegisterPacket::GetLocalVariableSize()
{
    size_t local_length = 0;
    local_length = sizeof(slave_id_);
    return local_length;
}


int BladeSlaveRegisterPacket::Pack() 
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize() ;
    unsigned char *net_write = (unsigned char *)malloc(length_);                                                                
    net_data_->set_write_data(net_write, length_);
    if (net_data_) 
	{
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteUint64(slave_id_);
        
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    }
    else 
	{
        return BLADE_NETDATA_PACK_ERROR;
	}
}

int BladeSlaveRegisterPacket::Unpack() 
{ 
    if (net_data_) 
	{
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetUint64(slave_id_);
        
        return BLADE_SUCCESS;
    }
    else return BLADE_NETDATA_UNPACK_ERROR;
}

int BladeSlaveRegisterPacket::Reply(BladePacket *resp_packet)
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


BladeSlaveRegisterReplyPacket::BladeSlaveRegisterReplyPacket(): ret_code_(0) 
{
    length_ = 0;
    operation_ = OP_NS_SLAVE_REGISTER_REPLY;
}

BladeSlaveRegisterReplyPacket::~BladeSlaveRegisterReplyPacket() 
{

}


size_t BladeSlaveRegisterReplyPacket::GetLocalVariableSize()
{
    size_t local_length = 0;
    local_length = sizeof(ret_code_) + 3*sizeof(uint64_t) + 2*sizeof(int8_t);
    return local_length;
}


int BladeSlaveRegisterReplyPacket::Pack()  //sender fill amframe packet's content 
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    unsigned char *net_write = (unsigned char *)malloc(length_);                                                                
    net_data_->set_write_data(net_write, length_);
    if(net_data_)
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt16(ret_code_);
        net_data_->WriteUint64(fetch_param_.min_log_id_);
        net_data_->WriteUint64(fetch_param_.max_log_id_);
        net_data_->WriteUint64(fetch_param_.ckpt_id_);
		if(fetch_param_.fetch_log_)
		{
        	net_data_->WriteInt8(1);
		}
		else
		{
        	net_data_->WriteInt8(0);
		}

		if(fetch_param_.fetch_ckpt_)
		{
        	net_data_->WriteInt8(1);
		}
		else
		{
        	net_data_->WriteInt8(0);
		}

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    }
    else
	{
        return BLADE_NETDATA_PACK_ERROR;
	}
}

int BladeSlaveRegisterReplyPacket::Unpack() //receiver fill local struct
{
    if(net_data_)
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt16(ret_code_);
  
        net_data_->GetUint64(fetch_param_.min_log_id_);
        net_data_->GetUint64(fetch_param_.max_log_id_);
        net_data_->GetUint64(fetch_param_.ckpt_id_);
		int8_t tmp;
        net_data_->GetInt8(tmp);
		fetch_param_.fetch_log_ = (tmp == 1);
        net_data_->GetInt8(tmp);
		fetch_param_.fetch_ckpt_ = (tmp == 1);
        return BLADE_SUCCESS;
    }
    else
	{
        return BLADE_NETDATA_UNPACK_ERROR;
	}
}

}//end of namespace message
}//end of namespace bladestore
