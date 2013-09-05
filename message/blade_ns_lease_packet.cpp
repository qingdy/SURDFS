/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * Version : 0.1
 * Author  : funing
 * Date    : 2012-3-30
 */
#include "blade_ns_lease_packet.h"
#include "blade_net_util.h"

namespace bladestore 
{
namespace message
{
//BladeRenewLeasePacket
BladeRenewLeasePacket::BladeRenewLeasePacket()
{
    operation_ = OP_NS_RENEW_LEASE;
    ds_id_ = 0;//wait for a really
}

BladeRenewLeasePacket::BladeRenewLeasePacket(uint64_t ds_id):ds_id_(ds_id)
{
    operation_ = OP_NS_RENEW_LEASE;
}

BladeRenewLeasePacket::~BladeRenewLeasePacket() 
{

}

size_t BladeRenewLeasePacket::GetLocalVariableSize()
{
    size_t local_length = 0;
    local_length = sizeof(ds_id_);
    return local_length;
}


int BladeRenewLeasePacket::Pack() 
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize() ;
    unsigned char *net_write = (unsigned char *)malloc(length_);                                                                
    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteUint64(ds_id_);
        
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    }
     else 
        return BLADE_NETDATA_PACK_ERROR;
}

int BladeRenewLeasePacket::Unpack() 
{ 
    if (net_data_) 
	{
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetUint64(ds_id_);
        
        return BLADE_SUCCESS;
    }
    else return BLADE_NETDATA_UNPACK_ERROR;
}

int BladeRenewLeasePacket::Reply(BladePacket *resp_packet)
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

//BladeGrantLeasePacket

BladeGrantLeasePacket::BladeGrantLeasePacket(): ret_code_(0) 
{
    length_ = 0;
    operation_ = OP_NS_GRANT_LEASE;
}

BladeGrantLeasePacket::~BladeGrantLeasePacket() 
{

}


size_t BladeGrantLeasePacket::GetLocalVariableSize()
{
    size_t local_length = 0;
    local_length = sizeof(ret_code_) + sizeof(blade_lease_);
    return local_length;    
}


int BladeGrantLeasePacket::Pack()  //sender fill amframe packet's content 
{
    
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    unsigned char *net_write = (unsigned char *)malloc(length_);                                                                
    net_data_->set_write_data(net_write, length_);
    if(net_data_) 
    {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt16(ret_code_);
        net_data_->WriteInt64(blade_lease_.lease_time_);
        net_data_->WriteInt64(blade_lease_.lease_interval_);
        net_data_->WriteInt64(blade_lease_.renew_interval_);

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } 
    else
        return BLADE_NETDATA_PACK_ERROR;
}

int BladeGrantLeasePacket::Unpack() //receiver fill local struct
{
    if(net_data_)
    {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt16(ret_code_);
  
        net_data_->GetInt64(blade_lease_.lease_time_);
        net_data_->GetInt64(blade_lease_.lease_interval_);
        net_data_->GetInt64(blade_lease_.renew_interval_);
        return BLADE_SUCCESS;
    } 
    else
        return BLADE_NETDATA_UNPACK_ERROR;
}

}//end of namespace message
}//end of namespace bladestore
