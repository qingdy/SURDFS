#include "leave_ds_packet.h"

namespace bladestore 
{
namespace message
{

using namespace std;


LeaveDsPacket::LeaveDsPacket()
{
    length_ = sizeof(length_) + sizeof(operation_) + sizeof(uint64_t);
    operation_ = OP_LEAVE_DS;
}

LeaveDsPacket::~LeaveDsPacket() 
{

}

int LeaveDsPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if(net_data_) 
	{
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteUint64(ds_id_);

		SetContentPtr(net_write, length_, true);	
        return BLADE_SUCCESS;
    }
    return BLADE_NETDATA_PACK_ERROR;
}

int LeaveDsPacket::Unpack()
{
    if(net_data_) 
	{
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetUint64(ds_id_);

        return BLADE_SUCCESS;
    }

    return BLADE_NETDATA_UNPACK_ERROR;
}

void LeaveDsPacket::set_ds_id(uint64_t ds_id)
{
	ds_id_ = ds_id;
}

uint64_t LeaveDsPacket::get_ds_id()
{
	return ds_id_;
}
   
size_t LeaveDsPacket::GetLocalVariableSize() 
{
	return length_;
}

} //end of namespace message 
} //end of namespace bladestore 
