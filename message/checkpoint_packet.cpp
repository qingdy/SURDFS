#include "checkpoint_packet.h"

namespace bladestore 
{
namespace message
{

using namespace std;

//StatusPacket
CheckPointPacket::CheckPointPacket() 
{
    length_ = sizeof(length_) + sizeof(operation_);
    operation_ = OP_CHECKPOINT;
}


CheckPointPacket::~CheckPointPacket() 
{

}

int CheckPointPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if(net_data_) 
	{
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);

		SetContentPtr(net_write, length_, true);	
        return BLADE_SUCCESS;
    }
    return BLADE_NETDATA_PACK_ERROR;
}

int CheckPointPacket::Unpack()
{
    if(net_data_) 
	{
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        return BLADE_SUCCESS;
    }

    return BLADE_NETDATA_UNPACK_ERROR;
}
   
size_t CheckPointPacket::GetLocalVariableSize() 
{
	return length_;
}

} //end of namespace message 
} //end of namespace bladestore 
