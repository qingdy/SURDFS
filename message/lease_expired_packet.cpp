#include "lease_expired_packet.h"

namespace bladestore
{
namespace message
{
LeaseExpiredPacket::LeaseExpiredPacket():block_id_(0)
{
    length_ = 0;
    operation_ = OP_LEASEEXPIRED;
}

LeaseExpiredPacket::LeaseExpiredPacket(int64_t file_id, int64_t block_id, int32_t block_version): file_id_(file_id), block_id_(block_id), block_version_(block_version)
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize();
    operation_ = OP_LEASEEXPIRED;
}

LeaseExpiredPacket::~LeaseExpiredPacket()
{
}

int LeaseExpiredPacket::Pack()
{
    unsigned char *net_write = (unsigned char *)malloc(length_);
    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteInt64(file_id_);
        net_data_->WriteInt64(block_id_);
        net_data_->WriteInt32(block_version_);
		if(is_safe_write_)
		{
        	net_data_->WriteInt8(1);
		}
		else
		{
        	net_data_->WriteInt8(0);
		}

        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int LeaseExpiredPacket::Unpack()
{
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetInt64(file_id_);
        net_data_->GetInt64(block_id_);
        net_data_->GetInt32(block_version_);
		int8_t is_safe_write;
        net_data_->GetInt8(is_safe_write);
		if(1 == is_safe_write)
		{
			is_safe_write_ = true;
		}
		else
		{
			is_safe_write_ = false;
		}
        return BLADE_SUCCESS;
    } else {
        return BLADE_NETDATA_UNPACK_ERROR;
    }
}

int64_t LeaseExpiredPacket::file_id()
{
    return file_id_;
}

int64_t LeaseExpiredPacket::block_id()
{
    return block_id_;
}

int32_t LeaseExpiredPacket::block_version()
{
	return block_version_;
}

size_t LeaseExpiredPacket::GetLocalVariableSize()
{
    size_t local_size = 2*sizeof(int64_t) + sizeof(int8_t) + sizeof(int32_t);
    return local_size;
}

}
}
