
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "blade_common_data.h"
#include "bad_block_report_packet.h"

namespace bladestore 
{
namespace message
{

BadBlockReportPacket::BadBlockReportPacket()
{
    operation_ = OP_BAD_BLOCK_REPORT;
    ds_id_ = 0;
}

BadBlockReportPacket::BadBlockReportPacket(uint64_t id, set<int64_t>& block_id) 
{
    operation_ = OP_BAD_BLOCK_REPORT;
    ds_id_ = id;
    bad_blocks_id_ = block_id;
}

BadBlockReportPacket::~BadBlockReportPacket() 
{
    bad_blocks_id_.clear();
}

size_t BadBlockReportPacket::GetLocalVariableSize()
{
    size_t local_size = 0;
    local_size = sizeof(ds_id_) + sizeof(int32_t) + sizeof(int64_t) * bad_blocks_id_.size();
    return local_size;
}

//sender fill amframe packet's content
int BadBlockReportPacket::Pack()
{
    length_ = sizeof(length_) + sizeof(operation_) + GetLocalVariableSize() ;
    unsigned char *net_write = (unsigned char *)malloc(length_);                                                                
    net_data_->set_write_data(net_write, length_);
    if (net_data_) {
        net_data_->WriteSize(length_);
        net_data_->WriteInt16(operation_);
        net_data_->WriteUint64(ds_id_);
        net_data_->WriteInt32(bad_blocks_id_.size());
        for(set<int64_t>::iterator iter = bad_blocks_id_.begin();iter != bad_blocks_id_.end();iter++) {
            net_data_->WriteInt64(*iter); 
        }
        
        SetContentPtr(net_write, length_, true);
        return BLADE_SUCCESS;
    } else { 
        return BLADE_NETDATA_PACK_ERROR;
    }
}

int BadBlockReportPacket::Unpack() { //receiver fill local struct
    if (net_data_) {
        net_data_->GetSize(length_);
        net_data_->GetInt16(operation_);
        net_data_->GetUint64(ds_id_);
        int32_t numBlocks = 0;
        int64_t tempBlockID = 0;
        net_data_->GetInt32(numBlocks);
        for (; numBlocks != 0; --numBlocks) {                
            net_data_->GetInt64(tempBlockID);
            bad_blocks_id_.insert(tempBlockID);
        }
        return BLADE_SUCCESS;
    }
    else return BLADE_NETDATA_UNPACK_ERROR;
}

}//end of namespace message
}//end of namespace bladestore
