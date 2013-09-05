#include <stdlib.h>
#include "int_to_string.h"
#include "block_meta_info.h"

namespace bladestore
{
namespace dataserver 
{

BlockMetaInfo::BlockMetaInfo() :
    block_id_(0), num_bytes_(0), 
    block_version_(0), file_id_(0)
{
}

BlockMetaInfo::BlockMetaInfo(int64_t block_id, int64_t num_bytes, 
        int32_t block_version,
        int64_t file_id,  
        const char *block_folderpath)
{
    block_id_           = block_id;
    num_bytes_          = num_bytes;
    block_version_      = block_version;
    file_id_            = file_id;
    block_folderpath_   = string(block_folderpath);
}

BlockMetaInfo::BlockMetaInfo(const BlockMetaInfo & block_meta_info)
{
    block_id_           = block_meta_info.block_id_;
    num_bytes_          = block_meta_info.num_bytes_;
    block_version_      = block_meta_info.block_version_;
    file_id_            = block_meta_info.file_id_;
    block_folderpath_   = block_meta_info.block_folderpath_;
}

string  BlockMetaInfo::block_folderpath() const
{
    if ('/' != block_folderpath_[block_folderpath_.size() - 1])
        return block_folderpath_ + "/";
    return block_folderpath_;
}

string  BlockMetaInfo::block_dst_path() const
{
    // subdir/blk_id
    return block_folderpath() + kBlockFilePrefix + Int64ToString(block_id_);
}

string  BlockMetaInfo::blockmeta_dst_path() const
{
    // subdir/blk_id_version_fid.meta
    return block_folderpath() + kBlockFilePrefix + Int64ToString(block_id_) 
        + kDelimiter+ Int32ToString(block_version_) + kDelimiter 
        + Int64ToString(file_id_) + kMetaFilePostfix; 
}


}//end of namespace common
}//end of namespace bladestore
