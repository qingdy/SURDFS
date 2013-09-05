#ifndef BLADESTORE_COMMON_BLOCK_META_INFO_H
#define BLADESTORE_COMMON_BLOCK_META_INFO_H

#include <stdint.h>
#include <string>

namespace bladestore
{
namespace dataserver 
{
using namespace std;

const char kBlockFilePrefix[] = "blk_";
const char kMetaFilePostfix[] = ".meta";
const char kDelimiter[] = "_";
const char kSubDirName[] = "subdir";

class BlockMetaInfo
{ 
public:
    BlockMetaInfo();
    BlockMetaInfo(const BlockMetaInfo & block_meta_info);
    /*
     * @para block_id,
     * @para num_bytes, block's length
     * @para block_version,
     * @para file_id,   block's fileid
     * @para block_folder_path,      block's local path
     */
    BlockMetaInfo(int64_t block_id, int64_t num_bytes, 
                  int32_t block_version, int64_t file_id, 
                  const char *block_folderpath); 

    //get operations
    int64_t block_id() const { return block_id_; }
    int64_t num_bytes() const { return num_bytes_; }
    int32_t block_version() const { return block_version_; }
    int64_t file_id() const { return file_id_; }

    string  block_folderpath() const;
    string  block_dst_path() const;
    string  blockmeta_dst_path() const;

    //set operations
    void    set_block_id(int64_t block_id) { block_id_ = block_id; }
    void    set_block_version(int32_t block_version) { block_version_ = block_version; }
    void    set_file_id(int64_t file_id) { file_id_ = file_id; }
    void    set_num_bytes(int64_t num_bytes) { num_bytes_ = num_bytes; }
    void    set_block_folderpath(string block_folderpath) { block_folderpath_ = block_folderpath; }

private:
    int64_t block_id_;
    int64_t num_bytes_;
    int32_t block_version_;
    int64_t file_id_;
    string  block_folderpath_;
};

}
}
#endif
