/*
 *version : 1.0
 *author  : chen yunyun, funing
 *date    : 2012-4-18
 * some common datastructure used by ns, ds and client;
 */
#ifndef BLADESTORE_COMMON_DATA_H
#define BLADESTORE_COMMON_DATA_H

#include "amframe.h"

namespace bladestore
{
namespace common
{
using namespace pandora;

class BladeCommonData
{
public:
    BladeCommonData()
    {
    
    }
    ~BladeCommonData()
    {
    
    }
    static    AmFrame amframe_;
};

//数据块元信息，包括自身ID，自身所属的文件的ID，版本号等；
class BlockInfo
{
public:
    BlockInfo()
    {
        block_id_ = 0;
        version_ = 0;
        file_id_ = 0;
    }
    BlockInfo(int64_t block_id, int32_t block_version) 
    {
        block_id_ = block_id;
        version_  = block_version;
        file_id_  = 0; 
    }
    BlockInfo(int64_t block_id, int32_t block_version, int64_t file_id) 
    {
        block_id_ = block_id;
        version_  = block_version;
        file_id_  = file_id; 
    }
    int64_t block_id()      const {return block_id_;}
    int32_t block_version() const {return version_;}
    int64_t file_id()       const {return file_id_;}
    void    set_block_id(int64_t block_id)           { block_id_ = block_id;}
    void    set_block_version(int32_t block_version) { version_ = block_version;}
    void    set_file_id(int64_t file_id) { file_id_ = file_id;}

    bool operator < (const BlockInfo & right) const
    {
        return (block_id_ < right.block_id_) || ((block_id_ == right.block_id_) && (version_ < right.version_));   
    }

private:
    int64_t block_id_;
    int64_t file_id_;
    int32_t version_;
};

struct WakeStruct
{
    int id_;
};

//用于内存map中Block的遍历
enum BLOCK_SCANNER_STATUS
{
    SCANNER_CONTINUE = 0x00,
    SCANNER_NEXT,
    SCANNER_SKIP,
    SCANNER_BREAK
};

//DS上报的基本负载信息
typedef struct DataServerMetrics {
    int64_t total_space_;//Bytes
    int64_t used_space_;//Bytes
    int32_t num_connection_;
    int32_t cpu_load_; 
}DataServerMetrics;

}//end of namespace common
}//end of namespace bladestore
#endif
