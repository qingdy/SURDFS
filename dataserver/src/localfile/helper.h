#ifndef BLADE_STORE_DATASERVER_HELPER_H
#define BLADE_STORE_DATASERVER_HELPER_H

#include <vector>
#include "block.h"
#include "block_meta_info.h"

using namespace bladestore::common;

namespace bladestore
{
namespace dataserver
{

class CHelper
{
public:
    static int32_t          IsDirName(const char *path);
    static void             GetBlockMetaInfoFromMetaPath(const char *metafilename, BlockMetaInfo &block_meta_info);
    static int32_t          GetSubdirIndex(const char *dirname); 
    static int32_t          IsBlockFile(const char *filename);
    static int32_t          IsMetaFile(const char *metafilename);
    static void             GetSubdirIndexVector(const char* dataset_path, const char* block_folderpath, vector<int32_t> &vector);
    static int32_t          DirCheckAndCreate(const char* dir_path);
private:
    CHelper();
    ~CHelper();
};

}//end of dataserver
}//end of bladestore

#endif
