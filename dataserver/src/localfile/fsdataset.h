#ifndef DATASERVER_FSDATASET_H
#define DATASERVER_FSDATASET_H

#include <stdio.h>
#include <unistd.h>
#include <ext/hash_map>
#include <string>
#include <set>
#include <map>
#include "bladestore_ops.h"
#include "blade_rwlock.h"
#include "blade_common_data.h"
#include "blade_common_define.h"
#include "block_meta_info.h"
#include "block.h"
#include "fsdir.h"
#include "readblockinfo.h"
#include "writeblockfd.h"

using namespace std;
using namespace __gnu_cxx;
using namespace pandora;
using namespace bladestore::message;
using namespace bladestore::common;

namespace bladestore
{
namespace dataserver
{

// class Block;
// class FSDir;
// class ReadBlockInfo;
// class WriteBlockFd;

class FSDataSet
{
public:
    FSDataSet();
    ~FSDataSet();
    /*
     * @para dir,        the path to store data-file & meta-file
     * @para tmp_dir,    the path to store replicating data-file & meta-file
     */
    void Init(const string & dir,const string & tmp_dir);

    //read and write block operations
    ReadBlockInfo *ReadBlockAndMetaFile(const Block & block);
    WriteBlockFd  *WriteBlockAndMetaFd(const Block & block);
    WriteBlockFd  *PrepareReplicate(const Block & block);

    /* 
     * close block file and meta file
     * @para block,      the block be closed
     * @para block_fd,   block's fd
     * @para meta_fd,    meta file's fd 
     * @para mode,       
     *  kWriteMode, close when write failed      
     *  kSafeWriteMode, close after safe write failed or succeed
     *  kReadMode, close after read
     *  kReplicateMode, close after replicate failed
     *  2 other successfule case kWriteCompleteMode, kReplicateCompleteMode,
     */
    bool CloseFile(const Block & block, int32_t block_fd, 
                   int32_t meta_fd, int8_t mode);
    void RemoveBlocks(set<int64_t> & blockid);
    bool RemoveBlock(int64_t id);

    //return length, -1 on failed
    int64_t GetBlockLength(const Block & block);
    //@para blocks_info, [OUT] 
    void GetBlockReport  (set<BlockInfo *> & blocks_info);
    /* 
     * @para block_id, 
     * @return 0 if not find the block_id in the hash_map
     */
    int64_t BlockIdToFileId (int64_t block_id);

    // FIXME : this function is used to debug, comment when release
    void GetBlockMap(hash_map<int64_t, BlockMetaInfo> &block_map){block_map = block_map_;}

    // return block num
    int64_t GetBlockNum();

private:
    //void ClearTempDirecdory(const string &tmp_dir);
    void CleanOrCreateTempDirectory(const string &tmp_dir);
    // bool BlockFileExists(const Block &block);
    // bool MetaFileExists(const Block &block);

    //@return, block's fd
    // int32_t GetBlockFileDescription (const Block &block, int64_t seek_offset);
    //@return block's fd
    // int32_t GetBlockFile(const Block &block);
    //find the block's local path
    // string GetFile(const Block & block);
    
    //@return, block's meta-file fd
    // int32_t GetMetaFileDescription (const Block & block, int64_t seek_offset);
    //find meta-file's local path
    // string GetMetaFile(const Block & block);
    //string GetMetaFile(const string & src, const Block & block);

    //tool func
    void ConstructPath(string &block_path, string &meta_path, const Block &block);
    
    //for CloseFile(), used under according modes
    bool CloseUncomplete(const Block & block, int32_t block_fd, int32_t meta_fd);
    bool CloseComplete(const Block & block, int32_t block_fd, int32_t meta_fd);
    bool ReplicateComplete(const Block & block, int32_t block_fd, int32_t meta_fd);
    bool ReplicateUncomplete(const Block & block, int32_t block_fd, int32_t meta_fd);

private:
    /* blockid -> blockmetainfo */
    hash_map<int64_t, BlockMetaInfo>     block_map_;
    FSDir *                              data_dir_ ;
    string                               tmp_dir_;
    CRWLock                              rw_lock_;
};

}//end of namespace dataserver
}//end of namespace bladestore

#endif
