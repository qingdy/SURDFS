#include <stdlib.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <vector>
#include <assert.h>
#include "errno.h"
#include "fsdataset.h"
#include "int_to_string.h"
#include "block.h"
#include "readblockinfo.h"
#include "writeblockfd.h"
#include "helper.h"

using namespace std;
using namespace pandora;

namespace bladestore
{
namespace dataserver
{

FSDataSet::FSDataSet() 
{
    data_dir_ = NULL;
} 

FSDataSet::~FSDataSet() 
{
    if (NULL != data_dir_)
        delete data_dir_;
}

void FSDataSet::Init(const string &dir, const string &tmp_dir)
{
    data_dir_ = new FSDir(dir);
    assert(data_dir_);
    data_dir_->GetBlockMap(block_map_);

    CleanOrCreateTempDirectory(tmp_dir);

    LOGV(LL_INFO, "FSDataSet : %s init completely.", dir.c_str());
}

#ifndef MAX_PATH_LEN
#define MAX_PATH_LEN 1024
#endif
void FSDataSet::CleanOrCreateTempDirectory(const string &tmp_dir)
{
    //check if tmp_dir ends with '/'
    if ('/' != tmp_dir[tmp_dir.size() - 1]) 
        tmp_dir_ = tmp_dir + "/";
    else 
        tmp_dir_ = tmp_dir;
    //check tmp_dir if it's a directory
    struct stat st;
    int ret;
    if (0 != stat(tmp_dir_.c_str(), &st)) 
    {
        if (ENOENT == errno) 
        {
            //dir not exist, make it
            ret = mkdir(tmp_dir_.c_str(), 0777); 
            if (0 != ret) 
                LOGV(LL_ERROR, "can't mkdir %s, errmsg=%s", 
                        tmp_dir_.c_str(), strerror(errno));
            return ;
        }
        //skip other error
        LOGV(LL_ERROR, "can not stat dir[%s], errmsg=%s", 
                tmp_dir_.c_str(), strerror(errno)); 
        return;
    }
    if (false == S_ISDIR(st.st_mode)) 
    {
        LOGV(LL_ERROR, "Given tmp_dir is not a dir");
        remove(tmp_dir_.c_str());
        ret = mkdir(tmp_dir_.c_str(), 0777); 
        if (0 != ret) 
            LOGV(LL_ERROR, "can't mkdir %s, errmsg=%s", 
                    tmp_dir_.c_str(), strerror(errno));
        return;
    }

    //open dir and rm all the files in the dir
    DIR * tmpdir;
    struct dirent * ent;
    string file;
    if (NULL == (tmpdir = opendir(tmp_dir_.c_str())))
    {
        LOGV(LL_ERROR, "can't open dir[%s], errmsg=%s", 
                tmp_dir_.c_str(), strerror(errno));
        return ;
    } 
    while (NULL != (ent = readdir(tmpdir)))
    {
        if (0 == (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, ".."))) 
            continue;
        file = tmp_dir_ + string(ent->d_name);
        LOGV(LL_INFO,"find file in tmp dir,tmp file:%s path:%s",
                ent->d_name,file.c_str());
        if (0 != stat(file.c_str(), &st)) continue;
        if (true == S_ISREG(st.st_mode))
        {
            if (0 != remove(file.c_str()))
                LOGV(LL_ERROR,"remove %s failed! when CleanTempDirectory, \
                        errmsg:%s -- FSDataSet",
                        file.c_str(),strerror(errno));
        }
    }
    closedir(tmpdir);
    LOGV(LL_DEBUG, "fsdataset init succ!");
    return ;
}

//XXX NOTE: remember to free the memory
ReadBlockInfo * FSDataSet::ReadBlockAndMetaFile(const Block & block)
{
    ReadBlockInfo * result = new ReadBlockInfo();
    assert(result);

    //judge : whether block file or meta file exist in the map and local
    hash_map<int64_t, BlockMetaInfo>::iterator it;
    rw_lock_.rlock()->lock();
    it = block_map_.find(block.block_id());
    if (it == block_map_.end())
    {
        rw_lock_.rlock()->unlock();
        LOGV(LL_ERROR,"block:%ld not in block_map when ReadBlockAndMetaFile",
                block.block_id());
        result->return_code_ = RET_BLOCK_NOT_EXIST_IN_DS;
        return result;
    }
    BlockMetaInfo block_meta_info(it->second);
    rw_lock_.rlock()->unlock();

    if (0 != access(block_meta_info.block_dst_path().c_str(),F_OK) 
        || 0 != access(block_meta_info.blockmeta_dst_path().c_str(),F_OK)) 
    {
        result->return_code_ = RET_BLOCK_OR_META_FILE_NOT_EXIST;
        LOGV(LL_ERROR,"block:%ld access failed when ReadBlockAndMetaFile",
                block.block_id());
        return result; 
    }

    if (block_meta_info.block_version() != block.block_version()) 
    {
        result->return_code_ = RET_READ_VERSION_NOT_MATCH;
        LOGV(LL_ERROR,"block version %d(map) != %d when ReadBlockAndMetaFile:%ld",
                block_meta_info.block_version(), 
                block.block_version(), 
                block.block_id());
        return result;
    }

    //get fd : get block file & meta file 's fd
    int32_t block_fd = open(block_meta_info.block_dst_path().c_str(), O_RDONLY);
    if (-1 == block_fd) 
    {
        result->return_code_ = RET_BLOCK_OR_META_FILE_OPEN_ERROR;
        LOGV(LL_ERROR,"open block_file error,block_fd:%d \
                when ReadBlockAndMetaFile,errmsg:%s",
                block_fd, strerror(errno));
        return result;
    }
    int32_t blockmeta_fd = open(block_meta_info.blockmeta_dst_path().c_str(), O_RDONLY);
    if (-1 == blockmeta_fd) 
    {
        result->return_code_ = RET_BLOCK_OR_META_FILE_OPEN_ERROR;
        LOGV(LL_ERROR,"open block_metafile error,blockmeta_fd:%d \
                when ReadBlockAndMetaFile,errmsg:%s", 
                blockmeta_fd, strerror(errno));
        int32_t close_block = close(block_fd);
        if (-1 == close_block)
        {
             LOGV(LL_ERROR,"close block:%ld failed! block_fd:%d errmsg:%s",
                    block.block_id(), block_fd, strerror(errno));
        }
        return result;
    }

    //return : fill the result
    result->block_fd_ = block_fd;
    result->block_length_ = block_meta_info.num_bytes();
    result->meta_fd_ = blockmeta_fd;
    result->return_code_ = RET_SUCCESS;
    LOGV(LL_INFO,"ReadBlockAndMetaFile succ! block_id:%ld,block_length:%ld, \
        ret:%d,block_fd:%d,meta_fd:%d",
        block.block_id(), 
        result->block_length_, 
        result->return_code_, 
        block_fd, 
        blockmeta_fd);

    return result;
}

//XXX NOTE: remember to free the memory
WriteBlockFd * FSDataSet::WriteBlockAndMetaFd(const  Block & block)
{
    WriteBlockFd * fd = new WriteBlockFd();
    assert(fd);

    if (0 >= block.block_version()) 
    {
        LOGV(LL_ERROR, "block version invalid:%d", block.block_version());
        fd->return_code_ = RET_PIPELINE_VERSION_NOT_INVALID;
        return fd;
    }
    // judge : find block in the map
    hash_map<int64_t, BlockMetaInfo>::iterator it;
    rw_lock_.rlock()->lock();
    it = block_map_.find(block.block_id());
    if (it != block_map_.end()) 
    {
        BlockMetaInfo block_meta_info(it->second);
        rw_lock_.rlock()->unlock();
        LOGV(LL_WARN,"block:%ld already exist in block_map when WriteBlockAndMetaFd",
                block.block_id());
        if (block.block_version() <= block_meta_info.block_version()) 
        {
            fd->return_code_ = RET_BLOCK_FILE_EXISTS;
            LOGV(LL_WARN, "block_version:%d(map) <= %d when WriteBlockAndMetaFd, \
                    block_id:%ld",
                    block_meta_info.block_version(), 
                    block.block_version(), 
                    block.block_id());
            return fd;
        }
        else 
        {
            LOGV(LL_ERROR, "block_version:%d(map) > %d when WriteBlockAndMetaFd, \
                    block_id:%ld,delete block!",
                    block_meta_info.block_version(), 
                    block.block_version(), 
                    block.block_id());
            RemoveBlock(block.block_id());
        }
    }
    else 
    {
        rw_lock_.rlock()->unlock();
    }
    
    // add a new block 
    string dir = data_dir_->AddBlock();
    LOGV(LL_INFO,"AddBlock:%d return:%s when WriteBlockAndMetaFd",
            block.block_id(),dir.c_str());
    if ("" == dir) 
    {
        fd->return_code_ = RET_BLOCK_FILE_CREATE_ERROR;
        LOGV(LL_ERROR,"AddBlock return Path Error:%s when \
                WriteBlockAndMetaFd.block_id:%ld",
                dir.c_str(), block.block_id());
        return fd;
    }

    BlockMetaInfo block_meta_info(block.block_id(), 0, block.block_version(), 
                                    block.file_id(), dir.c_str());
    rw_lock_.wlock()->lock();
    block_map_[block.block_id()] = block_meta_info;
    rw_lock_.wlock()->unlock();

    fd->block_fd_ = open(block_meta_info.block_dst_path().c_str(),
                         O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (-1 == (fd->block_fd_))
    {
        fd->return_code_ = RET_BLOCK_FILE_CREATE_ERROR;
        LOGV(LL_ERROR,"create block file: %s(fd:%d) error!, \
                block_id:%ld,errmsg=:%s",
                block_meta_info.block_dst_path().c_str(),
                fd->block_fd_,
                block.block_id(),
                strerror(errno));
        return fd;
    } 
    fd->meta_fd_  = open(block_meta_info.blockmeta_dst_path().c_str(),
                         O_RDWR | O_CREAT | O_TRUNC, 0666);      
    if (-1 == fd->meta_fd_) 
    {
        fd->return_code_ = RET_BLOCK_FILE_CREATE_ERROR;
        LOGV(LL_ERROR,"create block_meta file: %s(fd:%d) error! \
                block_id:%ld,errmsg=:%s",
                block_meta_info.blockmeta_dst_path().c_str(),
                fd->meta_fd_,
                block.block_id(),
                strerror(errno));
        return fd;
    }    

    fd->return_code_ = RET_SUCCESS; 
    LOGV(LL_DEBUG, "WriteBlockAndMetaFd succ! block_id:%ld,block_fd:%d,\
            meta_fd:%d,ret:%d", 
            block.block_id(), fd->block_fd_, fd->meta_fd_, fd->return_code_);
    return fd;
} 

void FSDataSet::ConstructPath(string &block_path, string &meta_path, const Block &block)
{
    block_path = tmp_dir_ + kBlockFilePrefix+ Int64ToString(block.block_id());
    meta_path = block_path + kDelimiter + Int32ToString(block.block_version())
                + kDelimiter + Int64ToString(block.file_id()) + kMetaFilePostfix;
}

WriteBlockFd * FSDataSet::PrepareReplicate(const Block & block)
{ 
    WriteBlockFd * fd = new WriteBlockFd();
    assert(fd);

    if (0 >= block.block_version()) 
    {
        LOGV(LL_ERROR,"block version invalid:%d when PrepareReplicate block:%ld",
                block.block_version(), block.block_id());
        fd->return_code_ = RET_PIPELINE_VERSION_NOT_INVALID;
        return fd;
    }

    // judge : check block in block_map
    hash_map<int64_t, BlockMetaInfo>::iterator it;
    rw_lock_.rlock()->lock();
    it = block_map_.find(block.block_id());
    if (it != block_map_.end())
    {
        BlockMetaInfo block_meta_info(it->second);
        rw_lock_.rlock()->unlock();
        LOGV(LL_INFO,"block:%ld already exist in block_map when PrepareReplicate",
                block.block_id());
        if (block.block_version() <= block_meta_info.block_version()) 
        {
            fd->return_code_ = RET_BLOCK_FILE_EXISTS;
            LOGV(LL_WARN, "block_version:%d(map) <= %d when PrepareReplicate block:%ld",
                 block_meta_info.block_version(), block.block_version(), block.block_id());
            return fd;
        }
        else 
        {
            LOGV(LL_ERROR, "block_version:%d(map) > %d when PrepareReplicate \
                    block:%d,delete block!",
                    block_meta_info.block_version(), 
                    block.block_version(), 
                    block.block_id());
            RemoveBlock(block.block_id());
        }
    }
    else 
    {
        rw_lock_.rlock()->unlock();
    }
        
    
    //open tmp file
    string replicate_block;
    string meta_string; 
    LOGV(LL_INFO,"start ConstructPath,block_id:%ld,block_version:%d, \
            num_bytes:%ld,file_id:%ld",
            block.block_id(),
            block.block_version(),
            block.num_bytes(),
            block.file_id());
    ConstructPath(replicate_block, meta_string, block);
    LOGV(LL_INFO,"end ConstructPath,block:%s, metafile:%s, block_id:%ld",
            replicate_block.c_str(),meta_string.c_str(),block.block_id());
    fd->block_fd_ = open(replicate_block.c_str(), O_RDWR | O_CREAT | O_TRUNC ,0666);
    if (-1 == (fd->block_fd_)) 
    {
        fd->return_code_ = RET_BLOCK_FILE_CREATE_ERROR;
        LOGV(LL_ERROR,"create block:%s error when PrepareReplicate,errmsg:%s",
                replicate_block.c_str(),strerror(errno));
        return fd;
    }
    fd->meta_fd_ = open(meta_string.c_str(),O_RDWR | O_CREAT | O_TRUNC,0666);
    if (-1 == fd->meta_fd_) 
    {
        fd->return_code_ = RET_BLOCK_FILE_CREATE_ERROR;
        LOGV(LL_ERROR,"create metafile:%s error when PrepareReplicate,errmsg:%s",
                meta_string.c_str(),strerror(errno));
        return fd;
    }
             
    fd->return_code_ = RET_SUCCESS;
    LOGV(LL_DEBUG, "PrepareReplicate succ! block_id:%ld,block_fd:%d,meta_fd:%d,ret:%d",
         block.block_id(), fd->block_fd_, fd->meta_fd_, fd->return_code_);
    return fd;
}

void FSDataSet::RemoveBlocks(set<int64_t> &blockid)
{
    set<int64_t>::const_iterator begin = blockid.begin();
    bool ret;
    while (begin != blockid.end()) 
    {
        int64_t id = *begin;
        ret = RemoveBlock(id);
        if (!ret)
            LOGV(LL_ERROR,"remove blk_%ld failed!  --FSDataSet",id);
        ++begin;
    }
}   

bool FSDataSet::RemoveBlock(int64_t id)
{     
    //check the block in the map
    hash_map<int64_t, BlockMetaInfo>::const_iterator it;
    rw_lock_.rlock()->lock();
    it = block_map_.find(id);
    if (it == block_map_.end())
    {
        rw_lock_.rlock()->unlock();
        LOGV(LL_ERROR, "blk_%ld not in block_map when RemoveBlock!  --FSDataSet",id);
        return false;
    }
    //construct block_meta_info
    BlockMetaInfo block_meta_info(it->second);
    rw_lock_.rlock()->unlock(); 

    //remove block_meta_info in the map
    rw_lock_.wlock()->lock();
    block_map_.erase(id);
    rw_lock_.wlock()->unlock();

    //remove block file and meta file
    bool result = true;
    if (0 != remove(block_meta_info.blockmeta_dst_path().c_str()))
    {
        result = false;
        LOGV(LL_ERROR,"remove meta file : %s failed! errmsg:%s --FSDataSet",
                block_meta_info.blockmeta_dst_path().c_str(),strerror(errno));
    }
    if (0 != remove(block_meta_info.block_dst_path().c_str()))
    {
        result = false;
        LOGV(LL_ERROR,"remove block file : %s failed! errmsg:%s  --FSDataSet",
                block_meta_info.block_dst_path().c_str(),strerror(errno));
    }

    //Delblock:num_blocks -1
    data_dir_->DelBlock(block_meta_info.block_folderpath().c_str());

    LOGV(LL_INFO,"remove block:%ld succeed!", block_meta_info.block_id());
    return result;
}

//XXX NOTE: remember to free the set
void FSDataSet::GetBlockReport(set<BlockInfo *> & blocks_info)
{
    //clear map
    rw_lock_.wlock()->lock();
    block_map_.clear();  
    data_dir_->GetBlockMap(block_map_);
    rw_lock_.wlock()->unlock();

    //fill map
    hash_map<int64_t,BlockMetaInfo>::const_iterator it;
    rw_lock_.rlock()->lock();
    it = block_map_.begin();
    while (it != block_map_.end())
    {
        BlockInfo * block_info_temp = new BlockInfo();
        block_info_temp->set_block_id((it->second).block_id());
        block_info_temp->set_block_version((it->second).block_version());
        block_info_temp->set_file_id((it->second).file_id());
        blocks_info.insert(block_info_temp);
        ++it;
    }
    rw_lock_.rlock()->unlock();
    LOGV(LL_DEBUG, "GetBlockReport succ! blocks_info size:%d", 
            blocks_info.size());
}

int64_t FSDataSet::GetBlockLength(const Block & block)
{
    // check block in block_map
    hash_map<int64_t, BlockMetaInfo>::iterator it;
    rw_lock_.rlock()->lock();
    it = block_map_.find(block.block_id());
    if (it == block_map_.end()) 
    {
        rw_lock_.rlock()->unlock();
        LOGV(LL_ERROR,"block:%ld not in block_map when GetBlockLength", 
                block.block_id());
        return -1;
    }
    BlockMetaInfo block_meta_info(it->second);
    rw_lock_.rlock()->unlock();

    // check block_version
    if (block_meta_info.block_version() != block.block_version()) 
    {
        LOGV(LL_ERROR,"version:%d(map) != %d(given) when GetBlockLength,block_id:%d", 
                block_meta_info.block_version(), 
                block.block_version(),
                block.block_id());
        return -1;
    }
    
    // stat block_dst_path and get num_bytes
    struct stat st_block;
    if (0 != stat(block_meta_info.block_dst_path().c_str(),&st_block)) 
    {
        LOGV(LL_ERROR,"Can't stat Block:%ld when GetBlockLength!  --FSDataSet",
                block_meta_info.block_id());
        return -1;
    }
    block_meta_info.set_num_bytes(st_block.st_size);
    LOGV(LL_INFO,"GetBlockLength succ! block_length:%ld,block_id:%ld",
            block_meta_info.num_bytes(),
            block_meta_info.block_id());
    return block_meta_info.num_bytes();
}

bool FSDataSet::CloseFile(const Block & block, int32_t block_fd, 
                            int32_t meta_fd, int8_t mode)
{
    bool result = true;
    LOGV(LL_DEBUG, "before CloseFile ! block_id:%ld block_fd:%d meta_fd:%d mode:%d", 
            block.block_id(), block_fd, meta_fd, mode);
    int32_t close_block = close(block_fd);
    LOGV(LL_DEBUG, "afer Close block_fd ! block_id:%ld block_fd:%d meta_fd:%d mode:%d", 
            block.block_id(), block_fd, meta_fd, mode);
    int32_t close_meta  = close(meta_fd);
    LOGV(LL_DEBUG, "after Close meta_fd ! block_id:%ld block_fd:%d meta_fd:%d mode:%d", 
            block.block_id(), block_fd, meta_fd, mode);
    if (-1 == close_block || -1 == close_meta) 
    {
        LOGV(LL_ERROR,"close block: %d or meta fd: %d failed! block_id:%ld errmsg:%s",
                block_fd, meta_fd, block.block_id(), strerror(errno));
        return false;
    }

    switch (mode) 
    {
        case kReadMode:
            break;
        case kReplicateCompleteMode:
        {
            result = ReplicateComplete(block, block_fd, meta_fd);
            break;
        }
        case kReplicateMode:
        {
            result = ReplicateUncomplete(block, block_fd, meta_fd);
            break;
        }
        case kWriteMode:
        {
            result = CloseUncomplete(block,block_fd,meta_fd);
            break;
        }
        case kWriteCompleteMode:
        case kSafeWriteMode:
        // case kReadAfterWriteCompleteMode:
        {
            result = CloseComplete(block,block_fd,meta_fd);
            break;
        }
        default: 
        {
            //XXX invalid mode
            assert(0);
        }
    }

    LOGV(LL_DEBUG, "CloseFile succ! block_id:%ld block_fd:%d meta_fd:%d mode:%d",
             block.block_id(), block_fd, meta_fd, mode);
    return result;
}  

bool FSDataSet::CloseUncomplete(const Block & block,
                    int32_t block_fd,int32_t meta_fd)
{
    LOGV(LL_DEBUG,"CloseUncomplete succ! block_id:%d,block_fd:%d,meta_fd:%d",
            block.block_id(),block_fd,meta_fd);
    return RemoveBlock(block.block_id());
}   
//TODO:it->second.set_numbytes();
bool FSDataSet::CloseComplete(const Block & block,
                    int32_t block_fd,int32_t meta_fd)
{
    // check block in block_map
    hash_map<int64_t, BlockMetaInfo>::iterator it;
    rw_lock_.rlock()->lock();
    LOGV(LL_INFO,"before find Block:%ld in block_map when CloseComplete!  \
            --FSDataSet",block.block_id());
    it = block_map_.find(block.block_id());
    LOGV(LL_INFO,"after find Block:%ld in block_map when CloseComplete!  \
            --FSDataSet",block.block_id());
    if (it == block_map_.end()) 
    {
        rw_lock_.rlock()->unlock();
        LOGV(LL_ERROR,"Can't find Block:%ld in block_map when CloseComplete! \
                --FSDataSet",block.block_id());
        return false;
    }
    // fill block_meta_info
    BlockMetaInfo block_meta_info(it->second.block_id(),0,it->second.block_version(),
                        it->second.file_id(),it->second.block_folderpath().c_str());
    rw_lock_.rlock()->unlock();

    // stat block_dst_path and get num_bytes
    struct stat st_block;
    if (0 != stat(block_meta_info.block_dst_path().c_str(),&st_block)) 
    {
        LOGV(LL_ERROR,"Can't stat Block:%ld when CloseComplete!  --FSDataSet",
                block.block_id());
        return false;
    }
    block_meta_info.set_num_bytes(st_block.st_size);

    // update block_map
    rw_lock_.wlock()->lock();
    block_map_[block_meta_info.block_id()] = block_meta_info;
    rw_lock_.wlock()->unlock();

    LOGV(LL_INFO,"CloseComplete succ! block_id:%ld,block_version:%d,\
            num_bytes:%d,file_id:%d",
            block_meta_info.block_id(),
            block_meta_info.block_version(),
            block_meta_info.num_bytes(),
            block_meta_info.file_id());
    return true;
}


bool FSDataSet::ReplicateComplete(const Block & block,
                        int32_t block_fd,int32_t meta_fd)
{
    // add block
    string dir = data_dir_->AddBlock();
    if ("" == dir)
    {
        LOGV(LL_ERROR,"add block return empty str error when ReplicateComplete");
        return false;
    }
    LOGV(LL_INFO,"add block return:%s in ReplicateComplete",dir.c_str());

    // fill the block map
    string replicate_block;
    string meta_string;
    ConstructPath(replicate_block, meta_string, block);

    struct stat st_block;
    if (0 != stat(replicate_block.c_str(), &st_block))
    {
        LOGV(LL_ERROR,"Can't stat Block:%ld when complete replication! \
                --FSDataSet",block.block_id());
        return false;
    } 
    BlockMetaInfo block_meta_info(block.block_id(), st_block.st_size , 
                                  block.block_version(), block.file_id(), 
                                  dir.c_str());
    rw_lock_.wlock()->lock();
    block_map_[block.block_id()] = block_meta_info;
    rw_lock_.wlock()->unlock();

    // move replicate block and meta file from /tmp to /data
    string dest_block = block_meta_info.block_dst_path();
    string dest_meta  = block_meta_info.blockmeta_dst_path();
    bool result = true;
    if (-1 == rename(replicate_block.c_str(), dest_block.c_str()))
    {
        result = false;
        int64_t blockid = block.block_id();
        rw_lock_.wlock()->lock();
        block_map_.erase(blockid);
        rw_lock_.wlock()->unlock();
        LOGV(LL_ERROR, "rename block:%s to block:%s \
                failed when ReplicateComplete",
                replicate_block.c_str(), dest_block.c_str());
    }
    if (-1 == rename(meta_string.c_str(), dest_meta.c_str()))
    {
        result = false;
        int64_t blockid = block.block_id();
        rw_lock_.wlock()->lock();
        block_map_.erase(blockid);
        rw_lock_.wlock()->unlock();
        LOGV(LL_ERROR, "rename block_metafile:%s to block_metafile:%s \
                failed when ReplicateComplete",
                meta_string.c_str(), dest_meta.c_str());
    }
    LOGV(LL_INFO,"ReplicateComplete succ! block_id:%ld,block_version:%d,\
            num_bytes:%d,file_id:%d",
            block_meta_info.block_id(),
            block_meta_info.block_version(),
            block_meta_info.num_bytes(),
            block_meta_info.file_id());
    return result;
}

bool FSDataSet::ReplicateUncomplete(const Block & block,int32_t block_fd,int32_t meta_fd)
{
    string replicate_block;
    string meta_string; 
    ConstructPath(replicate_block, meta_string, block);
    remove(replicate_block.c_str());
    remove(meta_string.c_str());
    LOGV(LL_DEBUG, "ReplicateUncomplete succ! block_id:%ld,block_fd:%d,meta_fd:%d",
            block.block_id(),block_fd,meta_fd);
    return true;
}

int64_t FSDataSet::BlockIdToFileId(int64_t block_id)
{
    hash_map<int64_t, BlockMetaInfo>::iterator it;
    int64_t file_id = 0;
    rw_lock_.rlock()->lock();
    it = block_map_.find(block_id);
    if (it == block_map_.end())
    {
        rw_lock_.rlock()->unlock();
        LOGV(LL_ERROR,"get file_id:%ld failed!",block_id);
        return -1;
    }
    file_id = it->second.file_id();
    rw_lock_.rlock()->unlock();
    return file_id;
}

int64_t FSDataSet::GetBlockNum()
{
    rw_lock_.rlock()->lock();
    int64_t block_num = block_map_.size();
    rw_lock_.rlock()->unlock();
    return block_num;
}

}//end of namesapce dataserver
}//end of namespace bladestore
