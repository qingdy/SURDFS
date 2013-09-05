#include <ctime>
#include <stdlib.h>
#include <stdio.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <set>
#include "errno.h"
#include "assert.h"
#include "blade_common_define.h"
#include "fsdir.h"
#include "int_to_string.h"
#include "iostream"
#include "log.h"
#include "helper.h"

using namespace std;
using namespace bladestore::common;
using namespace pandora;

namespace bladestore
{
namespace dataserver
{

FSDir::FSDir(const string & dir) : dir_(dir)
{
    // children_ = NULL;
    for (int32_t i = 0; i < BLADE_MAX_DIR_NUM; i++)
    {
        num_blocks_[i] = 0;
    }
    pthread_mutex_init(&num_blocks_mutex_, NULL);
    Init();
}

FSDir::~FSDir()
{
}

//rebuild all the FSDir objects
bool FSDir::Init()
{
    //if dir not end with /,add /
    if ('/' != dir_[dir_.size() - 1])
        dir_ += "/";

    //if dir not exist, create it;
    //if have no access rights, will return false 
    assert(CHelper::DirCheckAndCreate(dir_.c_str()) == 0);

    //access dir
    if (0 != access(dir_.c_str(),F_OK))
    {
        LOGV(LL_FATAL, "can't access dir:%s when FSDir Init, errmsg=%s", 
                dir_.c_str(), strerror(errno));
        return false;
    }

    //check subdir,if not exist create it,if exist,get num_blocks
    struct dirent * ent;
    struct stat st;
    string subdir_path;
    string file;
    DIR * subdir_fd;
    for (int32_t i = 0; i < BLADE_MAX_DIR_NUM; ++i)
    {
        //construct subdir_path
        subdir_path = dir_ + kSubDirName + Int32ToString(i) + "/";
        assert(CHelper::DirCheckAndCreate(subdir_path.c_str()) == 0);
        
        //open subdir
        if (NULL == (subdir_fd = opendir(subdir_path.c_str()))) 
        {
            LOGV(LL_FATAL, "can't open dir:%s, errmsg=%s", dir_.c_str(), 
                    strerror(errno));
            continue;
        }

        //get numblocks for every subdir
        while (NULL != (ent = readdir(subdir_fd)))
        {
            file = subdir_path + string(ent->d_name);
            if (0 != stat(file.c_str(), &st))
                continue;
            if (true == S_ISREG(st.st_mode))
            {
                if (true == CHelper::IsBlockFile(ent->d_name))
                {
                    ++num_blocks_[i];
                    LOGV(LL_INFO,"get block file:%s in subdir:%s when Init FSDir",
                            ent->d_name,subdir_path.c_str());
                }
            }
        }
        //close dir
        closedir(subdir_fd);
    }
    return true;
} 

// the max depth level of directory support is 2
// XXX Optimize: can use CAS replace mutex lock
string FSDir::AddBlock()
{
    string subdir_path;
    for (int32_t i = 0; i < BLADE_MAX_DIR_NUM; ++i)
    {
        subdir_path = dir_ + kSubDirName + Int32ToString(i) + "/"; 
        pthread_mutex_lock(&num_blocks_mutex_);
        if (num_blocks_[i] < BLADE_MAX_BLOCKS_PER_DIR)
        {
            ++num_blocks_[i];
            pthread_mutex_unlock(&num_blocks_mutex_);
            return subdir_path;
        }
        pthread_mutex_unlock(&num_blocks_mutex_);
    }
    assert(0);
}

void FSDir::DelBlock(const char* block_folderpath)
{
    //check path
    if (NULL == strstr(block_folderpath,dir_.c_str()))
    {
        LOGV(LL_ERROR, "block_folderpath:%s Error when DelBlock",
                block_folderpath);
        return;
    }
   
    //get subdir index vector
    vector<int32_t> index;
    CHelper::GetSubdirIndexVector(dir_.c_str(), block_folderpath, index);

    //path must be /xxx/sundirxx/
    if (1 != index.size())
    {
        LOGV(LL_ERROR,"can't get subdir index when DelBlock,block_folderpath:%s",
                block_folderpath);
        return;
    }

    //num_blocks_ --
    pthread_mutex_lock(&num_blocks_mutex_);
    assert(num_blocks_[index[0]] > 0);    
    --num_blocks_[index[0]];
    pthread_mutex_unlock(&num_blocks_mutex_);
    LOGV(LL_INFO,"DelBlock succ!:%s",block_folderpath);
} 

void FSDir::GetBlockMap(hash_map<int64_t, BlockMetaInfo> & block_map)
{
    //open dir
    if (0 != access(dir_.c_str(), F_OK))
    {
        LOGV(LL_FATAL,"access dir error:%s,when GetBlockMap,errmsg:%s",
                dir_.c_str(),strerror(errno));
        return;
    }

    //get block id info of this dir
    hash_map<int64_t, BlockMetaInfo>::iterator block_map_iter;
    BlockMetaInfo block_meta_info;
    set<string> block_delete_set;
    set<string>::iterator block_delete_set_it;
    string subdir_path;
    string dst_filepath;
    struct dirent *ent;
    struct stat st_block;
    struct stat st_file;

    DIR *subdir_fd;
    for (int32_t i=0; i < BLADE_MAX_DIR_NUM; i++)
    {
        subdir_path = dir_ + kSubDirName + Int32ToString(i) + "/";
        block_meta_info.set_block_folderpath(subdir_path);
        //check subdir exist or not,if not exist create it
        if (0 != access(subdir_path.c_str(),F_OK))
        {
            LOGV(LL_ERROR,"subdir%d not exist,create it return:%d",
                    i,CHelper::DirCheckAndCreate(subdir_path.c_str()));
            continue;
        }

        //open subdir
        if (NULL == (subdir_fd = opendir(subdir_path.c_str())))
        {
            LOGV(LL_FATAL,"open subdir error:%s when get block map,errmsg:%s",
                    subdir_path.c_str(),strerror(errno));
            continue;
        }

        //iterate subdir and get block map
        while (NULL != (ent = readdir(subdir_fd)))
        {
            // pass . and ..
            if (0 == strcmp(ent->d_name,".") || 0 == strcmp(ent->d_name,"..")) 
                continue;
            // stat file
            dst_filepath = subdir_path + string(ent->d_name);
            if (0 != stat(dst_filepath.c_str(),&st_file)) continue ;

            // file
            if (true == S_ISREG(st_file.st_mode))
            {
                // block file
                if (0 == CHelper::IsBlockFile(ent->d_name))
                {
                    block_delete_set_it = block_delete_set.find(dst_filepath);
                    if (block_delete_set_it == block_delete_set.end())
                    {
                        block_delete_set.insert(dst_filepath);
                    }
                    else
                    {
                        block_delete_set.erase(dst_filepath);
                    }
                    continue;
                }
                // not metafile
                if (0 != CHelper::IsMetaFile(ent->d_name))
                {
                    LOGV(LL_WARN, "detect illegal file:%s when GetBlockMap", 
                            dst_filepath.c_str());
                    remove(dst_filepath.c_str());
                    continue;
                }
                // construct class block_meta_info from filename and get numbytes
                CHelper::GetBlockMetaInfoFromMetaPath(ent->d_name,block_meta_info);

                // check block file exist and get num_bytes
                if (0 != stat(block_meta_info.block_dst_path().c_str(),&st_block)) 
                {
                    LOGV(LL_ERROR,"block file:%s not exist,but meta file exist:%s, \
                            remove metafile when GetBlockMap",
                            block_meta_info.block_dst_path().c_str(),
                            dst_filepath.c_str());
                    remove(block_meta_info.blockmeta_dst_path().c_str());
                    continue;
                }
                block_meta_info.set_num_bytes(st_block.st_size);
                
                // delete block file in block_delete_set
                LOGV(LL_DEBUG, "delete file:%s in block_delete_set", 
                        dst_filepath.c_str());
                block_delete_set_it = block_delete_set.find(block_meta_info.block_dst_path());
                if (block_delete_set_it == block_delete_set.end())
                {
                    block_delete_set.insert(block_meta_info.block_dst_path());
                }
                else
                {
                    block_delete_set.erase(block_meta_info.block_dst_path());
                }
                
                // insert block_meta_info into block_map
                block_map_iter = block_map.find(block_meta_info.block_id());
                if (block_map_iter != block_map.end())
                {
                    // block version < version in the map,delete this block
                    if (block_meta_info.block_version() \
                        < (block_map_iter->second.block_version()))
                    {
                        remove(block_meta_info.block_dst_path().c_str());
                        remove(block_meta_info.blockmeta_dst_path().c_str());
                        LOGV(LL_ERROR,"duplicate block_id:%ld, \
                                block_version:%d(map) < %d \
                                delete old version when GetBlockMap",
                                block_meta_info.block_id(),
                                block_map_iter->second.block_version(), 
                                block_meta_info.block_version());
                    }
                    // block version = version in the map 
                    // and the metafile path is different ,delete this block
                    else if (block_meta_info.block_version() \
                                == block_map_iter->second.block_version() \
                                && block_meta_info.blockmeta_dst_path() \
                                != block_map_iter->second.blockmeta_dst_path())
                    {
                        remove(block_meta_info.block_dst_path().c_str());
                        remove(block_meta_info.blockmeta_dst_path().c_str());
                        LOGV(LL_ERROR,"duplicate block id:%ld,\
                                block_version:%d \
                                (map) = %d \
                                but different path:%s(map) != %s, \
                                delete block and metafile in local",  
                                block_meta_info.block_id(),
                                block_map_iter->second.block_version(),
                                block_meta_info.block_version(),
                                block_map_iter->second.blockmeta_dst_path().c_str(),
                                block_meta_info.blockmeta_dst_path().c_str());
                    }
                    // block version > version in the map,
                    //delete the block in the map
                    else if( block_meta_info.block_version() \
                                > block_map_iter->second.block_version())
                    {
                        remove(block_map_iter->second.block_dst_path().c_str());
                        remove(block_map_iter->second.blockmeta_dst_path().c_str());
                        LOGV(LL_ERROR,"duplicate block id:%ld,delete old version:%d \
                                block and metafile in the map",
                                block_meta_info.block_id(),
                                block_meta_info.block_version());
                        block_map[block_meta_info.block_id()] = block_meta_info;
                        LOGV(LL_INFO,"insert block:%ld_%d_%ld_%ld %s into block_map",
                                block_meta_info.block_id(),
                                block_meta_info.block_version(),
                                block_meta_info.num_bytes(),
                                block_meta_info.file_id(),
                                block_meta_info.block_folderpath().c_str());
                    }
                }
                else
                {
                    block_map[block_meta_info.block_id()] = block_meta_info;
                    LOGV(LL_INFO,"insert block:%d_%d_%d_%d %s into block_map",
                            block_meta_info.block_id(),
                            block_meta_info.block_version(),
                            block_meta_info.num_bytes(),
                            block_meta_info.file_id(),
                            block_meta_info.block_folderpath().c_str());
                }
                bzero(&st_file,sizeof(struct stat));
                bzero(&st_block,sizeof(struct stat));
            }
        }
        
        //close subdir
        closedir(subdir_fd);
    }
    // check block_delete_set
    for (block_delete_set_it = block_delete_set.begin(); 
                block_delete_set_it != block_delete_set.end(); 
                block_delete_set_it++)
    {
        remove(block_delete_set_it->c_str());
        LOGV(LL_WARN, "delete illegal block file:%s \
                when GetBlockMap,errmsg=%s",
                block_delete_set_it->c_str(),strerror(errno));
    }
}

}//end of namesapce dataserver
}//end of namespace bladestore
