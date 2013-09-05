#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include "helper.h"
#include "log.h"
#include "blade_common_define.h"
#include "file_utils.h"
#include "block_meta_info.h"


using namespace bladestore::common;
namespace bladestore
{
namespace dataserver
{
    
#define MAX_PATH_LEN 1024

//   check subdir name is subdirXX or not
// @return 0 is DirName
int32_t CHelper::IsDirName(const char *path)
{
	if (0 != strncmp(path, kSubDirName, strlen(kSubDirName)))
		return -1;

	char * cur;
	cur = (char *)(path + strlen(kSubDirName));
	/* ensure the following characters after 'subdir' all digit-numbers */
	for (int i = 0; *(cur + i) != '\0'; ++i) 
	{
		if ('9' < *(cur + i) || '0' > *(cur + i))
			return -1;
	}
	int n = atoi(cur);
	if (0 <= n && BLADE_MAX_DIR_NUM > n)
		return 0;
	return -1;
} 

/**
  construct BlockMetaInfo from metafile path
  */
void CHelper::GetBlockMetaInfoFromMetaPath(const char *metafilename, 
                                    BlockMetaInfo &block_meta_info)
{
	if (0 <= IsMetaFile(metafilename))
    {
        LOGV(LL_ERROR,"GetBlockMetaInfoFromMetaPath:%s Error in CHelper",
                metafilename);
        return;
    }
	char temp[MAX_PATH_LEN];
	strncpy(temp,metafilename,MAX_PATH_LEN);
    char *buf = temp;
    char *outer_ptr = NULL;  
	char *pch;
	int32_t i = 1;
	while ((pch = strtok_r(buf, "_", &outer_ptr)) != NULL && i < 5)
	{
		switch (i)
		{
			case 2: block_meta_info.set_block_id(atol(pch));break;
			case 3: block_meta_info.set_block_version(atoi(pch));break;
			case 4: block_meta_info.set_file_id(atol(pch));break;
		}
		++i; 
        buf = NULL;
	}
	return;
}

/**
  get subdir index,input /subdir1/,return 1
  */
int32_t CHelper::GetSubdirIndex(const char *dirname)
{
    if (0 != IsDirName(dirname))
    {
        LOGV(LL_ERROR,"GetSubdirIndex ERROR,subdir:%s illegle",dirname);
        return -1;
    }
	char *pos = strstr(dirname,kSubDirName);
	return atoi(pos+strlen(kSubDirName));
}

/**
  check filename is correct block file
  */
int32_t CHelper::IsBlockFile(const char *filename)
{
	if (0 != strncmp(filename, kBlockFilePrefix, sizeof(kBlockFilePrefix) - 1))
		return -1;
	char *cur = (char *)(filename + sizeof(kBlockFilePrefix) -1);
	//check if is 0-9
	for (int i = 0; *(cur + i) != '\0'; ++i) 
    {
		if ('9' < *(cur + i) || '0' > *(cur + i))
			return -1;
	}
	return 0;
}

/**
    check filename is correct metafile
*/
int32_t CHelper::IsMetaFile(const char *metafilename)
{
    if (0 != strncmp(metafilename, kBlockFilePrefix, strlen(kBlockFilePrefix)))
        return -1;
    char *pos  = strstr(metafilename,kMetaFilePostfix);
    if (NULL == pos)
        return -1;  
    return 0;
}

/**
    get index vector from block_folderpath
*/
void  CHelper::GetSubdirIndexVector(const char* dataset_path, 
            const char* block_folderpath, vector<int32_t> &index_vector)
{
    if (NULL == strstr(block_folderpath, dataset_path))
    {
        LOGV(LL_ERROR,"illegle block_folderpath:%s",block_folderpath);
        return;
    }
    char temp[MAX_PATH_LEN];
    strncpy(temp,block_folderpath,MAX_PATH_LEN);
    char *buf = temp;
    char *outer_ptr = NULL;  
    char *pch;
    while (NULL != (pch = strtok_r(buf, "/", &outer_ptr)))
    {
        if(0 == IsDirName(pch))
        {
            index_vector.push_back(GetSubdirIndex(pch));
        }
        buf = NULL;
    }
    return;
}

/*
    check dir,if exist do nothing,else create dir
*/
int32_t  CHelper::DirCheckAndCreate(const char* dir_path)
{
    if (0 != access(dir_path, F_OK)) 
    {
        if (ENOENT == errno) 
        {
            bool ret = mkdir(dir_path, 0777);
            return ret;
        } 
        else 
        {
            LOGV(LL_FATAL, "can't access %s, errmsg=%s", 
                            dir_path, strerror(errno));
            return -1;
        }
    }
    return 0;
}

}//end of dataserver
}//end of bladestore
