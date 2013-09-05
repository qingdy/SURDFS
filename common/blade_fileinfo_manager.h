/*
 *version  :  1.0
 *author   :  chen yunyun
 *date     :  2012-5-5
 *fun      :  used by HA
 */
#ifndef  BLADESTORE_COMMON_FILEINFO_MANAGER_H 
#define  BLADESTORE_COMMON_FILEINFO_MANAGER_H 
#include <sys/types.h>
#include <dirent.h>
#include <sys/vfs.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include <new>
#include <algorithm>

#include "blade_common_define.h"

namespace bladestore
{
namespace common
{
class IFileInfo
{
public:
	virtual ~IFileInfo() {}

public:
	virtual int GetFD() const = 0;
};

class IFileInfoMgr
{
public:
	virtual ~IFileInfoMgr() 
	{

	}

public:
	virtual const IFileInfo *GetFileInfo(const uint64_t key_id) = 0;
	virtual int RevertFileInfo(const IFileInfo *file_info) = 0;
	virtual int EraseFileInfo(const uint64_t key_id)
	{
		UNUSED(key_id);
		return BLADE_ERROR;
	}
};

}//end of namespace common
}//end of namespace bladestore

#endif


