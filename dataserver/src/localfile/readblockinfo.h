#ifndef READBLOCKINFO_H
#define READBLOCKINFO_H

#include "blade_common_define.h"
using namespace bladestore::common;
namespace bladestore
{
namespace dataserver
{
class ReadBlockInfo
{
public:
	int32_t block_fd_;
    int32_t meta_fd_;
	int64_t block_length_;
	int32_t return_code_;
	bool mode_;//true is safe mode ;false is common mode
};
}//end of dataserver
}//end of bladestore

#endif
