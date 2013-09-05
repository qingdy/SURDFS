#ifndef WRITEBLOCKFD_H
#define WRITEBLOCKFD_H

namespace bladestore
{
namespace dataserver
{
class WriteBlockFd
{
public:
    int32_t block_fd_;
	int32_t meta_fd_;
	int32_t return_code_;
};
}//end of dataserver
}//end of bladestore

#endif
