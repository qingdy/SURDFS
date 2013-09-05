/*
 *version  :  1.0
 *author   :  chen yunyun
 *date     :  2012-4-19
 *
 */
#ifndef BLADESTORE_NAMESERVER_BLOCK_H
#define BLADESTORE_NAMESERVER_BLOCK_H

#include <set>

namespace bladestore
{
namespace nameserver
{

//Shared起智能指针的作用
class BladeBlockCollect
{
public:
	BladeBlockCollect()
	{
		dataserver_set_.clear();	
		version_ = -1;
		block_id_ = -1;
		replicas_number_ = -1;
		dataserver_set_.clear();
	}

	~BladeBlockCollect()
	{

	}

    bool operator < (BladeBlockCollect & right)
    {
        return (block_id_ < right.block_id_);
    }
    bool operator < (BladeBlockCollect * right)
    {
        return (block_id_ < right->block_id_);
    }

	std::set<uint64_t> dataserver_set_;	
	int32_t version_;
	int64_t block_id_;
	int16_t replicas_number_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
