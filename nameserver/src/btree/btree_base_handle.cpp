#include <stdio.h>
#include "btree_base_handle.h"
#include "btree_root_pointer.h"

namespace bladestore
{
namespace btree
{
/**
 * 构造
 */
BtreeBaseHandle::BtreeBaseHandle()
{
	root_pointer_ = NULL;
	ref_count_ = NULL;
}

/**
 * 析构
 */
BtreeBaseHandle::~BtreeBaseHandle()
{
	Clear();
}

/**
 * 清理,把引用计数减1
 */
void BtreeBaseHandle::Clear()
{
	if (ref_count_ != NULL)
	{
		int64_t old_value = 0;
		do
		{
			old_value = *ref_count_;
		}
		while (!BtreeRootPointer::Refcas(ref_count_, old_value, old_value - 1));
		ref_count_ = NULL;
	}
}

/**
 * root_pointer is null
 */
bool BtreeBaseHandle::is_null_pointer()
{
	return (NULL == root_pointer_);
}

/**
 * 构造
 */
BtreeCallback::~BtreeCallback()
{

}

}//end of namespace btree
}//end of namespace bladestore

