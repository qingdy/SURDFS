/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-4-18
 *
 */
#ifndef BLADESTORE_BTREE_BASE_HANDLE_H
#define BLADESTORE_BTREE_BASE_HANDLE_H

#include <stdint.h>

namespace bladestore
{
namespace btree
{
/**
 * 用于读的时候用
 */
class BtreeRootPointer;

class BtreeBaseHandle
{
public:
	friend class BtreeBase;

public:
	/**
	 * 构造
	 */
	BtreeBaseHandle();
	
	/**
	 * 析构
	 */
	virtual ~BtreeBaseHandle();

	/**
	 * root_pointer is null
	 */
	bool is_null_pointer();

	/**
	 * 清理,把引用计数减1
	 */
	void Clear();

protected:
	// root指针
	BtreeRootPointer *root_pointer_;
	volatile int64_t *ref_count_;
};

/**
 * 用于回调用
 */
class BtreeCallback
{
public:
	virtual ~BtreeCallback();
	virtual int callback_done(void *data) = 0;
};

} // end namespace btree
} // end namespace bladestore

#endif
