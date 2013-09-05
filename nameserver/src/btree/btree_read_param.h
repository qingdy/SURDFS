/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-8
 *
 */

#ifndef BLADESTORE_BTREE_READ_PARAM_H
#define BLADESTORE_BTREE_READ_PARAM_H

#include "btree_define.h"

namespace bladestore
{
namespace btree
{
/**
 * BTree搜索过程的辅助结构
 */
class BtreeNode;

class BtreeReadParam
{
	friend class BtreeBase;
public:
	/**
	 * 构造
	 */
	BtreeReadParam();

	/**
	 * 析构
	 */
	virtual ~BtreeReadParam();

#ifdef BLADE_BASE_BTREE_DEBUG
	/**
	 * 打印出来.
	 */
	void Print();
#endif

protected:
	// 找到的key是否相等
	int64_t found_;
	// 当前节点使用
	BtreeNode *node_[CONST_MAX_DEPTH];
	// 节点的个数
	int32_t node_length_;
	// 当前节点位置
	int16_t node_pos_[CONST_MAX_DEPTH];
};

}//end of namespace btree
}//end of namespace bladestore

#endif

