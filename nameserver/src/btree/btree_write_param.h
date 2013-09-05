/*
 *version  :  1.0
 *author   :  chen yunyun
 *date     :  2012-4-18
 *
 */
#ifndef BLADESTORE_BTREE_WRITE_PARAM_H
#define BLADESTORE_BTREE_WRITE_PARAM_H

#include "btree_define.h"

namespace bladestore
{
namespace btree
{
/**
 * BTree写过程的辅助结构
 */
class BtreeNode;
class BtreeReadParam;

class BtreeWriteParam : public BtreeReadParam
{
public:
	friend class BtreeBase;

public:
  	/**
   	 * 构造
   	 */
	BtreeWriteParam();

  	/**
   	 * 析构
   	 */
	~BtreeWriteParam();

private:
	// 当产生新root node时候用
	BtreeNode *new_root_node_;
	// 下一节点使用
	BtreeNode *next_node_[CONST_MAX_DEPTH];
	// tree_id
	int32_t tree_id;
};

}//end of namespace btree
}//end of namespace bladestore

#endif
