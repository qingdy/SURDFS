/*
 *version  : 1.0
 *authro   : chen yunyun
 *date     : 2012-4-18
 *
 */
#ifndef BLADESTORE_BTREE_ARRAY_LIST_H
#define BLADESTORE_BTREE_ARRAY_LIST_H

#include "btree_define.h"

namespace bladestore
{
namespace btree
{
/**
 * 用于list遍历
 */
class BtreeAlloc;
typedef struct BtreeNodeList
{
	int32_t pos;
	int32_t node_count;
	void **start;
	void **current;
	void **end;
} BtreeNodeList;

#ifdef BLADE_BTREE_CHECK
class BtreeBloomFilter
{
public:
	void Set(const void *node)
	{
		int a, b;
		a = mmhash(node, 3);
		b = a % 64;
		a /= 64;
		bits[a] |= (1ll << b);
		a = mmhash(node, 7);
		b = a % 64;
		a /= 64;
		bits[a] |= (1ll << b);
	}

	void Del(const void *node)
	{
		int a, b;
		a = mmhash(node, 3);
		b = a % 64;
		a /= 64;
		bits[a] &= ~(1ll << b);
		a = mmhash(node, 7);
		b = a % 64;
		a /= 64;
		bits[a] &= ~(1ll << b);
	}

	int Exist(const void *node)
	{
		int a, b;
		a = mmhash(node, 3);
		b = a % 64;
		a /= 64;
		if ((bits[a] & (1ll << b)) == 0) return 0;
		a = mmhash(node, 7);
		b = a % 64;
		a /= 64;
		if ((bits[a] & (1ll << b)) == 0) return 0;
		return 1;
	}

	void Clear()
	{
		memset(bits, 0, 10 * sizeof(uint64_t));
	}

private:
	int MmHash (const void *key, unsigned int seed)
	{
		const uint64_t m = 0xc6a4a7935bd1e995;
		const int r = 47;
		uint64_t h = seed ^ (8 * m);
		uint64_t k = (uint64_t)(long)key;
		k *= m;
		k ^= k >> r;
		k *= m;
		h ^= k;
		h *= m;
		h ^= h >> r;
		h *= m;
		h ^= h >> r;
		return (h % 631);
	}
	uint64_t bits[10];
};
#endif

/**
 * 用于放node的列表
 */
class BtreeArrayList
{
public:
	static const int32_t NODE_SIZE;
	static const int32_t NODE_COUNT_PRE_PAGE;

	/**
	 * 析构函数
	 */
	BtreeArrayList();

	/**
	 * 初始化
	 */
	void Init(int32_t struct_size);

	/**
	 * 增加到copy_node_中
	 */
	void add_node_list(BtreeAlloc *node_allocator, int32_t unique, void *node);

	/**
	 * 清空copy_list, 不对list上node进行释放, CLEAR_LIST_ONLY
	 */
	void clear_list_only(BtreeAlloc *node_allocator);

	/**
	 * 释放所有copy_node_
	 * @param sequence == 0 释放所有的node
	 */
	int32_t release_node_list(int64_t sequence, BtreeAlloc *key_has_ref);

	/**
	 * copy node数量
	 */
	int32_t get_copy_node_count();

	/**
	 * reset copy list
	 */
	void node_list_reset();

	/**
	 * 对后面的空的list进行回收
	 */
	void shrink_list(BtreeAlloc *node_allocator);

	/**
	 * 得到初始值
	 */
	void node_list_first(BtreeNodeList &list);

	/**
	 * 得到node_list中的next
	 */
	void *node_list_next(BtreeNodeList &list);


#ifdef BLADE_BTREE_CHECK
	void delete_bf(void *node);
#endif

private:
#ifdef BLADE_BTREE_CHECK
	BtreeBloomFilter bf;
#endif
	int32_t max_copy_node_count_;

	// 复制的节点数, copy_node的节点数
	int32_t copy_node_count_;

	// 当copy_node_不够用的时候用.
	void **copy_list_start_;
	void **copy_list_current_;
	void **copy_list_end_;

	// 存放复制过的节点
	void *copy_node_[0];
	// 下面不要定义变量, 保证copy_node_放在最后一个
};
}//end of namespace btree
}//end of namespace bladestore

#endif
