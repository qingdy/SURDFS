/*
 *version  :  1.0
 *authro   :  chen yunyun
 *date     :  2012-4-18
 *
 */
#ifndef BLADESTORE_BTREE_WRITE_HANDLE_H
#define BLADESTORE_BTREE_WRITE_HANDLE_H

#include <stdint.h>
#include <pthread.h>
#include "btree_alloc.h"
#include "btree_array_list.h"
#include "btree_base_handle.h"

namespace bladestore
{
namespace btree
{
/**
 * 用于写的时候用
 */
class BtreeWriteHandle : public BtreeBaseHandle, BtreeCallback
{
	friend class BtreeBase;
public:
	/**
	 * 构造
	 */
	BtreeWriteHandle();

	/**
	 * 析构
	 */
	~BtreeWriteHandle();

	/**
	 * 结束
	 */
	void End();

	/**
	 * 回滚
	 */
	void Rollback();

	/**
	 * 产生一个下一级的writehandle
	 */
	BtreeWriteHandle *get_write_handle();

	/**
	 * 让下级writehandle回调, 把下一级的handle提交上来
	 */
	int callback_done(void *data);

	/**
	 * 得到object数量
	 */
	int64_t get_object_count();

	/**
	 * 得到node数量
	 */
	int32_t get_node_count();

	/**
	 * 得到old_value
	 */
	char *get_old_value();

	/**
	 * 加key到list中
	 */
	void add_key_to_list(char *key, int32_t value);

private:
	/**
	 * 开始
	 */
	void Start(pthread_mutex_t *mutex, BtreeCallback *owner);

private:
	//上一个RootPointer
	BtreeRootPointer *prev_root_pointer_;

	//key的分配器
	BtreeAlloc *key_allocator_;

	BtreeAlloc *node_allocator_;

	//owner的树
	BtreeCallback *owner_;

	//用于释放时用
	pthread_mutex_t *mutex_;

	//是否出错了
	int32_t error_;

	//自己创建了prev_root_pointer
	int16_t ppr_create_;

	//第几层write_handle
	int16_t level_;

	//上层write_handle
	BtreeWriteHandle *parent_;

	//下一代副本, 如果存在下一代副本, 此handle是不能进行更新
	BtreeWriteHandle *next_handle_;

	//删除或者覆盖更新时返回原value
	char *old_value_;

	//存放引用计数要减一的key
	BtreeArrayList old_key_list_;

	BtreeArrayList new_key_list_;
	//下面不要定义变量, 保证放在最后一个
};

}//end of namespace btree
}//end of namespace bladestore

#endif


