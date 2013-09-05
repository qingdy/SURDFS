/*
 * Copyright (c) 2011,  Sohu R&D
 * All rights reserved.
 * 
 * File   name: mempool.h
 * Description: mempool class; implement effctive, thread safe malloc/free.
 * 
 * Version : 1.0
 * Author  : shiqin@sohu-inc.com
 * Date    : 12-1-17
 * 
 * step to use mempool:
 * 1.Call MemPoolInit() at first;
 * 2.Use MemPoolAlloc(size_t) instead of malloc(size_t), use MemPoolFree(void*) instead of free(void*);
 * 3.Call MemPoolDestory() after use.
 */

#ifndef PANDORA_SYSTEM_MEMPOOL_H
#define PANDORA_SYSTEM_MEMPOOL_H

#include <string>
#include <deque>
#include <iterator>
#include <algorithm>
#include "thread_spinlock.h"
#include "assert.h"
using std::string;
using std::deque;
using std::iterator;

namespace pandora
{

#define MAX_INDEX_NUM 16
#define MIN_MEMUNIT_SIZE 1024

#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName)\
                TypeName(const TypeName&);\
            void operator=(const TypeName&)
#endif

/*
 * This class is used to implement globally memory malloc.
 * DO NOT use Mempool with member fuctions.
 */
class Mempool
{

struct BlockHead
{
    char head_[2];
    size_t size_;
    int16_t index_;
};

public:
    Mempool();
    ~Mempool();
    size_t allocated_size() { return allocated_size_; }
    size_t pooled_size() { return pooled_size_; }
    void* Alloc(size_t m_size);
    bool Free(void *m_ptr);
    string Status();
private:
    int16_t GetIndex(size_t m_size);
    BlockHead* AllocByIndex(int m_index);
    BlockHead* AllocBySize(size_t m_size);
    bool IsValidBlock(BlockHead* const m_ptr);

    deque<BlockHead*> mem_blocks_[MAX_INDEX_NUM];
    int allocated_num_[MAX_INDEX_NUM];
    size_t allocated_size_;
    size_t pooled_size_;

    ThreadSpinLock pool_lock_;
    DISALLOW_COPY_AND_ASSIGN(Mempool);
};

/* 
 * DO NOT use Mempool with member fuction, use fuctions defined below.
 * Call MemPoolInit before using Mempool,
 * Call MemPoolDestroy when finish using Mempool.
 */
bool MemPoolInit();
void MemPoolDestory();

/*
 * @param m_dwSize [input] : the size of memory need to be alloced.
 * @return void* : the pointer to alloced memory, return NULL if failed.
 */ 
void* MemPoolAlloc(size_t m_size);

// @return bool : return false if free fails.
bool MemPoolFree(void* m_ptr);

// @return size_t : return the total size of memory alloced by mempool.
size_t MemPoolAllocatedSize();

// @return size_t : return the total size of memory in mempool.
size_t MemPoolPooledSize();

// @return size_t : return the size of an alloced block, return 0 if m_ptr points to a invalid block.
size_t GetBlockSize(const void* m_ptr);

/*
 * return the using status of mempool, with format:
 * 
 * block_size       alloced     pooled
 * 16               5           7
 * 32               5           3   
 * ..........
 *
 */ 
string MemPoolStat();

}//namespace pandora

#endif//PANDORA_MEMPOOL_H
