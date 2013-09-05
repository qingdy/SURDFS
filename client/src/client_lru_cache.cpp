/*
 * Copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: lrucache.cpp
 * Description: the class LRUCache.
 *
 * Version : 1.0
 * Author  : guili
 * Date    : 2012-4-09
 */

#include <vector>
#include "client_lru_cache.h"
#include "log.h"
#include "located_blocks.h"
#include "blade_common_define.h"

using namespace pandora;
using std::vector;
using namespace bladestore::common;
namespace bladestore
{
namespace client
{

LRUCache::LRUCache()
{
}

LRUCache::~LRUCache()
{
    for (CacheNode *cn = head_; cn != tail_; ) {
        CacheNode *deletecn = cn;
        cn = cn->next_;
        delete deletecn;
    }
    delete tail_;
}

int32_t LRUCache::Init(int32_t max_size)
{
    max_size_ = max_size;
    CacheNode *cn = new CacheNode();
    if (NULL == cn)
        return BLADE_ERROR;
    cache_map_.insert(valType("", cn));
    head_ = tail_ = cn;
    size_ = 1;
    return BLADE_SUCCESS;
}

CacheNode* LRUCache::LookupByFileName(string filename)
{
    LOGV(LL_INFO, "Findcachenodebyfilename, filename: %s.", filename.c_str());

    if (filename.empty())
        return NULL;

    CacheMap::iterator it = cache_map_.find(filename);
    if (it != cache_map_.end()) {
        CacheNode *cn = it->second;

        // move to the head of the list
        if (head_ == cn) {
            return cn;
        }
        if (tail_ == cn) {  // cn != head_
            tail_ = cn->prev_;
            cn->prev_->next_ = NULL;
            cn->prev_ = NULL;
            cn->next_ = head_;
            head_->prev_ = cn;
            head_ = cn;
            return cn;
        }
        cn->prev_->next_ = cn->next_;
        cn->next_->prev_ = cn->prev_;
        cn->next_ = head_;
        cn->prev_ = NULL;
        head_->prev_ = cn;
        head_ = cn;

        return cn;
    } else {
        return NULL;
    }
}

CacheNode* LRUCache::LookupByFileID(int64_t file_id)
{
    if (fid_map_.count(file_id)) {
        return fid_map_[file_id];
    } else {
        return NULL;
    }
}

CacheNode* LRUCache::AddCacheNode(string filename, int64_t file_id, int8_t file_type)
{
    LOGV(LL_INFO, "AddCacheNode, filename: %s, file id: %ld.",
         filename.c_str(), file_id);
    CacheNode * newcn = LookupByFileName(filename);

    if (NULL == newcn)
        newcn = AddNewCacheNode(filename);

    newcn->fid_ = file_id;
    newcn->file_length_ = 0;
    newcn->file_type_ = file_type;
    newcn->located_block_map_.clear();
    newcn->cached_time_ = time(NULL);

    if (file_id >= ROOTFID && file_type == BLADE_FILE_TYPE_FILE)
        fid_map_[file_id] =  newcn;

    return newcn;
}

void LRUCache::AddToFileIDMap(CacheNode * cache_node)
{
    int64_t file_id = cache_node->fid_;
    if (file_id >= ROOTFID && cache_node->file_type_ == BLADE_FILE_TYPE_FILE)
        fid_map_[file_id] = cache_node;
}

void LRUCache::DeleteCacheNode(string filename)
{
    CacheNode *deletecn = LookupByFileName(filename);

    if (NULL == deletecn)
        return;

    cache_map_.erase(deletecn->filename_);
    fid_map_.erase(deletecn->fid_);

    if (size_ > 1) {
        head_ = deletecn->next_;
        head_->prev_ = NULL;
        --size_;
        delete deletecn;
    } else {
        deletecn->filename_ = "";
        deletecn->fid_ = 0;
        deletecn->file_length_ = 0;
        deletecn->file_type_ = BLADE_FILE_TYPE_UNKNOWN;
        deletecn->located_block_map_.clear();
    }
}

CacheNode* LRUCache::AddNewCacheNode(string filename)
{
    if (size_ == max_size_) {
        DeleteCacheMapTail();
        --size_;
    }
    CacheNode *newcn = new CacheNode(filename, head_);
    head_->prev_ = newcn;
    head_ = newcn;
    cache_map_.insert(valType(filename, newcn));
    ++size_;
    return newcn;
}

int32_t LRUCache::DeleteCacheMapTail()
{
    CacheNode *cn = tail_;
    tail_ = tail_->prev_;
    tail_->next_ = NULL;
    cache_map_.erase(cn->filename_);
    fid_map_.erase(cn->fid_);
    delete cn;
    return 0;
}

}  // end of namespace client
}  // end of namespace bladestore
