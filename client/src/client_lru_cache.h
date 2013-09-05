/*
 * Copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: lrucache.h
 * Description: this file defines the LRU cache store on client
 * 
 * Version : 1.0
 * Author  : guili
 * Date    : 2012-3-27
 *
 */

#ifndef BLADESTORE_CLIENT_CLIENT_LRU_CACHE_H
#define BLADESTORE_CLIENT_CLINET_LRU_CACHE_H

#include <stdint.h>
#include <sys/types.h>
#include <ext/hash_map>
#include <string>
#include <map>
#include <utility>
#include "blade_common_define.h"
#include "client_cache_node.h"

using std::string;
using std::pair;

namespace bladestore
{

namespace common
{
class LocatedBlockSet;
}

namespace client
{

struct str_hash {
    size_t operator() (const std::string& str) const
    {
        return __gnu_cxx::__stl_hash_string(str.c_str());
    }
};

class LRUCache
{
public:
    LRUCache();
    ~LRUCache();

    // initialize the LRU cache with an empty CacheNode
    int32_t Init(int32_t max_size);

    CacheNode* LookupByFileName(string filename);
    CacheNode* LookupByFileID(int64_t file_id);

    CacheNode* AddCacheNode(string filename, int64_t file_id, int8_t file_type);
    void AddToFileIDMap(CacheNode * cache_node);

    void DeleteCacheNode(string filename);

//    only for test
//    int32_t max_size() const { return max_size_;}
//    int32_t size() const { return size_;}

private:
    typedef __gnu_cxx::hash_map<string, CacheNode*, str_hash> CacheMap;
    typedef CacheMap::value_type valType;

    // if cachenode of a file not exists, add a new one
    // else ,clear all the data in that cache node ,and return
    CacheNode* AddNewCacheNode(string filename);
    int32_t DeleteCacheMapTail();

    int32_t size_;
    int32_t max_size_;
    CacheNode *head_;
    CacheNode *tail_;

    CacheMap cache_map_;
    __gnu_cxx::hash_map<int64_t, CacheNode *> fid_map_;

    DISALLOW_COPY_AND_ASSIGN(LRUCache);
};

}  // end of namespace client
}  // end of namespace bladestore
#endif
