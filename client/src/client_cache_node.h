/*
 * Copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: cachenode.h
 * Description: This file defines the struct CacheNode.
 * 
 * Version : 1.0
 * Author  : guili
 * Date    : 2012-3-27
 */

#ifndef BLADESTORE_CLIENT_CLIENT_CACHE_NODE_H
#define BLADESTORE_CLIENT_CLIENT_CACHE_NODE_H

#include <stdint.h>
#include <sys/types.h>
#include <ctime>
#include <string>
#include <map>
#include <utility>

namespace bladestore
{

namespace common
{
    struct LocatedBlock;
    class LocatedBlockSet;
}

namespace client
{
using std::string;
using std::map;
using std::pair;
using bladestore::common::LocatedBlock;
using bladestore::common::LocatedBlockSet;

struct CacheNode
{
    CacheNode();
    CacheNode(string src, CacheNode *next);
    ~CacheNode();
    LocatedBlockSet ReadCacheNode(int64_t &offset, int64_t length);
    void RefreshCacheNode(const LocatedBlock &located_block);
    void RefreshCacheNode(const LocatedBlockSet &located_blocks);

    string filename_;
    int64_t fid_;
    int64_t file_length_;
    int8_t  file_type_;
    map<off_t, LocatedBlock> located_block_map_;

    time_t cached_time_;

    CacheNode *prev_;
    CacheNode *next_;
};
}  // end of namespace client
}  // end of namespace bladestore
#endif
