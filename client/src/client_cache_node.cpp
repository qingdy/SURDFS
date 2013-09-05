/*
 * Copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: cachenode.cpp
 * Description: This file defines the struct CacheNode.
 * 
 * Version : 1.0
 * Author  : guili
 * Date    : 2012-3-27
 */

#include <ctime>
#include "client_cache_node.h"
#include "located_blocks.h"
#include "blade_common_define.h"

using namespace bladestore::common;
namespace bladestore
{
namespace client
{
CacheNode::CacheNode():filename_(""), fid_(0), file_length_(0),
        file_type_(BLADE_FILE_TYPE_UNKNOWN), located_block_map_(),
        cached_time_(time(NULL)), prev_(NULL), next_(NULL) {}

CacheNode::CacheNode(string filename, CacheNode *next):filename_(filename),
        fid_(0), file_length_(0), file_type_(BLADE_FILE_TYPE_UNKNOWN),
        located_block_map_(), cached_time_(time(NULL)), prev_(NULL),
        next_(next) {}

CacheNode::~CacheNode() {}

LocatedBlockSet CacheNode::ReadCacheNode(int64_t &offset, int64_t length)
{
    vector<LocatedBlock> vlb;
    if (fid_ == -1 || length <= 0 || offset < 0) {
        // invalid, should refresh this CacheNode
        LocatedBlockSet lbs(-1, -1, vlb);
        return lbs;
    }

    // TODO add the max stored time to conf
    if (time(NULL) - cached_time_ > 36000) {
        located_block_map_.clear();
        cached_time_ = time(NULL);
        LocatedBlockSet lbs(fid_, file_length_, vlb);
        return lbs;
    }

    map<off_t, LocatedBlock> &lbmap = located_block_map_;

    int64_t first_index = offset - offset % BLADE_BLOCK_SIZE;
    int64_t last_index = offset + length - (offset + length) % BLADE_BLOCK_SIZE;

    for (; first_index <= last_index;) {
        map<off_t, LocatedBlock>::iterator it = lbmap.find(first_index);
        if (it != lbmap.end()) {
            vlb.push_back(it->second);
            first_index += BLADE_BLOCK_SIZE;
        } else {
            break;
        }
    }

    if (first_index <= last_index) {
        if (offset < first_index) {
            offset = first_index;
        }
    } else {
        offset = offset + length;
    }
    LocatedBlockSet lbs(fid_, file_length_, vlb);

    return lbs;
}

void CacheNode::RefreshCacheNode(const LocatedBlock &located_block)
{
    if (file_length_ < located_block.offset() + located_block.length()) {
        file_length_ = located_block.offset() + located_block.length();
    }
    pair<map<off_t, LocatedBlock>::iterator, bool> ret =
        located_block_map_.insert(
            map<off_t, LocatedBlock>::value_type(located_block.offset(),
                                                 located_block));
    if (!ret.second) {
        located_block_map_.erase(located_block.offset());
        located_block_map_.insert(
            map<off_t, LocatedBlock>::value_type(located_block.offset(),
                                                 located_block));
    }
}

void CacheNode::RefreshCacheNode(const LocatedBlockSet &located_blocks)
{
    fid_ = located_blocks.fid_;
    const vector <LocatedBlock> &vlb = located_blocks.located_block_;
    for (vector<LocatedBlock>::const_iterator it = vlb.begin();
            it != vlb.end(); ++it) {
        RefreshCacheNode((*it));

        file_length_ = located_blocks.file_length_;
    }
}

}  // end of namespace client
}  // end of namespace bladestore
