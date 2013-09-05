/*
 * Copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: locatedblocks.h
 * Description: This file defines the class LocatedBlock and the class LocatedBlockSet.
 * 
 * Version : 1.0
 * Author  : guili
 * Date    : 2012-4-09
 */

#ifndef BLADESTORE_COMMON_LOCATED_BLOCKS_H
#define BLADESTORE_COMMON_LOCATED_BLOCKS_H

#include <stdint.h>
#include <sys/types.h>
#include <vector>

using std::vector;
namespace bladestore
{
namespace common
{
class LocatedBlock
{
public:
    LocatedBlock();
    LocatedBlock(int64_t block_id, int32_t block_version, int64_t offset,
                 int64_t length, const vector<uint64_t> &dataserver_id);

    ~LocatedBlock();

    int16_t dataserver_num() const;

    int64_t block_id() const;
    int32_t block_version() const;
    int64_t offset() const;
    int64_t length() const;
    const vector<uint64_t> &dataserver_id() const;

    // get the size of the LocatedBlock
    int64_t GetSize() const;

    void set_block_id(int64_t block_id);
    void set_block_version(int32_t block_version);
    void set_offset(int64_t offset);
    void set_length(int64_t length);
    void set_dataserver_id(const vector<uint64_t> & dataserver_id);
private:
    int64_t block_id_;
    int32_t block_version_;
    off_t offset_;
    int64_t length_;
    vector<uint64_t> dataserver_id_;
};

struct LocatedBlockSet
{
    int64_t fid_;
    int64_t file_length_;
    vector<LocatedBlock> located_block_;

    LocatedBlockSet();
    LocatedBlockSet(const LocatedBlockSet &lbs);
    LocatedBlockSet(int64_t fid, int64_t file_length,
                    const vector<LocatedBlock> &located_block);

    ~LocatedBlockSet();
};

}  // end of namespace client
}  // end of namespace bladestore
#endif
