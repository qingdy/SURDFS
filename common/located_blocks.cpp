/*
 * Copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: located_blocks.cpp
 * Description: the class LocatedBlock and LocatedBlockSet.
 */

#include "located_blocks.h"

namespace bladestore
{

namespace common
{
LocatedBlock::LocatedBlock(): block_id_(0), block_version_(0),
    offset_(0), length_(0), dataserver_id_() {}

LocatedBlock::LocatedBlock(int64_t block_id, int32_t block_version,
                           int64_t offset, int64_t length,
                           const vector<uint64_t> &dataserver_id):
    block_id_(block_id), block_version_(block_version), offset_(offset),
    length_(length), dataserver_id_(dataserver_id) {}

int16_t LocatedBlock::dataserver_num() const
{
    return dataserver_id_.size();
}

int64_t LocatedBlock::block_id() const
{
    return block_id_;
}

int32_t LocatedBlock::block_version() const
{
    return block_version_;
}

int64_t LocatedBlock::offset() const
{
    return offset_;
}

int64_t LocatedBlock::length() const
{
    return length_;
}

const vector<uint64_t> &LocatedBlock::dataserver_id() const
{
    return dataserver_id_;
}

int64_t LocatedBlock::GetSize() const
{
    int64_t size = 0;
    size = sizeof(block_id_) + sizeof(block_version_) + sizeof(offset_) +
           sizeof(length_) + dataserver_id_.size()*sizeof(int64_t);
    return size;
}

void LocatedBlock::set_block_id(int64_t block_id)
{
    block_id_ = block_id;
}

void LocatedBlock::set_block_version(int32_t block_version)
{
    block_version_ = block_version;
}

void LocatedBlock::set_offset(int64_t offset)
{
    offset_ = offset;
}

void LocatedBlock::set_length(int64_t length)
{
    length_ = length;
}

void LocatedBlock::set_dataserver_id(const vector<uint64_t> &dataserver_id)
{
    dataserver_id_ = dataserver_id;
}

LocatedBlock::~LocatedBlock() {}

LocatedBlockSet::LocatedBlockSet(): fid_(-1), file_length_(-1), located_block_() {}

LocatedBlockSet::LocatedBlockSet(const LocatedBlockSet &lbs)
{
    fid_ = lbs.fid_;
    file_length_ = lbs.file_length_;
    located_block_ = lbs.located_block_;
}

LocatedBlockSet::LocatedBlockSet(int64_t fid, int64_t file_length,
                             const vector<LocatedBlock> &located_block):
        fid_(fid), file_length_(file_length), located_block_(located_block) {}

LocatedBlockSet::~LocatedBlockSet()
{}
}
}
