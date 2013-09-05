/*
 * =====================================================================================
 *
 *       Filename:  Block.cc
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  03/27/2012 03:54:23 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  ZQ (), 
 *   Organization:  
 *
 * =====================================================================================
 */
#include <stdlib.h>
#include "block.h"
#include "int_to_string.h"

using namespace std;

namespace bladestore
{
namespace common
{
Block::Block():block_id_(0),num_bytes_(0),block_version_(0),file_id_(0)
{ 
}

Block::Block(int64_t block_id,int64_t num_bytes,int32_t block_version)
{
    block_id_ = block_id;
    num_bytes_ = num_bytes;
    block_version_ = block_version;
    file_id_ = 0;
}

Block::Block(int64_t block_id)
{ 
    block_id_ = block_id;
    num_bytes_ = 0;
    block_version_ = 0;
    file_id_ = 0;
}

Block::Block(const Block & block)
{
    this->block_id_ = block.block_id_;
    this->num_bytes_ = block.num_bytes_;
    this->block_version_ = block.block_version_;
    this->file_id_ = block.file_id_;
}

Block::Block(int64_t block_id,int64_t num_bytes,int32_t block_version,int64_t file_id)
{
    block_id_ = block_id;
    num_bytes_ = num_bytes;
    block_version_ = block_version;
    file_id_ = file_id;
}

void Block::Set(int64_t block_id,int64_t num_bytes,int32_t block_version)
{
    block_id_ = block_id;
    num_bytes_ = num_bytes;
    block_version_ = block_version;
}
int64_t Block::block_id() const
{
    return block_id_;
}

void Block::set_block_id(int64_t bid)
{
    block_id_ = bid;
}

//get part of block file's name such as : blk_blockid
string Block::GetBlockName() const
{
    string block_id_string = Int64ToString(block_id_);
    string block_file_name("blk_");
    block_file_name += block_id_string;
        
    return block_file_name; 
}

int64_t Block::num_bytes() const
{
    return num_bytes_;
}

void Block::set_num_bytes(int64_t len)
{
    num_bytes_ = len;
}

int32_t Block::block_version() const
{
    return block_version_;
}

void Block::set_block_version(int32_t block_version)
{
    block_version_ = block_version;
}

string Block::ToString() const
{
    string block_version_string = Int32ToString(block_version_);
    return GetBlockName() + "_" + block_version_string;
}
int64_t Block::file_id() const
{
    return file_id_;
}
void Block::set_file_id(int64_t file_id)
{
    file_id_ = file_id;
}

}//end of namespace common
}//end of namespace bladestore
