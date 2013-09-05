/*
 * =====================================================================================
 *
 *       Filename:  block.h
 *
 *    Description: the minest unit to store data  
 *
 *        Version:  1.0
 *        Created:  03/26/2012 05:23:29 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  ZQ (), 
 *   Organization:  
 *
 * =====================================================================================
 */
#ifndef BLADESTORE_COMMON_BLOCK_H
#define BLADESTORE_COMMON_BLOCK_H

#include "stdint.h"
#include <string>

using namespace std;

namespace bladestore
{
namespace common
{ 
class Block
{ 
public:
    Block();
    Block(int64_t block_id,int64_t num_bytes,int32_t block_version);
    Block(int64_t block_id);
    Block(const Block & block);
    Block(int64_t block_id,int64_t num_bytes,int32_t block_version,int64_t file_id_); 

    void    Set(int64_t block_id,int64_t num_bytes,int32_t block_version);
    void    set_block_id(int64_t bid);
    void    set_num_bytes(int64_t len);
    void    set_block_version(int32_t block_version);
    void    set_file_id(int64_t file_id);
    int64_t block_id() const;
    int64_t num_bytes() const;
    int32_t block_version() const;
    int64_t file_id() const;

    string  GetBlockName() const;
    string  ToString() const;
    
    bool operator==(const Block &block) const
    {
        int64_t bid = block.block_id();
        return (memcmp(&block_id_,&bid,sizeof(block_id_)) == 0);
    };

    bool operator < (const Block &right) const
    { 
        return (block_id_ < right.block_id()) || ((block_id_ == right.block_id()) && (block_version_ < right.block_version()));
    }


private:
    int64_t block_id_;
    int64_t num_bytes_;
    int32_t block_version_;
    int64_t file_id_; 
};
}
}
#endif
