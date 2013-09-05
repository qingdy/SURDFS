/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: read_block.h
 * Description: This file defines ReadBlock packet.
 * 
 * Version : 1.0
 * Author  : YYY
 * Date    : 2012-6-18
 */

#ifndef BLADESTORE_MESSAGE_REPLICATE_BLOCK_H
#define BLADESTORE_MESSAGE_REPLICATE_BLOCK_H

#include <stdint.h>
#include <string>

#include "blade_packet.h"
#include "blade_common_define.h"

namespace bladestore 
{
namespace message
{

using namespace bladestore::common;
using std::string;

//----------------------declaration of ReplicateBlockPacket -------------------------------//
class ReplicateBlockPacket : public BladePacket 
{
public:
    ReplicateBlockPacket();

//    ReplicateBlockPacket(const ReplicateBlockPacket &read_block);

    ReplicateBlockPacket(int64_t block_id,  int64_t block_length, int32_t block_version,
                         int64_t file_id, int64_t block_offset, int32_t sequence, int64_t data_length,
                         int64_t checksum_length, char * data, char *checksum);
    ~ReplicateBlockPacket();

    int Pack();
    int Unpack();
    int Reply(BladePacket * resp_packet);
    size_t GetLocalVariableSize();

    int64_t block_id()      const {return block_id_;}
    int32_t block_length()  const {return block_length_;}
    int32_t block_version() const {return block_version_;}
    int64_t file_id()      const {return file_id_;}
    int64_t block_offset()  const {return block_offset_;}
    int32_t sequence()    const {return sequence_;}

    int64_t data_length()   const {return data_length_;}
    int64_t checksum_length() const { return checksum_length_;};
    char *  data() const { return data_;} //actual data
    char *  checksum() const { return checksum_;}

    void    set_delete_flag(bool flag) { delete_flag_ = flag;}
private:
    int64_t block_id_; 
    int64_t block_length_; 
    int32_t block_version_; 
    int64_t file_id_; 
    int64_t block_offset_; //start offset in the block 
    int32_t sequence_;
    int64_t data_length_;
    int64_t checksum_length_;
    char *  data_; //actual data
    char *  checksum_; 
    bool    delete_flag_;

    DISALLOW_COPY_AND_ASSIGN(ReplicateBlockPacket);

};

//----------------------declaration of ReplicateBlockReplyPacket--------------------------------//

class ReplicateBlockReplyPacket : public BladePacket
{
public: 
    ReplicateBlockReplyPacket();
    ReplicateBlockReplyPacket(int16_t ret_code, int64_t block_id,
                              int32_t sequence_); 
    ~ReplicateBlockReplyPacket();

    int Pack();
    int Unpack();
    size_t GetLocalVariableSize();

    int16_t ret_code()   const { return ret_code_;} 	
    int64_t block_id()   const { return block_id_;}
    int32_t sequence() const {return sequence_;}

private:
    int16_t ret_code_; 	//ret code to identify the type of the response
    int64_t block_id_; 
    int32_t sequence_;
    DISALLOW_COPY_AND_ASSIGN(ReplicateBlockReplyPacket);
}; 

}//end of namespace message
}//end of namespace bladestore

#endif
