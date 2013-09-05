/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: read_block.h
 * Description: This file defines ReadBlock packet.
 * 
 * Version : 1.0
 * Author  : YYY
 * Date    : 2012-6-6
 */

#ifndef BLADESTORE_MESSAGE_READ_BLOCK_H
#define BLADESTORE_MESSAGE_READ_BLOCK_H

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

//----------------------declaration of ReadBlockPacket -------------------------------//
class ReadBlockPacket : public BladePacket 
{
public:
    ReadBlockPacket();

    ReadBlockPacket(ReadBlockPacket &read_block);

    ReadBlockPacket(int64_t block_id, int32_t block_version, 
                    int64_t block_offset, int64_t request_data_length, int64_t sequence);
    ~ReadBlockPacket();

    int Pack();
    int Unpack();
    int Reply(BladePacket * resp_packet);
    size_t GetLocalVariableSize();

    int64_t block_id() { return block_id_;}
    int32_t block_version() { return block_version_;}
    int64_t block_offset() { return block_offset_;}
    int64_t request_data_length() { return request_data_length_;}
    int64_t sequence() { return sequence_;}
    void set_request_data_length(int64_t len) { request_data_length_ = len;}

private:
    int64_t block_id_; 
    int32_t block_version_; 
    int64_t block_offset_; //start offset in the block 
    int64_t request_data_length_;// length of data the client requset to read  
    int64_t sequence_; 

};

//----------------------declaration of ReadBlockReplyPacket--------------------------------//

class ReadBlockReplyPacket : public BladePacket
{
public: 
    ReadBlockReplyPacket();
    ReadBlockReplyPacket(const ReadBlockReplyPacket &read_block_reply);
    ReadBlockReplyPacket(int16_t ret_code, int64_t block_id, 
                         int64_t block_offset,int64_t sequence,
                         int64_t data_length_,char * data);
    ~ReadBlockReplyPacket();

    int Pack();
    int Unpack();
    size_t GetLocalVariableSize();

    int16_t ret_code() const{ return ret_code_;} 	
    int64_t block_id() const{ return block_id_;}
    int64_t block_offset() const{ return block_offset_;}
    int64_t  sequence() const{ return sequence_;}
    int64_t  data_length() const{ return data_length_;}
    char *   data() const{ return data_;}

private:
    int16_t ret_code_; 	//ret code to identify the type of the response
    int64_t block_id_; 
    int64_t block_offset_; //start offset in the block 
    int64_t  sequence_; //
    int64_t  data_length_; //
    char *   data_; //actual data

//    DISALLOW_COPY_AND_ASSIGN(ReadBlockReplyPacket);
}; 

}//end of namespace message
}//end of namespace bladestore

#endif
