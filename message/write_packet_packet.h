/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: write_packet.h
 * Description: This file defines WritePacket packet.
 *
 * Version : 1.0 
 * Author  : YYY
 * Date    : 2012-6-6
 * Revised :
 */

#ifndef BLADESTORE_MESSAGE_WRITE_PACKET_H
#define BLADESTORE_MESSAGE_WRITE_PACKET_H

#include <stdint.h>

#include "blade_packet.h"
#include "blade_common_define.h"


namespace bladestore 
{
namespace message
{
using namespace bladestore::common;

//-------------------------WritePacketPacket -------------------------//
class WritePacketPacket : public BladePacket
{
public:
    WritePacketPacket();
    WritePacketPacket(WritePacketPacket &write_packet);
    WritePacketPacket(int64_t block_id, int32_t block_version_,
            int64_t block_offset, int64_t sequence, int8_t target_num,
            int64_t data_length, int64_t checksum_length_, char * data, 
            char * checksum);
    ~WritePacketPacket();

    int Pack();
    int Unpack();
    int Init();
    size_t GetLocalVariableSize();
    
    int64_t block_id() const { return block_id_;} 
    int32_t block_version() const { return block_version_;} 
    int64_t block_offset() const { return block_offset_;}
    int64_t sequence() const { return sequence_;}
    int8_t  target_num() const { return target_num_;}
    int64_t data_length() const { return data_length_;}
    int64_t checksum_length() const { return checksum_length_;};
    char *  data() const { return data_;} //actual data
    char *  checksum() { return checksum_;}

private:
    int64_t block_id_; 
    int32_t block_version_; 
    int64_t block_offset_;
    int64_t sequence_;
    int8_t  target_num_;
    int64_t data_length_;
    int64_t checksum_length_;
    char *  data_; //actual data
    char *  checksum_; 
    
}; //end of class WritePacketPacket

//-------------------------WritePacketReplyPacket -------------------------//
class WritePacketReplyPacket : public BladePacket
{ 
public:
    WritePacketReplyPacket();
    WritePacketReplyPacket(int64_t block_id, int64_t sequence,
                           int16_t ret_code_);
    ~WritePacketReplyPacket();

    int Pack();
    int Unpack();
    size_t GetLocalVariableSize();

    int64_t block_id() const {return block_id_;}
    int64_t sequence() const {return sequence_;}
    int16_t ret_code() const {return ret_code_;};
    int16_t get_ret_code() const {return ret_code_;};

private:
    int64_t block_id_;
    int64_t sequence_;
    int16_t ret_code_;

    DISALLOW_COPY_AND_ASSIGN(WritePacketReplyPacket);
}; //end of class WritePacketReplyPacket

}//end of namespace message
}//end of namespace bladestore

#endif
