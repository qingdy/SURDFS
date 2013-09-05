/*
 * File   name: get_block_locations_packet.h
 * Description: This file defines GetBlockLocations packet.
 *
 * Version : 0.1
 * Author  : guili
 * Date    : 2012-4-05
 * Revised :
 */

#ifndef BLADESTORE_MESSAGE_GET_BLOCK_LOCATIONS_PACKET_H
#define BLADESTORE_MESSAGE_GET_BLOCK_LOCATIONS_PACKET_H

#include <stdint.h>
#include <string>
#include "blade_packet.h"
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "located_blocks.h"
#include "blade_common_data.h"
using std::string;
using namespace bladestore::common;

namespace bladestore
{
namespace message
{
class GetBlockLocationsPacket : public BladePacket
{
public:
    GetBlockLocationsPacket();
    GetBlockLocationsPacket(int64_t parent_id, string file_name, int64_t offset,
                            int64_t data_length);
    ~GetBlockLocationsPacket();
    int Pack();
    int Unpack();
    int Reply(BladePacket * resp_packet);
    int64_t parent_id();
    string file_name();
    int64_t offset();
    int64_t data_length();
private:
    int64_t parent_id_;
    string file_name_;  // filename or pathname
    int64_t offset_;    // requested data offset in the file
    int64_t data_length_;  // requested file data length
    size_t GetLocalVariableSize();
};  // end of class GetBlockLocationsPacket

class GetBlockLocationsReplyPacket : public BladePacket
{
public:
    GetBlockLocationsReplyPacket();
    GetBlockLocationsReplyPacket(int16_t ret_code,
                                 int64_t file_id,
                                 int64_t file_length,
                                 vector<LocatedBlock> &v_located_block);
    ~GetBlockLocationsReplyPacket();
    int Pack();
    int Unpack();
    int16_t ret_code();
    int64_t file_id();
    int64_t file_length();
    int16_t located_block_num();
    vector<LocatedBlock> &v_located_block();
    
    void set_ret_code(int16_t ret_code);
    void set_file_id(int64_t file_id);
    void set_file_length(int64_t file_length);
    void set_located_block_num(int16_t located_block_num);
    void set_v_located_block(vector<LocatedBlock> &v_located_block);
private:
    int16_t ret_code_;
    int64_t file_id_;
    int64_t file_length_;
    int16_t located_block_num_;
    vector<LocatedBlock> v_located_block_;
    size_t GetLocalVariableSize();
};  // end of class GetBlockLocationsReplyPacket
}  // end of namespace message
}  // end of namespace bladestore
#endif
