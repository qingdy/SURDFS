/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: add_block_packet.h
 * Description: This file defines AddBlock packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 * Revised :
 */

#ifndef BLADESTORE_MESSAGE_ADD_BLOCK_PACKET_H
#define BLADESTORE_MESSAGE_ADD_BLOCK_PACKET_H

#include <stdint.h>
#include <string>
#include <vector>
#include "bladestore_ops.h"
#include "blade_packet.h"
#include "blade_common_define.h"
#include "located_blocks.h"
#include "blade_common_data.h"

using std::string;
using std::vector;
using namespace bladestore::common;

namespace bladestore
{
namespace message
{
class AddBlockPacket : public BladePacket
{
public:
    AddBlockPacket();  // receiver
    AddBlockPacket(int64_t file_id, int8_t is_safe_write);  // sender
    ~AddBlockPacket();

    int Pack();
    int Unpack();
    int64_t file_id();
    int8_t is_safe_write();
    int Reply(BladePacket * resp_packet);
private:
    int64_t file_id_;
    int8_t is_safe_write_;
    size_t GetLocalVariableSize();
};  // end of class AddBlockPacket

class AddBlockReplyPacket : public BladePacket
{
public:
    AddBlockReplyPacket();  // receiver
    AddBlockReplyPacket(int16_t ret_code, int64_t block_id,
                        int32_t block_version, int64_t offset,
                        const vector<uint64_t> &dataserver_id);  // sender
    ~AddBlockReplyPacket();

    int Pack();
    int Unpack();
    int16_t ret_code();
    int16_t dataserver_num();
    LocatedBlock &located_block();
    void set_ret_code(int16_t ret_code);
    void set_dataserver_num(int16_t dataserver_num);
    void set_located_block(LocatedBlock &located_block);
private:
    int16_t ret_code_;
    int16_t dataserver_num_;
    LocatedBlock located_block_;
    size_t GetLocalVariableSize();
};  // end of class AddBlockReplyPacket

class LogAddBlockPacket : public BladePacket
{
public:
    LogAddBlockPacket(); //receiver
    LogAddBlockPacket(int64_t file_id, vector<uint64_t> &results, int8_t is_safe_write);
    ~LogAddBlockPacket();

    int Pack();
    int Unpack();
    int64_t file_id();
    int8_t is_safe_write();
    vector<uint64_t> &ds_vector();
private:
    int64_t file_id_;
    int8_t is_safe_write_;
    int16_t ds_num_;
    vector<uint64_t> ds_vector_;
    size_t GetLocalVariableSize();
}; //end of class LogAddBlockPacket
}  // end of namespace message
}  // end of namespace bladestore

#endif
