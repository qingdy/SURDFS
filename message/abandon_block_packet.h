/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: abandon_block_packet.h
 * Description: This file defines AbandonBlock packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-5
 * Revised :
 */

#ifndef BLADESTORE_MESSAGE_ABANDON_BLOCK_PACKET_H
#define BLADESTORE_MESSAGE_ABANDON_BLOCK_PACKET_H

#include <stdint.h>
#include <string>
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "blade_packet.h"
#include "blade_common_data.h"

using std::string;
using namespace bladestore::common;

namespace bladestore
{
namespace message
{
class AbandonBlockPacket : public BladePacket
{
public:
    AbandonBlockPacket();  // receiver
    AbandonBlockPacket(int64_t file_id, int64_t block_id);  // sender
    ~AbandonBlockPacket();
    int Pack();
    int Unpack();
    int64_t file_id();
    int64_t block_id();
    int Reply(BladePacket * resp_packet);
private:
    int64_t file_id_;
    int64_t block_id_;
    size_t GetLocalVariableSize();
};  // end of class AbandonBlockPacket

class AbandonBlockReplyPacket : public BladePacket
{
public:
    AbandonBlockReplyPacket();  // receiver
    explicit AbandonBlockReplyPacket(int16_t ret_code);  // sender
    ~AbandonBlockReplyPacket();
    int Pack();
    int Unpack();
    int16_t ret_code();
    void set_ret_code(int16_t ret_code);
private:
    int16_t ret_code_;
    size_t GetLocalVariableSize();
};  // end of class AbandonBlockReplyPacket
}  // end of namespace message
}  // end of namespace bladestore
#endif
