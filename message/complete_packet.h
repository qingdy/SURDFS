/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: complete_packet.h
 * Description: This file defines Complete packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 * Revised :
 */

#ifndef BLADESTORE_MESSAGE_COMPLETE_PACKET_H
#define BLADESTORE_MESSAGE_COMPLETE_PACKET_H

#include <stdint.h>
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "blade_packet.h"
#include "blade_common_data.h"

using namespace bladestore::common;

namespace bladestore
{
namespace message
{
class CompletePacket : public BladePacket
{
public:
    CompletePacket();
    CompletePacket(int64_t file_id_, int64_t block_id, int32_t block_size);
    ~CompletePacket();

    int Pack();
    int Unpack();
    int Reply(BladePacket * resp_packet);
    int64_t file_id();
    int64_t block_id();
    int32_t block_size();
private:
    int64_t file_id_;
    int64_t block_id_;
    int32_t block_size_;
    size_t GetLocalVariableSize();
};  // end of class CompletePacket

class CompleteReplyPacket : public BladePacket
{
public:
    CompleteReplyPacket();  // receiver
    explicit CompleteReplyPacket(int16_t ret_code);  // sender
    ~CompleteReplyPacket();

    int Pack();
    int Unpack();
    int16_t ret_code();
    void set_ret_code(int16_t ret_code);
private:
    int16_t ret_code_;
    size_t GetLocalVariableSize();
};  // end of class CompleteReplyPacket
}  // end of namespace message
}  // end of namespace bladestore

#endif
