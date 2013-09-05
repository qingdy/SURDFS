/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: is_valid_path_packet.h
 * Description: This file defines IsValidPath packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 * Revised :
 */

#ifndef BLADESTOR_MESSAGE_IS_VALID_PATH_PACKET_H
#define BLADESTOR_MESSAGE_IS_VALID_PATH_PACKET_H

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
class IsValidPathPacket : public BladePacket
{
public:
    IsValidPathPacket();  // receiver
    explicit IsValidPathPacket(int64_t parent_id, string file_name);  // sender
    ~IsValidPathPacket();

    int Pack();
    int Unpack();
    int Reply(BladePacket * resp_packet);
    int64_t parent_id();
    string file_name();
private:
    int64_t parent_id_;
    string file_name_;
    size_t GetLocalVariableSize();
};  // end of class IsValidPathPacket

class IsValidPathReplyPacket : public BladePacket
{
public:
    IsValidPathReplyPacket();  // receiver
    explicit IsValidPathReplyPacket(int16_t ret_code);  // sender
    ~IsValidPathReplyPacket();

    int Pack();
    int Unpack();
    int16_t ret_code();

    void set_ret_code(int16_t ret_code);
private:
    int16_t ret_code_;
    size_t GetLocalVariableSize();
};  // end of class IsValidPathReplyPacket
}  // end of namespace message
}  // end of namespace bladestore

#endif
