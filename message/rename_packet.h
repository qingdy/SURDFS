/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: rename_packet.h
 * Description: This file defines Rename packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 * Revised :
 */

#ifndef BLADESTORE_MESSAGE_RENAME_PACKET_H
#define BLADESTORE_MESSAGE_RENAME_PACKET_H

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
class RenamePacket:public BladePacket
{
public:
    RenamePacket();  // receiver
    RenamePacket(string src_file_name, string dst_file_name);  // sender
    ~RenamePacket();

    int Pack();
    int Unpack();
    int Reply(BladePacket * resp_packet);
    string get_src_file_name();
    string get_dst_file_name();
private:
    string src_file_name_;
    string dst_file_name_;
    size_t GetLocalVariableSize();
};  // end of class RenamePacket

class RenameReplyPacket:public BladePacket
{
public:
    RenameReplyPacket();  // receiver
    explicit RenameReplyPacket(int16_t ret_code);  // sender
    ~RenameReplyPacket();

    int Pack();
    int Unpack();
    int16_t get_ret_code();

    void set_ret_code(int16_t ret_code);
private:
    int16_t ret_code_;
    size_t GetLocalVariableSize();
};  // end of class RenameReplyPacket
}  // end of namespace message
}  // end of namespace bladestore
#endif

