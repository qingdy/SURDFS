/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: create_packet.h
 * Description: This file defines Create packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-5
 * Revised :
 */

#ifndef BLADESTORE_MESSAGE_CREAT_PACKET_H
#define BLADESTORE_MESSAGE_CREAT_PACKET_H

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
class CreatePacket : public BladePacket
{
public:
    CreatePacket();  // receiver
    CreatePacket(int64_t parent_id, string file_name, int8_t oflag,
                int16_t replication);  // sender
    ~CreatePacket();

    int Pack();
    int Unpack();
    int Reply(BladePacket * resp_packet);
    int64_t parent_id();
    string file_name();
    int8_t oflag();
    int16_t replication();
private:
    int64_t parent_id_;
    string file_name_;
    int8_t oflag_;
    int16_t replication_;
    size_t GetLocalVariableSize();
};  // end of class CreatePacket

class CreateReplyPacket : public BladePacket
{
public:
    CreateReplyPacket();  // receiver
    CreateReplyPacket(int16_t ret_code, int64_t file_id);  // sender
    ~CreateReplyPacket();

    int Pack();
    int Unpack();
    int16_t ret_code();
    int64_t file_id();
    void set_ret_code(int16_t ret_code);
    void set_file_id(int64_t file_id);
private:
    int16_t ret_code_;
    int64_t file_id_;
    size_t GetLocalVariableSize();
};  // end of class CreateReplyPacket

}  // end of namespace message
}  // end of namespace bladestore

#endif
