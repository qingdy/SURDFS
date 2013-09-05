/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: delete_file_packet.h
 * Description: This file defines DeleteFile packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 * Revised :
 */

#ifndef BLADESTORE_MESSAGE_DELETE_FILE_PACKET_H
#define BLADESTORE_MESSAGE_DELETE_FILE_PACKET_H

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
class DeleteFilePacket : public BladePacket
{
public:
    DeleteFilePacket();  // receiver
    explicit DeleteFilePacket(int64_t parent_id, string file_name, string path);  // sender
    ~DeleteFilePacket();

    int Pack();
    int Unpack();
    int Reply(BladePacket * resp_packet);
    int64_t parent_id();
    string file_name();
    string path();
private:
    int64_t parent_id_;
    string file_name_;
    string path_;
    size_t GetLocalVariableSize();
};  // end of class DeleteFilePacket

class DeleteFileReplyPacket : public BladePacket
{
public:
    DeleteFileReplyPacket();  // receiver
    explicit DeleteFileReplyPacket(int16_t ret_code);  // sender
    ~DeleteFileReplyPacket();

    int Pack();
    int Unpack();
    int16_t ret_code();
    void set_ret_code(int16_t ret_code);
private:
    int16_t ret_code_;
    size_t GetLocalVariableSize();
};  // end of class DeleteFileReplyPacket
}  // end of namespace message
}  // end of namespace bladestore

#endif
