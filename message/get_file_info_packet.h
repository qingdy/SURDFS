/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: get_file_info_packet.h
 * Description: This file defines GetFileInfo packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 */

#ifndef BLADESTORE_MESSAGE_GET_FILE_INFO_PACKET_H
#define BLADESTORE_MESSAGE_GET_FILE_INFO_PACKET_H

#include <stdint.h>
#include <string>
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "blade_packet.h"
#include "blade_common_data.h"
#include "blade_file_info.h"

using std::string;
using namespace bladestore::common;

namespace bladestore
{
namespace message
{
class GetFileInfoPacket : public BladePacket
{
public:
    GetFileInfoPacket();  // receiver
    explicit GetFileInfoPacket(int64_t parent_id, string file_name);  // sender
    ~GetFileInfoPacket();

    int Pack();
    int Unpack();
    int Reply(BladePacket * resp_packet);
    int64_t parent_id();
    string file_name();
private:
    int64_t parent_id_;
    string file_name_;
    size_t GetLocalVariableSize();
};  // end of class GetFileInfoPacket

class GetFileInfoReplyPacket : public BladePacket
{
public:
    GetFileInfoReplyPacket();  // receriver
    GetFileInfoReplyPacket(int16_t ret_code, int8_t t, int64_t id,
                           struct timeval mt, struct timeval ct,
                           struct timeval crt, int64_t c, int16_t n,
                           off_t file_size);  // sender
    ~GetFileInfoReplyPacket();

    int Pack();
    int Unpack();
    int16_t ret_code();
    FileInfo file_info();
    void set_ret_code(int16_t ret_code);
    void set_file_info(FileInfo &file_info);
private:
    int16_t ret_code_;
    FileInfo file_info_;
    size_t GetLocalVariableSize();
};  // end of class GetFileInfoReplyPacket
}  // end of namesapce message
}  // end of namespace bladestore

#endif
