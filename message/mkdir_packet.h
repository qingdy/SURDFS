/*
 * File   name: mkdir_packet.h
 * Description: This file defines Mkdir packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 * Revised :
 */

#ifndef BLADESTORE_MESSAGE_MKDIR_PACKET_H
#define BLADESTORE_MESSAGE_MKDIR_PACKET_H

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
class MkdirPacket : public BladePacket
{
public:
    MkdirPacket();  // receiver
    explicit MkdirPacket(int64_t parent_id, string file_name);  // sender
    ~MkdirPacket();

    int Pack();
    int Unpack();
    int Reply(BladePacket * resp_packet);
    int64_t parent_id();
    string file_name();
private:
    int64_t parent_id_; //file id of parent path
    string file_name_;  // filename or pathname
    size_t GetLocalVariableSize();
};  // end of class MkdirPacket

class MkdirReplyPacket : public BladePacket
{
public:
    MkdirReplyPacket();  // receiver
    explicit MkdirReplyPacket(int16_t ret_code, int64_t file_id);  // sender
    ~MkdirReplyPacket();

    int Pack();
    int Unpack();
    int16_t ret_code();
    int64_t file_id();
    void set_file_id(int64_t file_id);
    void set_ret_code(int16_t ret_code);
private:
    int16_t ret_code_;
    int64_t file_id_;
    size_t GetLocalVariableSize();
};  // end of class MkdirReplyPacket
}  // end of namespace message
}  // end of namespace bladestore

#endif
