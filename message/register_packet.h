/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: register_packet.h
 * Description: This file defines register packet.
 *
 * Version : 0.1
 * Author  : funing
 * Date    : 2012-4-5
 * Revised :
 */

#ifndef BLADESTORE_MESSAGE_REGISTER_PACKET_H
#define BLADESTORE_MESSAGE_REGISTER_PACKET_H

#include <stdint.h>
#include <string>
#include "blade_common_define.h"
#include "blade_packet.h"

using std::string;
using namespace bladestore::common;

namespace bladestore
{
namespace message
{
class RegisterPacket : public BladePacket
{
public:
    RegisterPacket();  // receiver
    RegisterPacket(int32_t , uint64_t);  // sender
    ~RegisterPacket();

    int Pack();
    int Unpack();
    int Reply(BladePacket * resp_packet);

    int32_t rack_id() {return rack_id_;}
    uint64_t ds_id()  {return ds_id_;}
private:
    int32_t rack_id_;
    uint64_t ds_id_;
    size_t GetLocalVariableSize();
};  // end of class RegisterPacket

class RegisterReplyPacket : public BladePacket
{
public:
    RegisterReplyPacket();  // receiver
    RegisterReplyPacket(int16_t ret_code);  // sender
    ~RegisterReplyPacket();

    int Pack();
    int Unpack();

    int16_t ret_code() {return ret_code_;}
    void set_ret_code(int16_t ret_code) {ret_code_ = ret_code;}
private:
    int16_t ret_code_;
    size_t GetLocalVariableSize();
};  // end of class RegisterReplyPacket

}  // end of namespace message
}  // end of namespace bladestore

#endif
