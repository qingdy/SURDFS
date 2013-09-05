/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: renew_lease_packet.h
 * Description: This file defines RenewLease packet.
 *
 * Version : 0.1
 * Author  : Yeweimin
 * Date    : 2012-4-6
 * Revised :
 */

#ifndef BLADESTORE_MESSAGE_RENEW_LEASE_PACKET_H
#define BLADESTORE_MESSAGE_RENEW_LEASE_PACKET_H

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
class RenewLeasePacket : public BladePacket
{
public:
    RenewLeasePacket();  // receiver
    explicit RenewLeasePacket(int64_t block_id);  // sender
    ~RenewLeasePacket();

    int Pack();
    int Unpack();
    int Reply(BladePacket * resp_packet);
    int64_t get_block_id();
private:
    int64_t block_id_;
    size_t GetLocalVariableSize();
};  // end of class RenewLeasePacket

class RenewLeaseReplyPacket : public BladePacket
{
public:
    RenewLeaseReplyPacket();  // receiver
    explicit RenewLeaseReplyPacket(int16_t ret_code);  // sender
    ~RenewLeaseReplyPacket();

    int Pack();
    int Unpack();
    int16_t get_ret_code();

    void set_ret_code(int16_t ret_code);
private:
    int16_t ret_code_;
    size_t GetLocalVariableSize();
};  // end of class RenewLeaseReplyPacket
}  // end of namespace message
}  // end of namespace bladestore
#endif
