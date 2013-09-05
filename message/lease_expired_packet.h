/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 *
 * File   name: lease_expired_packet.h
 * Description: defined LeaseExpiredPacket class
 *
 * Version : 1.0
 * Author  : Yeweimin
 * Date    : 2012-6-8
 */

#ifndef BLADESTORE_MESSAGE_LEASE_EXPIRED_PACKET_H
#define BLADESTORE_MESSAGE_LEASE_EXPIRED_PACKET_H

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
class LeaseExpiredPacket : public BladePacket
{
public:
    LeaseExpiredPacket(); //receiver
    explicit LeaseExpiredPacket(int64_t file_id, int64_t block_id , int32_t block_version);
    ~LeaseExpiredPacket();

    int Pack();
    int Unpack();
    int64_t block_id();
    int64_t file_id();
    int32_t block_version();
	bool is_safe_write_;

private:
	int64_t file_id_;
    int64_t block_id_;
	int32_t block_version_;
    size_t GetLocalVariableSize();
}; //end of class 
} // end of namespace message 
} // end of namespace bladestore 
#endif
