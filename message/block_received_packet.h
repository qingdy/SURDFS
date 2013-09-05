/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * Version : 1.0
 * Author  : funing
 * Date    : 2012-3-30
 *
 * Version : 1.1
 * Author  : YYY 
 * Date    : 2012-6-20
 */

#ifndef BLOCK_RECEIVED_PACKET_H
#define BLOCK_RECEIVED_PACKET_H

#include <stdint.h>
#include <string>
#include "blade_packet.h"
#include "blade_common_define.h"

using std::string;
using namespace bladestore::common;

namespace bladestore
{
namespace message
{

class BlockReceivedPacket : public BladePacket
{
public:
    BlockReceivedPacket();
    BlockReceivedPacket(uint64_t, BlockInfo & b);
    ~BlockReceivedPacket();
   
    int Pack();//sender
    int Unpack();//reciever

    uint64_t ds_id() {return ds_id_;}
    BlockInfo& block_info() { return block_info_;}
private:
    uint64_t ds_id_;
    BlockInfo block_info_;
    size_t GetLocalVariableSize();
}; 

}//end of namespace message
}//end of namespace bladestore
#endif
