/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * Version : 0.1
 * Author  : funing
 * Date    : 2012-3-27
 * Revised :
 */
#ifndef BAD_BLOCK_REPORT_PACKET_H
#define BAD_BLOCK_REPORT_PACKET_H

#include <stdint.h>
#include <string>
#include <set>
#include "blade_packet.h"

namespace bladestore
{
namespace message
{
using std::string;
using std::set;

class BadBlockReportPacket : public BladePacket
{
public:
    BadBlockReportPacket();
    BadBlockReportPacket(uint64_t, set<int64_t>&);
    ~BadBlockReportPacket();
   
    int Pack();//sender
    int Unpack();//reciever

    uint64_t ds_id() {return ds_id_;}
    set<int64_t>& bad_blocks_id() {return bad_blocks_id_;}
private:
    uint64_t ds_id_;
    set<int64_t> bad_blocks_id_ ;
    size_t GetLocalVariableSize();
    DISALLOW_COPY_AND_ASSIGN(BadBlockReportPacket);

};

}//end of namespace message
}//end of namespace bladestore
#endif
