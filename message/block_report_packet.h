/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * Version : 0.1
 * Author  : funing
 * Date    : 2012-3-27
 * Revised :
 */
#ifndef BLOCK_REPORT_PACKET_H
#define BLOCK_REPORT_PACKET_H

#include <stdint.h>
#include <string>
#include <set>
#include "blade_packet.h"
#include "blade_common_define.h"

using std::string;
using std::set;
using namespace bladestore::common;

namespace bladestore
{
namespace message
{

class BlockReportPacket : public BladePacket
{
public:
    BlockReportPacket();
    BlockReportPacket(uint64_t, set<BlockInfo *>&);
    ~BlockReportPacket();
   
    virtual int Pack();//sender
    virtual int Unpack();//reciever
    int Reply(BladePacket *reply_packet);

    uint64_t ds_id() {return ds_id_;}
    set<BlockInfo *>& report_blocks_info() {return report_blocks_info_;}
private:
    uint64_t ds_id_;
    set<BlockInfo *> report_blocks_info_;
    size_t GetLocalVariableSize();
};


class BlockReportReplyPacket : public BladePacket
{
public:    
    BlockReportReplyPacket();
    BlockReportReplyPacket(int16_t ret_code);
    ~BlockReportReplyPacket();
    
    virtual int Pack ();//sender
    virtual int Unpack ();//reciever

    int16_t ret_code() {return ret_code_;}
    void set_ret_code(int16_t ret_code) {ret_code_ = ret_code;}
private:
    int16_t ret_code_;
    size_t GetLocalVariableSize();
};


}//end of namespace message
}//end of namespace bladestore
#endif
