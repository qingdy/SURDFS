/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * Version : 0.1
 * Author  : funing
 * Date    : 2012-3-27
 * Revised :
 */
#ifndef HEARTBEAT_PACKET_H
#define HEARTBEAT_PACKET_H

#include <stdint.h>
#include <string>
#include <set>
#include <map>
#include "bladestore_ops.h"
#include "blade_packet.h"
#include "blade_common_data.h"

namespace bladestore
{
namespace message
{
using std::string;
using std::map;
using std::set;
using namespace bladestore::common;

class HeartbeatPacket : public BladePacket
{
public:
    HeartbeatPacket();
    HeartbeatPacket(int32_t rack_id, uint64_t dsID, DataServerMetrics );
    ~HeartbeatPacket();
   
    virtual int Pack();//sender
    virtual int Unpack();//reciever
    int Reply(BladePacket *reply_packet);

    int32_t rack_id() {return rack_id_;}
    uint64_t ds_id() {return ds_id_;}
    DataServerMetrics ds_metrics() {return ds_metrics_;}

private:
    int32_t rack_id_;
    uint64_t ds_id_; //dataserver ID
    DataServerMetrics ds_metrics_;
    size_t GetLocalVariableSize();
}; //end of class HeartbeatPacket

class HeartbeatReplyPacket : public BladePacket
{
public:    
    HeartbeatReplyPacket();
    HeartbeatReplyPacket(int16_t ret_code);
    ~HeartbeatReplyPacket();
    
    virtual int Pack ();//sender
    virtual int Unpack ();//reciever
    void ClearAll();

    int16_t ret_code() {return ret_code_;}
    set<int64_t>& blocks_to_remove() {return blocks_to_remove_;}
    map<BlockInfo, set<uint64_t> >& blocks_to_replicate() {return blocks_to_replicate_;}
    void set_ret_code(int16_t ret_code) {ret_code_ = ret_code;}
    void set_blocks_to_remove(set<int64_t>& remove) {blocks_to_remove_ = remove;}
    void set_blocks_to_replicate(map<BlockInfo, set<uint64_t> >& replicate) {blocks_to_replicate_ = replicate;}

private:
    int16_t ret_code_;
    set<int64_t> blocks_to_remove_; //需要删除的错误块和无效块 
    map<BlockInfo, set<uint64_t> > blocks_to_replicate_;//需要复制的Block以及目的DS 
    size_t GetLocalVariableSize();
}; //end of class HeartbeatReplyPacket

}//end of namespace message
}//end of namespace bladestore
#endif
