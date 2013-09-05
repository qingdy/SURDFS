/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * Version : 1.0
 * Author  : funing
 * Date    : 2012-3-27
 *
 * Version : 1.1
 * Author  : YYY
 * Date    : 2012-6-12
 * Revised :
 */
#ifndef BLOCK_TO_GET_LENGTH_H
#define BLOCK_TO_GET_LENGTH_H

#include <stdint.h>
#include <string>

#include "blade_packet.h"
#include "blade_common_define.h"
//#include "blade_common_data.h"

namespace bladestore
{
namespace message
{
using namespace bladestore::common;
using namespace bladestore::message;

class BlockToGetLengthPacket : public BladePacket
{
public:
    BlockToGetLengthPacket();
    BlockToGetLengthPacket(int64_t block_id, int32_t block_version);
    ~BlockToGetLengthPacket();
   
    int    Pack();//sender
    int    Unpack();//reciever
    int    Reply(BladePacket *resp_packet);
    size_t GetLocalVariableSize();

    int64_t  block_id(){ return block_id_;}
    int32_t  block_version(){ return block_version_;}
	BlockToGetLengthPacket * CopySelf();

private:
    int64_t  block_id_; //block_id_ that needs to get length
    int32_t  block_version_;

    DISALLOW_COPY_AND_ASSIGN(BlockToGetLengthPacket);
};//end 


class BlockToGetLengthReplyPacket : public BladePacket
{
public:    
    BlockToGetLengthReplyPacket();
    BlockToGetLengthReplyPacket(int16_t ret_code, uint64_t ds_id,
                                int64_t block_id, int32_t block_version,
                                int64_t block_length);
    ~BlockToGetLengthReplyPacket();
    
    int    Pack();//sender
    int    Unpack();//reciever
    size_t GetLocalVariableSize();

    int16_t  ret_code(){return ret_code_;}
    uint64_t ds_id(){return ds_id_;}
    int64_t  block_id(){return block_id_;}
    int32_t  block_version(){return block_version_;}
    int64_t  block_length(){return block_length_;}
private:
    int16_t  ret_code_;
    uint64_t ds_id_;
    int64_t  block_id_;
    int32_t  block_version_;
    int64_t  block_length_;;

    DISALLOW_COPY_AND_ASSIGN(BlockToGetLengthReplyPacket);
}; //end

}//end of namespace message
}//end of namespace bladestore
#endif
