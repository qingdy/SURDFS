/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: write_pipeline.h
 * Description: This file defines WritePipeline packet.
 * 
 * Version : 1.0
 * Author  : YYY
 * Date    : 2012-6-6
 *
 */

#ifndef BLADESTORE_MESSAGE_WRITE_PIPELINE_H
#define BLADESTORE_MESSAGE_WRITE_PIPELINE_H

#include <stdint.h>
#include <string>
#include <vector>

#include "blade_common_define.h"
#include "blade_packet.h"


namespace bladestore 
{
namespace message
{

using std::string;
using std::vector;
using namespace bladestore::common;

//--------------------------------start of declaration of WritePipelinePacket------------------------//
class WritePipelinePacket: public BladePacket
{
public:
    WritePipelinePacket();
    WritePipelinePacket(WritePipelinePacket &write_pipeline);
    WritePipelinePacket (int64_t file_id, int64_t block_id,
                         int32_t block_version, int8_t is_safe_write,
                         const vector<uint64_t> & ids);
    ~WritePipelinePacket();

    int Pack();
    int Unpack();
    size_t GetLocalVariableSize();

    int64_t file_id(){ return file_id_;}
    int64_t block_id(){ return block_id_;}
    int32_t block_version(){ return block_version_;}
    int8_t  is_safe_write(){ return is_safe_write_;}
    int8_t  target_num(){ return target_num_;}
    vector<uint64_t> dataserver_ids(){ return dataserver_ids_;}

private:
    int64_t file_id_;
    int64_t block_id_;
    int32_t block_version_;
    int8_t  is_safe_write_;//write:0 safe_write:1
    int8_t  target_num_;
    vector<uint64_t> dataserver_ids_; 

}; 


//--------------------------------start of declaration of WritePipelineReplyPacket------------------------//
class WritePipelineReplyPacket: public BladePacket
{
public:
    WritePipelineReplyPacket();
    WritePipelineReplyPacket(int64_t block_id, int32_t block_version,
                             int16_t ret_code);
    ~WritePipelineReplyPacket();

    int Pack();
    int Unpack();
    size_t GetLocalVariableSize();

    int64_t block_id(){ return block_id_;}
    int32_t block_version(){ return block_version_;}
    int16_t ret_code(){ return ret_code_;}
    int16_t get_ret_code(){ return ret_code_;}

private:
    int64_t block_id_;
    int32_t block_version_;
    int16_t ret_code_;

    DISALLOW_COPY_AND_ASSIGN(WritePipelineReplyPacket);
};

}//end of namespace message
}//end of namespace bladestore

#endif
