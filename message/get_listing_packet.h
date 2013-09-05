/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: get_listing_packet.h
 * Description: This file defines GetListing packet.(example to other)
 * 
 * Version : 0.1
 * Author  : carloswjx
 * Date    : 2012-3-27
 *
 * Version : 0.2 
 * Author  : Yeweimin
 * Date    : 2012-3-28
 * Revised :
 */
#ifndef BLADESTORE_MESSAGE_GET_LISTING_PACKET_H
#define BLADESTORE_MESSAGE_GET_LISTING_PACKET_H

#include <stdint.h>
#include <string>
#include <vector>
#include "bladestore_ops.h"
#include "blade_packet.h"
#include "blade_common_define.h"
#include "blade_common_data.h"

using std::string;
using std::vector;
using namespace bladestore::common;

namespace bladestore
{
namespace message
{
class GetListingPacket : public BladePacket
{
public:
    GetListingPacket();
    explicit GetListingPacket(int64_t parent_id, string file_name);
    ~GetListingPacket();
    int Pack();  // sender
    int Unpack();  // reciever
    int Reply(BladePacket * resp_packet);
    int64_t parent_id();
    string file_name();
private:
    int64_t parent_id_;
    string file_name_;  // filename or pathname
    size_t GetLocalVariableSize();
};  // end of class GetListingPacket

class GetListingReplyPacket : public BladePacket
{
public:
    GetListingReplyPacket();
    GetListingReplyPacket(int16_t ret_code, const vector<string>& files);
    ~GetListingReplyPacket();
    int Pack();  // sender
    int Unpack();  // reciever
    int16_t ret_code();
    int32_t file_num();
    vector<string>& file_names();

    void set_ret_code(int16_t ret_code);
    void set_file_num(int32_t file_num);
    void set_file_names(vector<string> &file_names);
private:
    int16_t ret_code_;
    int32_t file_num_;
    vector<string> file_names_;
    size_t GetLocalVariableSize();
};  // end of class GetListingReplyPacket
}  // end of namespace message
}  // end of namespace bladestore
#endif
