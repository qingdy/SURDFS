/* copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: bladestore_ops.h
 * Description: This file defines bladestore oprations.
 * 
 * Version : 0.1
 * Author  : carloswjx
 * Date    : 2012-3-28
 *
 * Version : 
 * Author  : 
 * Date    :
 * Revised :
 */
#ifndef BLADESTORE_MESSAGE_BALDESTORE_OPS_H
#define BLADESTORE_MESSAGE_BALDESTORE_OPS_H

#include <stdint.h>

namespace bladestore 
{
namespace message
{
const int16_t OP_UNKNOWN = 0;
const int16_t RET_SUCCESS = 1;
//client - nameserver 1001 - 2000
const int16_t OP_GETLISTING = 1001;
const int16_t OP_ISVALIDPATH= 1002;
const int16_t OP_MKDIR = 1003;
const int16_t OP_GETBLOCKLOCATIONS = 1004;
const int16_t OP_ABANDONBLOCK = 1005;
const int16_t OP_ADDBLOCK = 1006;
const int16_t OP_CREATE = 1007;
const int16_t OP_GETFILEINFO = 1008;
const int16_t OP_RENAME = 1009;
const int16_t OP_DELETEFILE = 1010;
const int16_t OP_RENEWLEASE = 1011;
const int16_t OP_COMPLETE = 1012;
const int16_t OP_GETLISTING_REPLY = 1013;
const int16_t OP_ISVALIDPATH_REPLY = 1014;
const int16_t OP_MKDIR_REPLY = 1015;
const int16_t OP_GETBLOCKLOCATIONS_REPLY = 1016;
const int16_t OP_ABANDONBLOCK_REPLY = 1017;
const int16_t OP_ADDBLOCK_REPLY = 1018;
const int16_t OP_CREATE_REPLY = 1019;
const int16_t OP_GETFILEINFO_REPLY = 1020;
const int16_t OP_RENAME_REPLY = 1021;
const int16_t OP_DELETEFILE_REPLY = 1022;
const int16_t OP_RENEWLEASE_REPLY = 1023;
const int16_t OP_COMPLETE_REPLY = 1024;
const int16_t OP_STATUS = 1025;
const int16_t OP_STATUS_REPLY = 1026;
const int16_t OP_NS_LOG = 1027;
const int16_t OP_CHECKPOINT = 1028;
const int16_t OP_NS_SLAVE_REGISTER = 1029;
const int16_t OP_NS_SLAVE_REGISTER_REPLY = 1030;
const int16_t OP_LEASEEXPIRED = 1031;
const int16_t OP_LEAVE_DS= 1032;

//client - dataserver 2001 - 3000
const int16_t OP_READBLOCK = 2001;
const int16_t OP_WRITEPIPELINE = 2002;
const int16_t OP_WRITEPACKET = 2003;
const int16_t OP_READBLOCK_REPLY = 2004;
const int16_t OP_WRITEPIPELINE_REPLY = 2005;
const int16_t OP_WRITEPACKET_REPLY = 2006;
const int16_t OP_REPLICATEBLOCK = 2007;
const int16_t OP_REPLICATEBLOCK_REPLY = 2008;


//define dataserver ret code  4001-5000

//read logic ret code
const int16_t RET_READ_INIT= 4001;
//const int16_t RET_READ_BLOCK_INFO_SUCCESS = 4002;

//const int16_t RET_READ_PACKET_SUCCESS = 4003;
const int16_t RET_READ_ERROR = 4002;
const int16_t RET_BLOCK_NOT_EXIST_IN_DS = 4003;
const int16_t RET_BLOCK_OR_META_FILE_NOT_EXIST = 4004;
const int16_t RET_BLOCK_OR_META_FILE_OPEN_ERROR = 4005;
const int16_t RET_READ_VERSION_NOT_MATCH= 4006; 
const int16_t RET_READ_CHECKSUM_NOT_MATCH = 4007;
const int16_t RET_READ_REQUEST_INVALID = 4008;
const int16_t RET_READ_MEMMALLOC_ERROR = 4009;
const int16_t RET_READ_CRC_CHECK_ERROR = 4010;
const int16_t RET_READ_PREAD_ERROR = 4011;

//write logic ret code

const int16_t RET_BLOCK_FILE_EXISTS = 4012;
const int16_t RET_BLOCK_FILE_CREATE_ERROR = 4013;
const int16_t RET_PIPELINE_TARGET_NUM_NOT_MATCH = 4014;
const int16_t RET_PIPELINE_STATUS_NULL = 4015;
const int16_t RET_PIPELINE_VERSION_NOT_MATCH= 4016; 
const int16_t RET_PIPELINE_DATASERVER_ID_NOT_MATCH= 4017; 
const int16_t RET_PIPELINE_VERSION_NOT_INVALID= 4018; 

const int16_t RET_WRITE_PACKET_CHECKSUM_ERROR= 4022;
const int16_t RET_WRITE_PACKET_WRITE_FILE_ERROR= 4023;
//const int16_t RET_WRITE_PACKET_NEW_PACKET_ERROR= 4024;
const int16_t RET_WRITE_MEMMALLOC_ERROR = 4025;
const int16_t RET_WRITE_MODE_INVALID = 4026;
const int16_t RET_WRITE_FTRUNCATE_ERROR = 4027;
const int16_t RET_WRITE_COMPLETE_SUCCESS = 4028;
const int16_t RET_WRITE_ITEM_NOT_IN_MAP = 4029;
const int16_t RET_OPERATION_ON_GOING = 4100;//for writepipeline and replicate use


//nameserver - dataserver 3001-4000
const int16_t OP_REGISTER = 3001;
const int16_t OP_HEARTBEAT = 3002;
const int16_t OP_BLOCK_REPORT = 3003;
const int16_t OP_BLOCK_RECEIVED = 3004;
const int16_t OP_BAD_BLOCK_REPORT = 3005;
const int16_t OP_BLOCK_TO_GET_LENGTH = 3006;

const int16_t OP_REGISTER_REPLY = 3101;
const int16_t OP_HEARTBEAT_REPLY = 3102;
const int16_t OP_BLOCK_REPORT_REPLY = 3103;
const int16_t OP_BLOCK_RECEIVED_REPLY = 3104;
const int16_t OP_BAD_BLOCK_REPORT_REPLY = 3105;
const int16_t OP_BLOCK_TO_GET_LENGTH_REPLY = 3106;
const int16_t OP_GET_LENGTH = 3204;

//TODO dataserver ret code to nameserverme  6100-7000
const int16_t RET_GET_BLOCK_LENGTH_SUCCESS = 6001;
const int16_t RET_GET_BLOCK_LENGTH_BLOCK_NOT_EXIST = 6002;

//nameserver - slave 
const int16_t OP_NS_RENEW_LEASE = 3301;
const int16_t OP_NS_GRANT_LEASE = 3302;
const int16_t OP_NS_ADD_BLOCK = 3303;

//define namserver ret code 5001-6000
const int16_t RET_INVALID_PARAMS = 5001;
const int16_t RET_FILE_EXIST = 5002;
const int16_t RET_NOT_FILE = 5003;
const int16_t RET_INVALID_OFFSET = 5004;
const int16_t RET_GET_BLOCK_ALLOC_ERROR = 5005;
const int16_t RET_CREATE_FILE_ERROR = 5006;
const int16_t RET_ALOC_BLOCK_ERROR = 5007;
const int16_t RET_INVALID_BLOCKID = 5008;
const int16_t RET_MKDIR_ERROR = 5009;

const int16_t RET_LOOKUP_FILE_ERROR = 5010;
const int16_t RET_DIR_EXIST = 5011;
const int16_t RET_DIR_NOT_EXIST = 5012;
const int16_t RET_NOT_DIR = 5013;
const int16_t RET_INVALID_DIR_NAME = 5014;
const int16_t RET_GETALLOC_ERROR = 5015;
const int16_t RET_LAST_BLOCK_NO_COMPLETE = 5016;
const int16_t RET_LAYOUT_INSERT_ERROR = 5017;
const int16_t RET_FILE_NOT_EXIST = 5018;
const int16_t RET_BLOCK_NOT_EXIST = 5019;
const int16_t RET_NO_FILES = 5020;
const int16_t RET_NOT_PERMITTED = 5021;
const int16_t RET_DIR_NOT_EMPTY = 5022;
const int16_t RET_ERROR_FILE_TYPE = 5023;
const int16_t RET_DST_PATH_EXIST = 5024;
const int16_t RET_BLOCK_NO_LEASE = 5025;
const int16_t RET_ERROR = 5026;
const int16_t RET_FILE_COMPLETED = 5027;
const int16_t RET_NO_DATASERVER = 5028;

const int16_t RET_REGISTER_INVALID = 5102;
const int16_t RET_NEED_REGISTER = 5103;
const int16_t RET_NEED_OP = 5104;
}//end of namespace message
}//end of namespace bladestore
#endif
