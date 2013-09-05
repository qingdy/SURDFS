/*
 *version: 1.0
 *author : chenyunyun
 *date   : 2012-3-26
 */
#ifndef BLADESTORE_COMMON_BLADE_COMMON_DEFINE_H
#define BLADESTORE_COMMON_BLADE_COMMON_DEFINE_H
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

#include "log.h"

namespace 
{
const int CHECK_BLOCK_FLAG = 1;
const int CHECK_DS_FLAG = 2;

const int READ_THREAD_FLAG = 1;
const int WRITE_THREAD_FLAG = 2;
const int LOG_THREAD_FLAG = 3;
}

namespace bladestore
{
namespace common
{
//some macro define
//
#define __INT64_C(c) c ## L
#define __UINT64_C(c) c ## UL
#define  UINT64_MAX     (__UINT64_C(18446744073709551615))
#define  INT64_MAX ((__INT64_C(9223372036854775807))
#define UNUSED(arg) ((void)(arg))

#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&);  \
	void operator=(const TypeName&)	
#endif

// Bladestore Log Synchronization Type
const int64_t BLADE_LOG_NOSYNC = 0;
const int64_t BLADE_LOG_SYNC = 1;
const int64_t BLADE_LOG_DELAYED_SYNC = 2;

const int BLADE_SUCCESS = EXIT_SUCCESS;
const int BLADE_ERROR = EXIT_FAILURE;
const int BLADE_MAX_FILENAME_LENGTH = 1024;//max filename length 
const int BLADE_MAX_FETCH_CMD_LENGTH = 1024;
const int64_t BLADE_MAX_ROW_KEY_LENGTH = 16384; // 16KB
const int8_t BLADE_FILE_TYPE_UNKNOWN = 0;
const int8_t BLADE_FILE_TYPE_FILE = 1;
const int8_t BLADE_FILE_TYPE_DIR = 2;

//const used by nameserver
const int LEASE_RENEW_INTERVAL_USECS = 30 * 1000000;
const int LEASE_EXPIRE_WINDOW_USECS = 30 * 1000000; 
const int LEASE_CHECK_SLEEP_USECS = 30 * 1000000;

//const used by dataserver
const int64_t BLADE_BLOCK_SIZE = 64u << 20; //(64MB)
const int32_t BLADE_MAX_BLOCKS_PER_DIR = 64;
const int32_t BLADE_MAX_DIR_NUM = 512;
const int32_t BLADE_CHUNK_SIZE = 16u << 10; //(16KB)
const int32_t BLADE_CHECKSUM_SIZE = 4; //TODO 
const int32_t BLADE_METAFILE_SIZE = 16u << 10; //(16KB)

const int32_t BLADE_MAX_PACKET_LENGTH = 4u << 20; //(4MB)
const int32_t BLADE_MAX_PACKET_DATA_LENGTH = 1u << 20; //(1MB) TODO 需要考虑包头和校验码大小，这里先定义一个临时值

const int32_t DS_REPLICATE_SYNC_UNIT = 5; //num of packet send at one time in replicate

const int32_t BLADE_MAX_CONNECT_COUNT = 100; //TODO 
const int32_t BLADE_MAX_TASK_QUEUE_LENGTH = 1000; //TODO 

const int32_t BLADE_CLIENT_QUEUE_SIZE = 100; //TODO 
const int32_t BLADE_NAMESERVER_QUEUE_SIZE = 100; //TODO adjust
const int32_t BLOCKREPORT_TIMEOUT = 3000;
const int32_t NS_OP_TIMEOUT = 3000;
const int32_t REGISTER_TIMEOUT = 3000;

// time related const
const int32_t DS_PUSH_TIME = 1000; //minisecond
const int32_t DS_WRITE_TIME_OUT = 60*1000;//mimisecond
const int32_t DS_WRITE_PIPELINE_TIME_OUT = 60*1000;//minisecond
const int32_t DS_REPLICATE_TIME_OUT = 10*1000;//timeout for replication

//fs_interface use
const int32_t DS_CHECK_INTERVAL = 60*1000000;
const int32_t DS_BLOCK_EXPIRE_TIME = 210*1000000;
//const int32_t DS_CHECK_INTERVAL = 30*1000000;
//const int32_t DS_BLOCK_EXPIRE_TIME = 60*1000000;

const int8_t kWriteMode = 0;
const int8_t kSafeWriteMode = 1;
const int8_t kReplicateMode = 2;
const int8_t kReadMode = 3;
const int8_t kWriteCompleteMode = 4;
const int8_t kSafeWriteCompleteMode = 5;
const int8_t kReplicateCompleteMode = 6;
const int8_t kReadAfterWriteCompleteMode = 7;

const int32_t MAX_STORAGE_SPACE = 5000000;
const int32_t MAX_CONNECTION_NUM = 1000;
const int32_t DS_RECONNECT_NS_TIME = 10;//
const int32_t DS_REGISTER_WAIT = 10000;//minisecond
const int32_t HEARTBEAT_INTERVAL_SECS = 10;//can be moved to another place
const int64_t LAST_REPORT_INTERVAL = 2 *3600000000 ;// microsecond
//const int64_t LAST_REPORT_INTERVAL = 300000000;// 5 minutes ; microsecond

const uint32_t MAX_PATH_LENGTH = 1024; //used for create, mkdir, rename
const int32_t MAX_PATH_DEPTH = 20; //used for create, mkdir, rename


//!< Default lease interval of 5 mins
const int BLADE_LEASE_INTERVAL_SECS = 300;

const int BLADE_PREFETCHSIZE = BLADE_BLOCK_SIZE * 10; //(64MB)

const short int BLADE_MIN_REPLICAS_PER_FILE = 3; //default degree of replication
const short int BLADE_MAX_REPLICAS_PER_FILE = 10; //max. replicas per chunk of file
const int64_t ROOTFID = 2;

//const used by client 
const int64_t CLIENT_LEASE_SOFTLIMIT_PERIOD = 30;
const int64_t CLIENT_REQUEST_TO_NS_TIME_OUT = 60000;
const int64_t CLIENT_REQUEST_TO_DS_TIME_OUT = 60000;
const int64_t CLIENT_RW_REQUEST_TO_DS_TIME_OUT = 60000;
const int64_t CLIENT_LEASE_CHECK_SLEEP_USECS = 1000000;
const int32_t CLIENT_MAX_RESEND_TIMES = 3;
const int32_t CLIENT_LRU_REFRESH_WHEN_ERROR = 1;

const uint64_t CLIENT_MIN_BUFFERSIZE = 1024;        // 1k
const uint64_t CLIENT_MAX_BUFFERSIZE = 104857600;   // 100M
    
//error code -1 - -1000 for common
const int BLADE_TYPE_ERROR = -1; 
const int BLADE_INVALID_ARGUMENT = -2; 
const int BLADE_IO_ERROR = -3; 
const int BLADE_FILE_NOT_EXIST = -4;
const int BLADE_NOT_INIT = -5;
const int BLADE_MEM_OVERFLOW = -6;
const int BLADE_NEED_RETRY = -7;
const int BLADE_SIZE_OVERFLOW = -8;
const int BLADE_NETDATA_PACK_ERROR = -9;
const int BLADE_NETDATA_UNPACK_ERROR = -10;
const int BLADE_CONNECT_ERROR = -11;
const int BLADE_SENDMSG_ERROR = -12;
const int BLADE_RECVMSG_ERROR = -13;
const int BLADE_RESPONSE_TIME_OUT = -14;
const int BLADE_INIT_TWICE = -15;
const int BLADE_BUF_NOT_ENOUGH = -16;
const int BLADE_ALLOCATE_MEMORY_FAILED = -17;
const int BLADE_ALLOC_ERROR = -18;
const int BLADE_OPEN_FILE_ERROR = -19;
const int BLADE_READ_FILE_ERROR = -20;
const int BLADE_WRITE_FILE_ERROR = -21;
const int BLADE_FILE_FORMAT_ERROR = -22;
const int BLADE_FILE_OP_ERROR = -23;
const int BLADE_FILESYSTEM_ERROR = -24;

//error code -1001 - -2000 for client
const int CLIENT_OPEN_NOT_RDONLY = -1001;
const int CLIENT_CAN_NOT_CONNECT_DATASERVER = -1002;
const int CLIENT_INVALID_PATH= -1003;
const int CLIENT_OPEN_FILE_ERROR = -1004;
const int CLIENT_CREATE_INVALID_REPLICATION = -1005;
const int CLIENT_INVALID_FILENAME = -1006;
const int CLIENT_INVALID_FILEID = -1007;



//error code -2001 - -3000 for nameserver
const static int BLADE_NEED_REGISTER = -2001;
const static int BLADE_DISCONTINUOUS_LOG = -2002;
const static int BLADE_ENTRY_NOT_EXIST = -2003;
const static int BLADE_READ_NOTHING = -2004;
const static int BLADE_PARTIAL_FAILED = -2005;
const static int BLADE_GETALLOC_ERROR = -2006;



//error code -3001 - -4000 for dataserver
const int32_t BlADE_BLOCK_OR_META_FILE_NOT_EXIST = -3001;
const int32_t BLADE_BLOCK_OR_META_FILE_OPEN_ERROR = -3002;
const int32_t BLADE_MOLLOC_CHUCKSUM_ERROR = -3003;
const int32_t BLADE_READ_BLOCK_INFO_SUCCESS = -3004;
}//end of namespace common
}//end of namespace bladestore
#endif
