/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_HA_LOG_ENTRY_H
#define BLADESTORE_HA_LOG_ENTRY_H

#include "blade_record_header.h"

using namespace bladestore::common;

namespace bladestore
{
namespace ha
{

extern const char* DEFAULT_CKPT_EXTENSION;

enum LogCommand
{
	BLADE_LOG_UNKNOWN = 0,

	//client->ns 1 - 200
	BLADE_LOG_CREATE = 1 ,
    BLADE_LOG_MKDIR = 2,
    BLADE_LOG_ADDBLOCK = 3,
    BLADE_LOG_COMPLETE = 4,
    BLADE_LOG_ABANDONBLOCK = 5,
    BLADE_LOG_RENAME = 6,
    BLADE_LOG_DELETEFILE = 7,
	
	//ds->ns  201 - 400 
    BLADE_LOG_REGISTER = 201,
    BLADE_LOG_BLOCK_REPORT = 202,
    BLADE_LOG_BAD_BLOCK_REPORT = 203,
    BLADE_LOG_BLOCK_RECEIVED = 204,

	//inner 401+
	BLADE_LOG_SWITCH_LOG = 401 ,
	BLADE_LOG_NOP = 402 ,
	BLADE_LOG_CHECKPOINT = 403,
    BLADE_LOG_EXPIRE_LEASE = 404,
    BLADE_LOG_LEAVE_DS = 405
};

/**
 * 一条日志项由四部分组成:
 *     BladeRecordHeader + 日志序号 + LogCommand + 日志内容
 * BladeLogEntry中保存BladeRecordHeader, 日志序号, LogCommand 三部分
 * BladeLogEntry中的data_checksum_项是对"日志序号", "LogCommand", "日志内容" 部分的校验
 */
struct BladeLogEntry
{
	BladeRecordHeader header_;
	uint64_t seq_;
	int32_t cmd_;

	static const int16_t MAGIC_NUMER = 0xAAAALL;
	static const int16_t LOG_VERSION = 1;

	BladeLogEntry()
	{
		memset(this, 0x00, sizeof(BladeLogEntry));
	}

	void SetLogSeq(const uint64_t seq) 
	{
		seq_ = seq;
	}
	void SetLogCommand(const int32_t cmd) 
	{
		cmd_ = cmd;
	}

	/**
	 * 调用该函数需要保证set_log_seq函数已经被调用
	 */
	int FillHeader(const char* log_data, const int64_t data_len);

	/**
	 * 计算日志序号+LogCommand+日志内容的校验和
  	 */
	int32_t CalcDataChecksum(const char* log_data, const int64_t data_len) const;

	/**
	 * After deserialization, this function get the length of log
	 */
	int32_t GetLogDataLen() const
	{
		return header_.data_length_ - sizeof(uint64_t) - sizeof(LogCommand);
	}

	int CheckHeaderIntegrity() const;

	/**
	 * 调用deserialization之后, 调用该函数检查数据正确性
	 */
	int CheckDataIntegrity(const char* log_data) const;

	static int GetHeaderSize() 
	{
		return sizeof(BladeRecordHeader) + sizeof(uint64_t) + sizeof(LogCommand);
	}
	
    //序列化和反序列化的方法
    int Serialize(char* buf, const int64_t buf_len, int64_t& pos) const; 
    int Deserialize(const char* buf, const int64_t data_len, int64_t& pos); 

    int64_t GetSerializeSize(void) const;	

};

}//end of namespace ha 
}//end of namespace bladestore

#endif 

