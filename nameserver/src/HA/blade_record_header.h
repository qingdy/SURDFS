/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_HA_RECORD_HEADER_H
#define BLADESTORE_HA_RECORD_HEADER_H

#include "blade_serialization.h"

namespace bladestore
{
namespace ha
{

static const int16_t MAGIC_NUMER = 0xB0CC;
    
inline void FormatI64(const int64_t value, int16_t& check_sum)
{
	int i = 0;
	while (i < 4)
	{
		check_sum =  check_sum ^ ((value >> i * 16) & 0xFFFF);
		++i;
	}
}
    
inline void FormatI32(const int32_t value,int16_t& check_sum)
{
	int i = 0;
	while (i < 2)
	{
		check_sum =  check_sum ^ ((value >> i * 16) & 0xFFFF);
		++i;
	}
}
    
struct BladeRecordHeader
{ 
	int16_t magic_;          // magic number
	int16_t header_length_;  // header length
	int16_t version_;        // version
	int16_t header_checksum_;// header checksum
	int64_t reserved_;       // reserved,must be 0
	int32_t data_length_;    // length before compress
	int32_t data_zlength_;   // length after compress, if without compresssion 
                             // data_length_= data_zlength_
	int32_t data_checksum_;  // record checksum
  
	inline void SetMagicNum(const int16_t magic = MAGIC_NUMER)
	{
		magic_ = magic;
	}

	void SetHeaderChecksum();

	int CheckHeaderChecksum() const;

	int CheckMagicNum(const int16_t magic = MAGIC_NUMER) const; 

	inline int CheckDataZlength(const int32_t commpressed_len) const
	{ 
		int ret = BLADE_SUCCESS; 
		if(commpressed_len != data_zlength_)
		{
			ret = BLADE_ERROR;
		} 
		return ret;
	}

	inline int CheckDataLength(const int32_t data_len) const
	{
		int ret = BLADE_SUCCESS;
		if (data_len != data_length_)
		{
			ret = BLADE_ERROR;
		}
		return ret;
	}

	inline bool IsCompress() const
	{  
		bool ret = true;
		if (data_length_ == data_zlength_)
		{
			ret = false;
		}
		return ret;
	}

	int CheckCheckSum(const char *buf, const int64_t len) const;

	static int CheckRecord(const char *buf, const int64_t len, const int16_t magic);
	static int CheckRecord(const BladeRecordHeader &record_header, const char *payload_buf, 
                            const int64_t payload_len, const int16_t magic);
	static int CheckRecord(const char* ptr, const int64_t size, const int16_t magic, 
                            BladeRecordHeader& header, const char*& payload_ptr, int64_t& payload_size);

	//序列化和反序列化的方法
	int Serialize(char* buf, const int64_t buf_len, int64_t& pos) const; 
	int Deserialize(const char* buf, const int64_t data_len, int64_t& pos);	

	int64_t GetSerializeSize(void) const;

};

static const int BLADE_RECORD_HEADER_LENGTH = sizeof(BladeRecordHeader);

}// end of namespace ha
}// end of namespace bladestore

#endif
