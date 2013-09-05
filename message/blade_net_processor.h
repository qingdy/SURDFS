/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-3-29
 *
 */
#ifndef BLADESTORE_MESSAGE_NET_DATA_PROCESSOR_H
#define BLADESTORE_MESSAGE_NET_DATA_PROCESSOR_H
#include <string>
namespace bladestore
{
namespace message
{
class NetDataProcessor
{
public:
	NetDataProcessor();
	~NetDataProcessor();
	void Destroy();

	void set_read_data(const unsigned char * data, int length);
	void set_write_data(const unsigned char * data, int length);
	char * GetData();
	int  GetDataLen();
	char * GetFree();
	int GetFreeLen();
	bool IsSpaceEnough(int size);
	bool IsDataEnough(int size);

	bool WriteInt8(const int8_t data);
	bool WriteInt16(const int16_t data);
	bool WriteInt32(const int32_t data);
	bool WriteInt64(const int64_t data);
	bool WriteUint64(const uint64_t data);
	bool WriteSize(const size_t data);

	bool GetInt8(int8_t & data);
	bool GetInt16(int16_t & data);
	bool GetInt32(int32_t & data);
	bool GetInt64(int64_t & data);
	bool GetUint64(uint64_t & data);
	bool GetSize(size_t & data);

	bool WriteString(const char *string);
	bool WriteString(const std::string & string);
	bool GetString(char *& string);
	bool WriteChars(const char * string,int64_t length);
	bool WriteBytes(const char * string,int64_t length);
	bool GetChars(char * string, int64_t length);
	bool GetString(std::string &string);
	bool ReadBytes(void * dst, int len);
private:	
	unsigned char * data_start_;
	unsigned char * data_end_;
	unsigned char * data_free_;
	unsigned char * data_data_;
};

}//end of namespace message
}//end of namespace bladestore
#endif
