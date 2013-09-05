#include <assert.h>
#include "blade_net_processor.h"
#include "log.h"
namespace bladestore
{
namespace message
{

NetDataProcessor::NetDataProcessor()
{
	data_data_ = data_start_ = data_end_ = data_free_ = NULL;
}

NetDataProcessor::~NetDataProcessor()
{
	Destroy();
}

void NetDataProcessor::Destroy()
{
	data_start_ = data_end_ = data_free_ = data_data_  = NULL;
}

void NetDataProcessor::set_read_data(const unsigned char * data, int len)
{
	unsigned char * my_data = const_cast<unsigned char *>(data);
	data_start_ = data_data_ = my_data;
	data_end_ = data_free_ = my_data + len;
}

void NetDataProcessor::set_write_data(const unsigned char * data, int len)
{
	unsigned char * my_data = const_cast<unsigned char *>(data);
	data_start_ = data_free_ =  my_data;
	data_end_ = my_data + len;
}

char * NetDataProcessor::GetData()
{
	return (char *)data_start_;
}

int NetDataProcessor::GetDataLen()
{
	if(NULL == data_data_ || NULL == data_free_)
	{
		LOGV(MSG_ERROR, "data has not been initialized!!");
		return 0;
	}
	return data_free_ - data_data_;
}

char * NetDataProcessor::GetFree()
{
	return (char *)data_free_;
}

int NetDataProcessor::GetFreeLen()
{
	return data_end_ - data_free_;
}

bool NetDataProcessor::IsSpaceEnough(int size)
{
	return (data_end_ - data_free_) >= size;
}

bool NetDataProcessor::IsDataEnough(int size)
{
	return (data_free_ - data_data_) >= size;
}

bool NetDataProcessor::WriteInt8(const int8_t data)
{
	if(!IsSpaceEnough(1))
	{
		LOGV(MSG_ERROR, "space is not enough");
		return false;
	}
	*data_free_++ = data;	
	return true;
}

bool NetDataProcessor::WriteInt16(const int16_t data)
{
	if(!IsSpaceEnough(2))
	{
		LOGV(MSG_ERROR,"space is not enough");
		return false;
	}
	int16_t n = data;
	data_free_[1] = (unsigned char)n;
	n >>= 8;
	data_free_[0] = (unsigned char)n;
	data_free_ += 2;
	return true;
}

bool NetDataProcessor::WriteInt32(const int32_t data)
{
	if(!IsSpaceEnough(4))
	{
		LOGV(MSG_ERROR,"space is not enough");
		return false;
	}
	int32_t n = data;
	data_free_[3] = (unsigned char)n;
	n >>= 8;
	data_free_[2] = (unsigned char)n;
	n >>= 8;
	data_free_[1] = (unsigned char)n;
	n >>= 8;
	data_free_[0] = (unsigned char)n;
	data_free_ += 4;
	return true;
	
}

bool NetDataProcessor::WriteInt64(const int64_t data)
{
	if(!IsSpaceEnough(8))
	{
		LOGV(MSG_ERROR,"space is not enough");
		return false;
	}
	int64_t n = data;
	data_free_[7] = (unsigned char)n;
	n >>= 8;
	data_free_[6] = (unsigned char)n;
	n >>= 8;
	data_free_[5] = (unsigned char)n;
	n >>= 8;
	data_free_[4] = (unsigned char)n;
	n >>= 8;
	data_free_[3] = (unsigned char)n;
	n >>= 8;
	data_free_[2] = (unsigned char)n;
	n >>= 8;
	data_free_[1] = (unsigned char)n;
	n >>= 8;
	data_free_[0] = (unsigned char)n;
	data_free_ += 8;
	return true;
}

bool NetDataProcessor::WriteUint64(const uint64_t data)
{
	if(!IsSpaceEnough(8))
	{
		LOGV(MSG_ERROR,"space is not enough");
		return false;
	}
	uint64_t n = data;
	data_free_[7] = (unsigned char)n;
	n >>= 8;
	data_free_[6] = (unsigned char)n;
	n >>= 8;
	data_free_[5] = (unsigned char)n;
	n >>= 8;
	data_free_[4] = (unsigned char)n;
	n >>= 8;
	data_free_[3] = (unsigned char)n;
	n >>= 8;
	data_free_[2] = (unsigned char)n;
	n >>= 8;
	data_free_[1] = (unsigned char)n;
	n >>= 8;
	data_free_[0] = (unsigned char)n;
	data_free_ += 8;
	return true;
}

bool NetDataProcessor::WriteSize(const size_t data)
{
	if(8 == sizeof(size_t))
	{
		return WriteInt64(static_cast<int64_t>(data));
	}
	else if(4 == sizeof(size_t))
	{
		return WriteInt32(static_cast<int32_t>(data));
	}
	LOGV(MSG_ERROR, "cannt reach here , but it goes here");
	return false;
}

bool NetDataProcessor::GetSize(size_t & data)
{
	bool ret;
	if(8 == sizeof(size_t))
	{
		int64_t my_data = static_cast<int64_t>(data);
		ret = GetInt64(my_data);
		data = static_cast<size_t>(my_data);
		return ret;
	}
	else if(4 == sizeof(size_t))
	{
		int32_t my_data = static_cast<int32_t>(data);
		ret = GetInt32(my_data);
		data = static_cast<size_t>(my_data);
		return ret;
	}
	LOGV(MSG_ERROR, "cannt reach here , but it goes here");
	return false;
}

bool NetDataProcessor::GetInt8(int8_t & data)
{
	if(!IsDataEnough(1))
	{
		LOGV(MSG_ERROR, "data is not enough");
		return false;
	}
	data = (uint8_t)(* data_data_++);
	return true;
}

bool NetDataProcessor::GetInt16(int16_t & data)
{
	if(!IsDataEnough(2))
	{
		LOGV(MSG_ERROR, "data is not enough");
		return false;
	}
	uint16_t my_data = (unsigned char)data_data_[0];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[1];
	data_data_ += 2;
	data = my_data;
	return true;
}

bool NetDataProcessor::GetInt32(int32_t & data)
{
	if(!IsDataEnough(4))
	{
		LOGV(MSG_ERROR, "data is not enough");
		return false;
	}
	uint32_t my_data = (unsigned char)data_data_[0];
	my_data <<= 8;
	my_data |=(unsigned char) data_data_[1];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[2];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[3];
	data_data_ += 4;
	data = my_data;
	return true;
}

bool NetDataProcessor::GetInt64(int64_t & data)
{
	if(!IsDataEnough(8))
	{
		LOGV(MSG_ERROR, "data is not enough");
		return false;
	}
	uint64_t my_data = (unsigned char)data_data_[0];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[1];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[2];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[3];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[4];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[5];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[6];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[7];
	data_data_ += 8;
    data = my_data;
	return true;
}

bool NetDataProcessor::GetUint64(uint64_t & data)
{
	if(!IsDataEnough(8))
	{
		LOGV(MSG_ERROR, "data is not enough");
		return false;
	}
	uint64_t my_data = (unsigned char)data_data_[0];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[1];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[2];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[3];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[4];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[5];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[6];
	my_data <<= 8;
	my_data |= (unsigned char)data_data_[7];
	data_data_ += 8;
    data = my_data;
	return true;
}

bool NetDataProcessor::WriteString(const char * string)
{
	int len = (string?strlen(string):0);
	if(len > 0) 
		len++;
	if(!IsSpaceEnough(len + sizeof(int32_t)))
	{
		LOGV(MSG_ERROR, "space is not enough");
		return false;
	}
	WriteInt32(len);
	if(len > 0)
	{
		memcpy(data_free_, string, len - 1);
		data_free_[len -1] = '\0';
		data_free_ += len;
	}
	return true;
}

bool NetDataProcessor::WriteChars(const char * string,int64_t length)
{
	if(length < 0)
        return false;
	if(!IsSpaceEnough(length))
	{
		LOGV(MSG_ERROR, "space is not enough");
		return false;
	}
//	write_int32(len);
	memcpy(data_free_, string, length);
	data_free_ += length;
	return true;
}

bool NetDataProcessor::WriteBytes(const char * string,int64_t length)
{
	if(length < 0)
        return false;
	if(!IsSpaceEnough(length))
	{
		LOGV(MSG_ERROR, "space is not enough");
		return false;
	}
//	write_int32(len);
	memcpy(data_free_, string, length);
	data_free_ += length;
	return true;
}

bool NetDataProcessor::WriteString(const std::string & string)
{
	return	WriteString(string.c_str());
}

bool NetDataProcessor::GetString(char *& string)
{
	if (NULL != string) free(string);
	string = NULL;	
	if(data_data_ + sizeof(int32_t) > data_free_)
	{
		LOGV(MSG_ERROR, "data is not enough");
		return false;
	}
	int32_t slen = -1;
	GetInt32(slen);
	if(data_free_ - data_data_ < slen)
	{
		LOGV(MSG_ERROR, "data is not enough");
		return false;
	}
	if(slen > 0)
	{
		string = (char *)malloc(slen);
		memcpy(string, data_data_, slen);	
		string[slen - 1 ] = '\0';
	}
	else
	{
		string = NULL;
	}
	data_data_ += slen;
	assert(data_free_ >= data_data_);
	return true;
}

bool NetDataProcessor::GetChars(char * string, int64_t length)
{
	if (NULL == string || length < 0)
	{
		LOGV(MSG_ERROR, "string is NULL , invalid parameter");
		return false;
	}

	if(data_data_ + length > data_free_)
	{
		LOGV(MSG_ERROR, "data is not enough");
		return false;
	}

	memcpy(string, data_data_, length);	
	data_data_ += length;
	assert(data_free_ >= data_data_);
	return true;
}

bool NetDataProcessor::GetString(std::string & string)
{
	char * str = NULL;
	bool ret = GetString(str);
	if(!ret)
		return false;
	if(NULL != str)
	{
		string.assign(str);
		free(str);
	}
	else
	{
		string = "";
	}
	return true;
}

bool NetDataProcessor::ReadBytes(void * dst, int len)
{
	if(data_data_ + len > data_free_)
	{
		LOGV(MSG_ERROR, "data is not enough");
		return false;
	}
	memcpy(dst, data_data_, len);
	data_data_ += len;
	assert(data_free_ >= data_data_);
	return true;
}


}//end of namespace message
}//end of namespace bladestore
