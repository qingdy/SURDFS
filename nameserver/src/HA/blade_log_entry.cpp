#include "blade_log_entry.h"
#include "utility.h"
#include "blade_crc.h"
#include "log.h"

using namespace pandora;

namespace bladestore
{
namespace ha
{

const char* DEFAULT_CKPT_EXTENSION = "checkpoint";

int BladeLogEntry::FillHeader(const char* log_data, const int64_t data_len)
{
	int ret = BLADE_SUCCESS;
	
	if ((NULL == log_data && data_len != 0) || (NULL != log_data && data_len <= 0))
	{
		ret = BLADE_INVALID_ARGUMENT;
	}
	else
	{
		header_.SetMagicNum(MAGIC_NUMER);
		header_.header_length_ = BLADE_RECORD_HEADER_LENGTH;
	    header_.version_ = LOG_VERSION;
	    header_.reserved_ = 0;
	    header_.data_length_ = sizeof(uint64_t) + sizeof(LogCommand) + data_len;
	    header_.data_zlength_ = header_.data_length_;
	    if (NULL != log_data)
	    {
			header_.data_checksum_ = CalcDataChecksum(log_data, data_len);
		}
		else
		{
			header_.data_checksum_ = 0;
		}
		header_.SetHeaderChecksum();
	}

	return ret;
}

int32_t BladeLogEntry::CalcDataChecksum(const char* log_data, const int64_t data_len) const
{
	uint32_t data_checksum = 0;

	if (data_len > 0)
	{
		data_checksum = Func::crc(data_checksum, reinterpret_cast<const char*>(&seq_), sizeof(seq_));
		data_checksum = Func::crc(data_checksum, reinterpret_cast<const char*>(&cmd_), sizeof(cmd_));
		data_checksum = Func::crc(data_checksum, reinterpret_cast<const char*>(log_data), data_len);
	}

	return static_cast<int32_t>(data_checksum);
}

int BladeLogEntry::CheckHeaderIntegrity() const
{
	int ret = BLADE_SUCCESS;
	if (BLADE_SUCCESS == header_.CheckHeaderChecksum() && BLADE_SUCCESS == header_.CheckMagicNum(MAGIC_NUMER))
	{
		ret = BLADE_SUCCESS;
	}
	else
	{
		LOGV(LL_WARN, "check_header_integrity error: ");
		hex_dump(&header_, sizeof(header_), true, LL_WARN);
	}

	return ret;
}

int BladeLogEntry::CheckDataIntegrity(const char* log_data) const
{
	int ret = BLADE_SUCCESS;

	if (NULL == log_data)
	{
		ret = BLADE_INVALID_ARGUMENT;
	}
	else
	{
		int64_t crc_check_sum = CalcDataChecksum(log_data, header_.data_length_ - sizeof(uint64_t) - sizeof(LogCommand));
		if ((BLADE_SUCCESS == header_.CheckHeaderChecksum()) && (BLADE_SUCCESS == header_.CheckMagicNum(MAGIC_NUMER)) && (crc_check_sum == header_.data_checksum_))
		{
			ret = BLADE_SUCCESS;
		}
		else
		{
			LOGV(LL_WARN, "Header: ");
			hex_dump(&header_, sizeof(header_), true, LL_WARN);
			LOGV(LL_WARN, "Body: ");
			hex_dump(log_data - sizeof(uint64_t) - sizeof(LogCommand), header_.data_length_, true, LL_WARN);
			ret = BLADE_ERROR;
		}
	}
	return ret;
}

int BladeLogEntry::Serialize(char* buf, const int64_t buf_len, int64_t& pos) const 
{
	int ret = BLADE_SUCCESS;
	int64_t serialize_size = GetSerializeSize();
	if (NULL == buf || (pos + serialize_size) > buf_len)
	{
		ret = BLADE_ERROR;
	}
	else
	{
		int64_t new_pos = pos;
		if ((BLADE_SUCCESS == header_.Serialize(buf, buf_len, new_pos))
					&& (BLADE_SUCCESS == serialization::encode_i64(buf, buf_len, new_pos, seq_))
					&& (BLADE_SUCCESS == serialization::encode_i32(buf, buf_len, new_pos, cmd_)))
		{
			pos = new_pos;
		}   
		else
		{   
			ret = BLADE_ERROR;
		}   
	}

	return ret;
}

int BladeLogEntry::Deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
	int ret = BLADE_SUCCESS;

	int64_t serialize_size = GetSerializeSize();
	if (NULL == buf || (serialize_size > data_len - pos))
	{
		ret = BLADE_ERROR;
	}
	else
	{
		int64_t new_pos = pos;
		if ((BLADE_SUCCESS == header_.Deserialize(buf, data_len, new_pos))
							&& (BLADE_SUCCESS == serialization::decode_i64(buf, data_len, new_pos, (int64_t*)&seq_))
							&& (BLADE_SUCCESS == serialization::decode_i32(buf, data_len, new_pos, (int32_t*)&cmd_)))
		{
			pos = new_pos;
		}
		else
		{
			ret = BLADE_ERROR;
		}
	}

	return ret;
}

int64_t BladeLogEntry::GetSerializeSize(void) const
{
	return header_.GetSerializeSize()
			+ serialization::encoded_length_i64(seq_)
			+ serialization::encoded_length_i32(cmd_);	
}

}//end of namespace ha
}//end of namespace bladestore
