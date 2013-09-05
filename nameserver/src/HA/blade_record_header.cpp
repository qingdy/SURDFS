#include "blade_record_header.h"
#include "blade_common_define.h"
#include "blade_crc.h"

using namespace bladestore::common;

namespace bladestore
{ 
namespace ha
{

void BladeRecordHeader::SetHeaderChecksum()
{ 
	header_checksum_ = 0;
	int16_t checksum = 0;

	FormatI64(magic_, checksum);
	checksum = checksum ^ header_length_;
	checksum = checksum ^ version_;
	checksum = checksum ^ header_checksum_;
	checksum = checksum ^ reserved_;
	FormatI32(data_length_, checksum);
	FormatI32(data_zlength_, checksum);
	FormatI32(data_checksum_, checksum);
	header_checksum_ = checksum;
}

int BladeRecordHeader::CheckHeaderChecksum() const
{ 
	int ret           = BLADE_ERROR;
	int16_t checksum  = 0;

	FormatI64(magic_, checksum);
	checksum = checksum ^ header_length_;
	checksum = checksum ^ version_;
	checksum = checksum ^ header_checksum_;
	checksum = checksum ^ reserved_;
	FormatI32(data_length_, checksum);
	FormatI32(data_zlength_, checksum);
	FormatI32(data_checksum_, checksum);
	if (0 == checksum)
	{
		ret = BLADE_SUCCESS;
	}

	return ret;
}

int BladeRecordHeader::CheckMagicNum(const int16_t magic) const
{ 
	int ret = BLADE_SUCCESS;
	if(magic_ != magic)
	{
		ret = BLADE_ERROR;
	}
	return ret;
}

int BladeRecordHeader::CheckCheckSum(const char *buf, const int64_t len) const
{ 
	int ret = BLADE_SUCCESS;

	/**
	 * for network package, maybe there is only one recorder header 
	 * without payload data, so the payload data lenth is 0, and 
	 * checksum is 0. we skip this case and return success 
	 */
	if (0 == len && (data_zlength_ != len || data_length_ != len || data_checksum_ != 0))
	{
		ret = BLADE_ERROR;
	}
	else 
	{
		if ((data_zlength_ != len) || (NULL == buf) || (len < 0))
		{
			ret = BLADE_ERROR;
		}

		if (BLADE_SUCCESS == ret)
		{ 
			int32_t crc_check_sum = Func::crc(0, buf, len);
			if (crc_check_sum !=  data_checksum_)
			{ 
				ret = BLADE_ERROR;
			}
		}
	}

	return ret;
}

int BladeRecordHeader::CheckRecord(const char *buf, const int64_t len, const int16_t magic)
{ 
	int ret = BLADE_SUCCESS;
	BladeRecordHeader record_header;
	int64_t pos = 0;
	ret = record_header.Deserialize(buf, len, pos);
	int record_len = len - pos;

	if ((BLADE_SUCCESS == ret) && (record_len > 0)
							&& (BLADE_SUCCESS == record_header.CheckHeaderChecksum())
							&& (BLADE_SUCCESS == record_header.CheckMagicNum(magic))
							&& (BLADE_SUCCESS == record_header.CheckDataZlength(record_len))
							&& (BLADE_SUCCESS == record_header.CheckCheckSum(buf + pos, record_len)))
	{
		ret = BLADE_SUCCESS; 
	}
	else
	{
		ret = BLADE_ERROR;
	}
	return ret;
}

int BladeRecordHeader::CheckRecord(const BladeRecordHeader &record_header, const char *payload_buf, const int64_t payload_len, const int16_t magic)
{ 
	int ret = BLADE_SUCCESS;

	if ((payload_len > 0) && (NULL != payload_buf) && (BLADE_SUCCESS == record_header.CheckHeaderChecksum()) && (BLADE_SUCCESS == record_header.CheckMagicNum(magic)) && (BLADE_SUCCESS == record_header.CheckDataZlength(payload_len)) && (BLADE_SUCCESS == record_header.CheckCheckSum(payload_buf, payload_len)))
	{
		ret = BLADE_SUCCESS; 
	}
	else
	{
		ret = BLADE_ERROR;
	}

	return ret;
}

int BladeRecordHeader::CheckRecord(const char* ptr, const int64_t size, 
                                 const int16_t magic, BladeRecordHeader& header, 
                                 const char*& payload_ptr, int64_t& payload_size)
{
	int ret             = BLADE_SUCCESS;
	int64_t payload_pos = 0;

	if (NULL == ptr || size < BLADE_RECORD_HEADER_LENGTH)
	{
		ret = BLADE_INVALID_ARGUMENT;
 	}
  
	if (BLADE_SUCCESS == ret)
	{
		ret = header.Deserialize(ptr, size, payload_pos);
	}

	if (BLADE_SUCCESS == ret)
	{
		payload_ptr = ptr + payload_pos;
		payload_size = size - payload_pos;
		if (header.header_length_ != payload_pos)
		{
			ret = BLADE_ERROR;
		}
		else
		{
			bool check = ((payload_size > 0)
                    && (BLADE_SUCCESS == header.CheckMagicNum(magic))
                    && (BLADE_SUCCESS == header.CheckDataZlength(payload_size))
                    && (BLADE_SUCCESS ==header.CheckHeaderChecksum())
                    && (BLADE_SUCCESS == header.CheckCheckSum(payload_ptr, payload_size))
                   );
			if (!check) ret = BLADE_ERROR;
		}
	}

	return ret;
}

int BladeRecordHeader::Serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{   
	int ret                 = BLADE_SUCCESS;
	int64_t serialize_size  = GetSerializeSize();

	if((NULL == buf) || (serialize_size + pos > buf_len)) 
	{   
		ret = BLADE_ERROR;
	}

	if (BLADE_SUCCESS == ret 
						&& (BLADE_SUCCESS == serialization::encode_i16(buf, buf_len, pos, magic_))
						&& (BLADE_SUCCESS == serialization::encode_i16(buf, buf_len, pos, header_length_))
						&& (BLADE_SUCCESS == serialization::encode_i16(buf, buf_len, pos, version_))
						&& (BLADE_SUCCESS == serialization::encode_i16(buf, buf_len, pos, header_checksum_))
						&& (BLADE_SUCCESS == serialization::encode_i64(buf, buf_len, pos, reserved_))
						&& (BLADE_SUCCESS == serialization::encode_i32(buf, buf_len, pos, data_length_))
						&& (BLADE_SUCCESS == serialization::encode_i32(buf, buf_len, pos, data_zlength_))
						&& (BLADE_SUCCESS == serialization::encode_i32(buf, buf_len, pos, data_checksum_)))
	{   
		ret = BLADE_SUCCESS;
	}   
	else
	{   
		ret = BLADE_ERROR;
	}   

	return ret;
}

int BladeRecordHeader::Deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
      int ret                 = BLADE_SUCCESS;
      int64_t serialize_size  = GetSerializeSize();

      if (NULL == buf || serialize_size > data_len)
      {
        ret = BLADE_ERROR;
      }

      if (BLADE_SUCCESS == ret
          && (BLADE_SUCCESS == serialization::decode_i16(buf, data_len, pos, &magic_))
          && (BLADE_SUCCESS == serialization::decode_i16(buf, data_len, pos, &header_length_))
          && (BLADE_SUCCESS == serialization::decode_i16(buf, data_len, pos, &version_))
          && (BLADE_SUCCESS == serialization::decode_i16(buf, data_len, pos, &header_checksum_))
          && (BLADE_SUCCESS == serialization::decode_i64(buf, data_len, pos, &reserved_))
          && (BLADE_SUCCESS == serialization::decode_i32(buf, data_len, pos, &data_length_))
          && (BLADE_SUCCESS == serialization::decode_i32(buf, data_len, pos, &data_zlength_))
          && (BLADE_SUCCESS == serialization::decode_i32(buf, data_len, pos, &data_checksum_)))
      {
        ret = BLADE_SUCCESS;
      }
      else
      {
        ret = BLADE_ERROR;
      }

      return ret;

}

int64_t  BladeRecordHeader::GetSerializeSize(void) const
{
      return (serialization::encoded_length_i16(magic_)
        + serialization::encoded_length_i16(header_length_)
        + serialization::encoded_length_i16(version_)
        + serialization::encoded_length_i16(header_checksum_)
        + serialization::encoded_length_i64(reserved_)
        + serialization::encoded_length_i32(data_length_)
        + serialization::encoded_length_i32(data_zlength_)
        + serialization::encoded_length_i32(data_checksum_));

}

}//end of namespace ha
}//end of namespace bladestore

