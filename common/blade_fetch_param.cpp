#include <stdint.h>

#include "blade_fetch_param.h"
#include "blade_common_define.h"
#include "log.h"

using namespace pandora;

namespace bladestore
{
namespace common
{

int64_t BladeFetchParam::get_serialize_size(void) const
{
	return sizeof(*this);
}

int BladeFetchParam::serialize(char* buf, const int64_t buf_len, int64_t& pos) const
{
	int err = BLADE_SUCCESS;

	if (pos + (int64_t) sizeof(*this) > buf_len)
	{
		LOGV(LL_WARN, "buf is not enough, pos=%ld, buf_len=%ld, need_len=%ld", pos, buf_len, sizeof(*this));
		err = BLADE_ERROR;
	}
	else
	{
		*(reinterpret_cast<BladeFetchParam*> (buf + pos)) = *this;
		pos += sizeof(*this);
	}

	return err;
}

int BladeFetchParam::deserialize(const char* buf, const int64_t buf_len, int64_t& pos)
{
	int err = BLADE_SUCCESS;

	if (pos + (int64_t) sizeof(*this) > buf_len)
	{
		LOGV(LL_WARN, "buf is not enough, pos=%ld, buf_len=%ld, need_len=%ld", pos, buf_len, sizeof(*this));
		err = BLADE_ERROR;
	}
	else
	{
		*this = *(reinterpret_cast<const BladeFetchParam*> (buf + pos));
		pos += sizeof(*this);
	}

	return err;
}

}//end of namespace common
}//end of namespace bladestore
