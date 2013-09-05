/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *fun     : used by HA;
 */
#ifndef BLADESTORE_COMMON_FETCH_PARAM_H
#define BLADESTORE_COMMON_FETCH_PARAM_H

namespace bladestore
{
namespace common
{
/// Fetch线程需要获取的日志号范围和checkpoint号
/// 当一次checkpoint有多个文件时, ckpt_ext_数组描述这多个文件的后缀名
/// Fetch线程会将ckpt_id_下的多个不同后缀名的checkpoint文件都获取到
struct BladeFetchParam
{
	uint64_t min_log_id_;
	uint64_t max_log_id_;
	uint64_t ckpt_id_;
	bool fetch_log_; // whether to fetch log files
	bool fetch_ckpt_; // whether to fetch chekpoint files

	BladeFetchParam() : min_log_id_(0), max_log_id_(0), ckpt_id_(0), fetch_log_(false), fetch_ckpt_(false) 
	{

	}

	int64_t get_serialize_size(void) const;
	int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
	int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
};

}//end of namespace common
}//end of namespace bladestore

#endif
