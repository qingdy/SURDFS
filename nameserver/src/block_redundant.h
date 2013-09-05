/*
 *version : 1.0
 *author  : chen yunyun, funing
 *date    : 2012-4-13
 *
 */
#ifndef BLADESTORE_NAMESERVER_REDUNDANT_H
#define BLADESTORE_NAMESERVER_REDUNDANT_H
#include <set>
#include <vector>
#include <algorithm>

#include "blade_meta_manager.h"
#include "blade_block_collect.h"
#include "blade_dataserver_info.h"
#include "block_scanner.h"
#include "block_scanner_manager.h"

#ifdef GTEST
#define private public
#define protected public
#endif

namespace bladestore
{
namespace nameserver
{

//管理block副本过多时的删除操作
class BlockRedundantLauncher : public Launcher
{
public:
	struct CompareBlockCount
	{
		bool operator ()(DataServerInfo * x, DataServerInfo * y)
		{
			return x->blocks_hold_.size() < y->blocks_hold_.size();
		}
	};

public:
	BlockRedundantLauncher(MetaManager & meta_manager);
	virtual ~BlockRedundantLauncher();

	int Check(int64_t block_id);
	int build_blocks(const std::set<int64_t> & block_list);

private:
	int check_redundant_block(int64_t block_id);
	int calc_max_capacity_ds(const std::set<uint64_t> & ds_list, int32_t count, std::set<uint64_t> & result_ds_list);

private:
	MetaManager & meta_manager_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
