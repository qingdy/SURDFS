#include "block_redundant.h"
#include "blade_nameserver_param.h"
#include "blade_common_data.h"

namespace bladestore
{
namespace nameserver
{

BlockRedundantLauncher::BlockRedundantLauncher(MetaManager & meta_manager) : meta_manager_(meta_manager)
{

} 

BlockRedundantLauncher::~BlockRedundantLauncher()
{

}

int BlockRedundantLauncher::check_redundant_block(int64_t block_id)
{
	meta_manager_.get_layout_manager().blocks_mutex_.rlock()->lock();
	BLOCKS_MAP_ITER blocks_map_iter  = meta_manager_.get_layout_manager().blocks_map_.find(block_id);

	if (meta_manager_.get_layout_manager().blocks_map_.end() == blocks_map_iter)
	{
		meta_manager_.get_layout_manager().blocks_mutex_.rlock()->unlock();
		return SCANNER_NEXT;
	}

	BladeBlockCollect * block_collect = blocks_map_iter->second;
	std::set<uint64_t> & ds_list = block_collect->dataserver_set_;
	int16_t replicas_number = block_collect->replicas_number_;
	int count = ds_list.size() - replicas_number;


	if (count > 0)
	{
		LOGV(LL_DEBUG, "blockid :%d , expect replicas : %d , but real replicas :%d ! ", block_id, replicas_number, ds_list.size());
		std::set<uint64_t> result_ds_list;
		std::set<uint64_t>::iterator result_ds_iter;

		calc_max_capacity_ds(ds_list, count, result_ds_list);		
		meta_manager_.get_layout_manager().blocks_mutex_.rlock()->unlock();
		result_ds_iter = result_ds_list.begin();
		for (; (result_ds_iter != result_ds_list.end())&& (count > 0); result_ds_iter++, count--)
		{
			meta_manager_.get_layout_manager().remove_block_ds_relation(block_id, *result_ds_iter, meta_manager_.is_master());
		}
		return SCANNER_CONTINUE;
	}
	else if (0 == count)
	{
		meta_manager_.get_layout_manager().blocks_mutex_.rlock()->unlock();
		return SCANNER_CONTINUE;
	}

	meta_manager_.get_layout_manager().blocks_mutex_.rlock()->unlock();

	return SCANNER_NEXT;
}

int BlockRedundantLauncher::calc_max_capacity_ds(const std::set<uint64_t> & ds_list, int32_t count, std::set<uint64_t> & result_ds_list)
{
	std::vector<DataServerInfo* > middle_result;
	std::vector<DataServerInfo * > result;
	std::vector<DataServerInfo * >::iterator result_iter;
	std::set<uint64_t>::iterator ds_iter;
	DataServerInfo * ptr; 
	ds_iter = ds_list.begin();
	for ( ; ds_iter != ds_list.end(); ds_iter++) 
	{
		ptr = meta_manager_.get_layout_manager().get_dataserver_info(*ds_iter);
		//@fix bug according to gtest result
		if (NULL != ptr)
		{
			middle_result.push_back(ptr);
		}
	}

	std::sort(middle_result.begin(), middle_result.end(), CompareBlockCount());

	std::set<int32_t> racks;
	result_iter = middle_result.begin();
	for (; result_iter != middle_result.end(); result_iter++)
	{
		ptr = *result_iter; 
		if (racks.find(ptr->rack_id_) != racks.end())
		{
			result_ds_list.insert(ptr->dataserver_id_);
		}
		else
		{
			result.push_back(ptr);
			racks.insert(ptr->rack_id_);
		}
	}
	count -= result_ds_list.size();
	if (count <= 0)
		return count;

	result_iter = result.begin();	
	for(; (result_iter != result.end()) && (count > 0); result_iter++)
	{
		result_ds_list.insert((*result_iter)->dataserver_id_);
		count--;
	}
	return count;
}

int BlockRedundantLauncher::Check(int64_t block_id)
{
	return check_redundant_block(block_id);
}

int BlockRedundantLauncher::build_blocks(const std::set<int64_t > & block_list)
{
	return 0;
}

}//end of namespace nameserver
}//end of namespace bladestore
