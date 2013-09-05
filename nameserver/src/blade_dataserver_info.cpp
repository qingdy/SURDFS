#include "blade_dataserver_info.h"

namespace bladestore
{
namespace nameserver
{

DataServerInfo::DataServerInfo()
{
	rack_id_ = -1;
	load_score_= 0;
	last_update_time_ = 0;
	dataserver_id_ = 0;
	is_alive_ = false;
	Reset();
}

DataServerInfo::DataServerInfo(int32_t rackid,uint64_t dsID)
{
	load_score_ = 0;
	last_update_time_ = 0;
	rack_id_ = rackid;
	dataserver_id_= dsID;
	is_alive_ = false;
}
DataServerInfo::DataServerInfo(int32_t rackid,uint64_t dsID,DataServerMetrics ds_metrics)
{
	dataserver_metrics_ = ds_metrics;
	load_score_ = (dataserver_metrics_.used_space_ * 1.0) / dataserver_metrics_.total_space_;
	last_update_time_ = 0;
	rack_id_ = rackid;
	dataserver_id_= dsID;
	is_alive_ = false;
}
void DataServerInfo::clear_reply_blocks()
{
	blocks_to_remove_.clear();
	blocks_to_get_length_.clear();
	blocks_to_replicate_.clear();
}
void DataServerInfo::clear_all()
{
	blocks_to_remove_.clear();
	blocks_hold_.clear();
	blocks_to_get_length_.clear();
	blocks_to_replicate_.clear();
}

void DataServerInfo::Reset()
{
	dataserver_metrics_.num_connection_ = 0;
	load_score_ = 0;
	dataserver_metrics_.total_space_ = 0;
	dataserver_metrics_.used_space_ = 0;
	last_update_time_ = 0;
	dataserver_metrics_.cpu_load_ = 0;
	is_alive_ = false;
	blocks_to_remove_.clear();
	blocks_hold_.clear();
	blocks_to_get_length_.clear();
	map<BlockInfo, set<uint64_t> >::iterator iter = blocks_to_replicate_.begin();
	for (; iter != blocks_to_replicate_.end(); iter++)
	{
		
	}
	blocks_to_replicate_.clear();
}

void DataServerInfo::update_heartbeat(DataServerMetrics ds_metrics)
{
	dataserver_metrics_ = ds_metrics;
    load_score_ = (dataserver_metrics_.used_space_ * 1.0) / dataserver_metrics_.total_space_;
	last_update_time_ = TimeUtil::GetTime();
}

int DataServerInfo::get_number_of_blocks_to_replicate() 
{
    return blocks_to_replicate_.size();
}

int DataServerInfo::get_number_of_blocks_to_remove()
{
	return blocks_to_remove_.size();
}

int DataServerInfo::get_number_of_blocks_to_get_length()
{
	return blocks_to_get_length_.size();
}

set<int64_t> DataServerInfo::get_blocks_to_remove()
{
	return blocks_to_remove_;
}

map<BlockInfo, set<uint64_t> > DataServerInfo::get_blocks_to_replicate()
{
	return blocks_to_replicate_;
}

set<int64_t> DataServerInfo::get_blocks_to_get_length()
{
	return blocks_to_get_length_;
}

void DataServerInfo::add_blocks_to_remove(int64_t blockID)
{
	blocks_to_remove_.insert(blockID);
}

void DataServerInfo::add_blocks_to_replication(BlockInfo & b, uint64_t dsID)
{
	map<BlockInfo, set<uint64_t> >::iterator iter = blocks_to_replicate_.find(b);
	if (blocks_to_replicate_.end() == iter)
	{
		std::set<uint64_t> ds_list;
		ds_list.insert(dsID);
		blocks_to_replicate_[b] = ds_list;
	}
	else
	{
		blocks_to_replicate_[b].insert(dsID);
	}
}

void DataServerInfo::add_blocks_to_get_length(int64_t blockID)
{
	blocks_to_get_length_.insert(blockID);
}

}//end of namespace nameserver
}//end of namespace bladestore
