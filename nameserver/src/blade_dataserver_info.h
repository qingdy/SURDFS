/*
 *version : 1.0
 *author  : funing 
 *date    : 2012-4-10
 *
 */
#ifndef BLADESTORE_NAMESERVER_DATASERVERINFO_H
#define BLADESTORE_NAMESERVER_DATASERVERINFO_H
#include <stdint.h>
//#include <ext/hash_map>
#include <set>
#include <map>
#include <vector>
#include <time.h>

#include "blade_common_data.h"

using namespace std;
using namespace bladestore::common;
namespace bladestore
{
namespace nameserver
{

class DataServerInfo
{
public:
	DataServerInfo();
    DataServerInfo(int32_t ,uint64_t);
	DataServerInfo(int32_t rack_id, uint64_t ds_id, DataServerMetrics ds_metrics);
	~DataServerInfo()
	{

	}

	void update_heartbeat(DataServerMetrics ds);
	
	int get_number_of_blocks_to_replicate();
	int get_number_of_blocks_to_remove();
	int get_number_of_blocks_to_get_length();
	
	bool is_alive()
	{
		return is_alive_;
	}

    friend bool operator < (DataServerInfo left, DataServerInfo  right)
    {
        return (left.dataserver_id_ < right.dataserver_id_);
    }
    friend bool operator < (DataServerInfo & left, DataServerInfo & right)
    {
        return (left.dataserver_id_ < right.dataserver_id_);
    }
    bool operator < (DataServerInfo * right)
    {
        return (this->dataserver_id_ < right->dataserver_id_);
    }

	void set_is_alive(bool is_alive)
	{
		is_alive_ = is_alive;
	}

	void Reset();
	
	void clear_all();
    
    void clear_reply_blocks();

	set<int64_t> get_blocks_to_remove();
	map<BlockInfo, set<uint64_t> > get_blocks_to_replicate();
	set<int64_t> get_blocks_to_get_length();
	
	void add_blocks_to_remove(int64_t blockID);
	void add_blocks_to_replication(BlockInfo & b, uint64_t  dsID);
	void add_blocks_to_get_length(int64_t blockID);
    
    int64_t get_free_space()
    {
        return dataserver_metrics_.total_space_ - dataserver_metrics_.used_space_;
    }

public:	
	int32_t rack_id_;
	uint64_t dataserver_id_;
	
	int64_t last_update_time_ ;
    double load_score_;	
	bool is_alive_;
    DataServerMetrics dataserver_metrics_;

	set<int64_t> blocks_hold_;
	set<int64_t> blocks_to_remove_;
	map<BlockInfo, set<uint64_t> > blocks_to_replicate_;
	set<int64_t> blocks_to_get_length_;
};

//typedef Handle<DataServerInfo> DataServerInfoPtr;

}//end of namespace common
}//end of namespace bladestore
#endif 
