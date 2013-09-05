/*
 *version : 1.0
 *author  : chen yunyun, funing
 *date    : 2012-4-16
 *
 */
#ifndef BLADESTORE_NAMESERVER_LAYOUT_MANAGER_H
#define BLADESTORE_NAMESERVER_LAYOUT_MANAGER_H
#include <ext/hash_map>
#include <set>

#include "blade_common_define.h"
#include "blade_common_data.h"
#include "blade_dataserver_info.h"
#include "blade_block_collect.h"
#include "blade_rwlock.h"
#include "block_scanner.h"
#include "circular_fifo.h"
#include "heartbeat_packet.h"
#include "register_packet.h"
#include "blade_packet.h"

using namespace bladestore::common;
using namespace bladestore::message;

namespace bladestore
{
namespace nameserver
{

//store the dataserver information
typedef __gnu_cxx::hash_map<uint64_t, DataServerInfo *, __gnu_cxx::hash<uint64_t> > SERVER_MAP;
typedef __gnu_cxx::hash_map<uint64_t, DataServerInfo *, __gnu_cxx::hash<uint64_t> >::iterator SERVER_MAP_ITER;
//store the block information
typedef __gnu_cxx::hash_map<int64_t, BladeBlockCollect *, __gnu_cxx::hash<int64_t> > BLOCKS_MAP;
typedef __gnu_cxx::hash_map<int64_t, BladeBlockCollect *, __gnu_cxx::hash<int64_t> >::iterator BLOCKS_MAP_ITER;
//store the rack information
typedef __gnu_cxx::hash_map<int32_t, std::set<DataServerInfo *>, __gnu_cxx::hash<int32_t> > RACKS_MAP;
typedef __gnu_cxx::hash_map<int32_t, std::set<DataServerInfo *>, __gnu_cxx::hash<int32_t> >::iterator RACKS_MAP_ITER;

struct AliveDataServer
{
public:
	bool operator ()(const std::pair<uint64_t, DataServerInfo * > & node)
	{
		return node.second->is_alive();
	}
};

struct GetAliveDataServerList
{
public:
	GetAliveDataServerList(std::set<uint64_t> & ds_list) : ds_list_(ds_list)
	{
	
	}
	bool operator ()(const std::pair<int64_t, DataServerInfo *> & node)
	{
		if (node.second->is_alive())
		{
			ds_list_.insert(node.first);
			return true;
		}
		return false;
	}

private:
	std::set<uint64_t> & ds_list_;
};

//管理nameserver元数据
class BladeLayoutManager
{
public:
	BladeLayoutManager();
	~BladeLayoutManager();

	BladeBlockCollect *  get_block_collect(int64_t block_id); 
	DataServerInfo * get_dataserver_info(uint64_t ds_id);
	DataServerInfo * get_dataserver_info(uint64_t ds_id, bool & is_new);
	BladeBlockCollect  * create_block_collect(int64_t block_id);
	int64_t calc_all_block_bytes() const;

	//删除block
	void remove_block_collect(const int64_t block_id, bool is_master = true);

	bool remove_dataserver_info(const uint64_t ds_id);
	
	//做具体处理的一些函数
    	int register_ds(BladePacket *, BladePacket * &);
    	int update_ds(HeartbeatPacket *, HeartbeatReplyPacket * &);
    	int join_ds(DataServerInfo *& ds_stat_info, bool & is_new);
	int check_ds(const int64_t ds_dead_time, std::set<uint64_t> & ds_dead_list);
	int Foreach(BlockScanner &block_scanner);

	int get_alive_ds_size()
	{
		return std::count_if(server_map_.begin(), server_map_.end(), AliveDataServer());
	}
	//修改block的副本数量
	int change_replication_num(int64_t block_id, int16_t replicas_num);

	bool Insert(const BladeBlockCollect * block_collect, bool overwrite);
	bool build_block_ds_relation(int64_t block_id, uint64_t server_id );
	bool remove_block_ds_relation(int64_t block_id, uint64_t server_id, bool is_master = true);
	bool release_ds_relation(uint64_t ds_id, bool is_master = true);
	void clear_ds();
	
	CRWLock & get_server_mutex()
	{
		return server_mutex_;
	}

	CRWLock & get_blocks_mutex()
	{
		return blocks_mutex_;
	}

	CRWLock & get_racks_mutex()
	{
		return racks_mutex_;	
	}
	
	SERVER_MAP & get_server_map()
	{
		return server_map_;
	}

	BLOCKS_MAP & get_blocks_map()
	{
		return blocks_map_;
	}

	RACKS_MAP & get_racks_map()
	{
		return racks_map_; 
	}
	
	int32_t get_ds_size()
	{
		return alive_ds_size_;
	}

public:
	SERVER_MAP server_map_;
	BLOCKS_MAP blocks_map_;
	RACKS_MAP  racks_map_;

	//待检测的block列表, 无锁队列，注意支持的模型是（多生产者，单消费者）   
    CircularFIFO<int64_t, 20> block_to_check_;

	CRWLock  server_mutex_;
	CRWLock  blocks_mutex_;
	CRWLock  racks_mutex_;

private:
	int32_t alive_ds_size_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
