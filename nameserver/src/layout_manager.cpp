#include "layout_manager.h"
#include "blade_net_util.h"
#include "blade_dataserver_info.h"
#include "register_packet.h"

namespace bladestore
{
namespace nameserver
{

BladeLayoutManager::BladeLayoutManager() : alive_ds_size_(0) 
{
}

BladeLayoutManager::~BladeLayoutManager()
{
	clear_ds();
}

//@notice layout_manager is responsable for allocate and deallocate DataServerInfo 
void BladeLayoutManager::clear_ds()
{
	SERVER_MAP_ITER iter = server_map_.begin();
	for (; iter != server_map_.end(); iter++)
	{
		delete iter->second;
	}
	server_map_.clear();
}

int32_t BladeLayoutManager::register_ds(BladePacket *bpacket, BladePacket * &breply)
{
    RegisterPacket * packet = static_cast<RegisterPacket *>(bpacket);
    int32_t rack_id = packet->rack_id();
    uint64_t ds_id = packet->ds_id();
	DataServerInfo *ds_info = NULL;
	{
		CWLockGuard write_lock_guard(server_mutex_, true);
		SERVER_MAP_ITER iter = server_map_.find(ds_id);
		if (iter == server_map_.end())
		{
			ds_info = new DataServerInfo(rack_id, ds_id);
            if (NULL == ds_info)
            {
                return BLADE_ERROR;
            }
			ds_info->Reset();
			ds_info->set_is_alive(true);
			ds_info->last_update_time_ = TimeUtil::GetTime();

			server_map_.insert(SERVER_MAP::value_type(ds_id, ds_info));
			++alive_ds_size_;
		}
		else if (!iter->second->is_alive()) 
		{
			iter->second->set_is_alive(true);
			ds_info = iter->second;
			ds_info->last_update_time_ = TimeUtil::GetTime();
			++alive_ds_size_;
		}
		else
		{
			LOGV(LL_INFO, "ds_id:%d is in server map and alived!", ds_id);
			return BLADE_SUCCESS;
		}
	}

	{
		CWLockGuard write_lock_guard(racks_mutex_, true);
		RACKS_MAP_ITER rack_iter = racks_map_.find(rack_id);
		if (rack_iter == racks_map_.end())
		{
			set<DataServerInfo *> temp;
			temp.insert(ds_info);
			racks_map_.insert(RACKS_MAP::value_type(rack_id, temp));
            LOGV(LL_DEBUG, "New Rack! rackid : %d, ds_id : %ld", rack_id, ds_id);
		}
		else
		{
            LOGV(LL_DEBUG, "rack id : %d, size : %ld", rack_id, (rack_iter->second).size());
			(rack_iter->second).insert(ds_info);
            LOGV(LL_DEBUG, "Rack exist! rack_id : %d, ds_id : %ld, size : %ld", rack_id, ds_id, (rack_iter->second).size());
		}
	}

    LOGV(LL_INFO, "Register success, %d, alive_ds:%d", ds_id, alive_ds_size_);
    return BLADE_SUCCESS;
}

int32_t BladeLayoutManager::update_ds(HeartbeatPacket *packet, HeartbeatReplyPacket * &reply)
{
    uint64_t ds_id = packet->ds_id();

    CWLockGuard write_lock_guard(server_mutex_, true);    
    SERVER_MAP_ITER iter = server_map_.find(ds_id);
    if (iter == server_map_.end() || !iter->second->is_alive_)
    {
		LOGV(LL_INFO, "dataserver : (%s) not found", BladeNetUtil::AddrToString(ds_id).c_str());				
        return BLADE_ERROR;
    }
    
    iter->second->update_heartbeat(packet->ds_metrics());
    LOGV(LL_DEBUG, "Update dsid:%s, cpuload:%d", BladeNetUtil::AddrToString(ds_id).c_str(), 
            iter->second->dataserver_metrics_.cpu_load_);

	LOGV(LL_DEBUG, "BLOCK TO REMOVE SIZE : %d", iter->second->blocks_to_remove_.size());
    reply->set_blocks_to_remove(iter->second->blocks_to_remove_);
	LOGV(LL_DEBUG, "BLOCK TO REPLICATE SIZE : %d", iter->second->blocks_to_replicate_.size());
    reply->set_blocks_to_replicate(iter->second->blocks_to_replicate_);
    
    iter->second->clear_reply_blocks();
    return BLADE_SUCCESS;   
}

//@notice  锁上在内部
void BladeLayoutManager::remove_block_collect(int64_t block_id, bool is_master)
{
	BladeBlockCollect * block_collect = NULL;
	set<uint64_t> ds_list;

	//处理BLOCKS_MAP
	{
		CRLockGuard read_lock_guard(blocks_mutex_, true);	
		BLOCKS_MAP_ITER  blocks_map_iter = blocks_map_.find(block_id);
		if (blocks_map_iter == blocks_map_.end())
		{
			LOGV(LL_ERROR, "block_: %d not found", block_id);	
			return ;
		}
		block_collect = blocks_map_iter->second;
		ds_list =  block_collect->dataserver_set_;	
	}

	//处理SERVER_MAP
	{
		CWLockGuard write_lock_guard(server_mutex_, true);	
		SERVER_MAP_ITER server_map_iter;
		DataServerInfo * dataserver_info;
		set<uint64_t>::iterator ds_list_iter = ds_list.begin();
		for ( ; ds_list_iter != ds_list.end(); ds_list_iter++)
		{
			server_map_iter = server_map_.find(*ds_list_iter);	
			dataserver_info = server_map_iter->second;	
			(dataserver_info->blocks_hold_).erase(block_id);
			if (is_master)
			{
				(dataserver_info->blocks_to_remove_).insert(block_id);
			}
		}

	}

	//删除BladeBlockCollect
	{
		CWLockGuard write_lock_guard(blocks_mutex_, true);	
		BLOCKS_MAP_ITER  blocks_map_iter = blocks_map_.find(block_id);
		if (blocks_map_iter == blocks_map_.end())
		{
			LOGV(LL_ERROR, "block_: %d not found", block_id);	
			return ;
		}
        if (NULL != blocks_map_iter->second)
        {
            delete blocks_map_iter->second;
            blocks_map_iter->second = NULL;
        }
		blocks_map_.erase(blocks_map_iter);
	}
}

BladeBlockCollect * BladeLayoutManager::get_block_collect(int64_t block_id)
{
	BLOCKS_MAP_ITER iter = blocks_map_.find(block_id);
	if (iter != blocks_map_.end())
		return iter->second;
	return NULL;
}

DataServerInfo * BladeLayoutManager::get_dataserver_info(uint64_t ds_id)
{
	SERVER_MAP_ITER iter = server_map_.find(ds_id);
	if (server_map_.end() == iter || (false == iter->second->is_alive()))
		return NULL;
	return iter->second;
}

bool BladeLayoutManager::remove_dataserver_info(const uint64_t ds_id)
{
	SERVER_MAP_ITER iter = server_map_.find(ds_id);	
	if ((server_map_.end() != iter) && iter->second->is_alive())
	{
		iter->second->set_is_alive(false);
        iter->second->Reset();
        if (alive_ds_size_ > 0)
        {
		    alive_ds_size_ --;
        }
		return true;	
	}
	return false;
}

DataServerInfo * BladeLayoutManager::get_dataserver_info(uint64_t ds_id, bool & renew)
{
	renew = false;
	DataServerInfo * dataserver_info = NULL;
	SERVER_MAP_ITER iter = server_map_.find(ds_id);
	if (server_map_.end() == iter)
	{
		dataserver_info = new DataServerInfo();
		server_map_.insert(SERVER_MAP::value_type(ds_id, dataserver_info));
        ++alive_ds_size_;    
		renew = true;
		return dataserver_info;
	}
    LOGV(LL_DEBUG, "server_map size:%ld",server_map_.size());	
	dataserver_info = iter->second;
	if (dataserver_info->is_alive())
	{
		return dataserver_info;
	}
	
	renew = true;	
	dataserver_info->Reset();
	dataserver_info->dataserver_id_ = ds_id;
	return dataserver_info;
}

int BladeLayoutManager::check_ds(const int64_t ds_dead_time, std::set<uint64_t> & ds_dead_list)
{
	DataServerInfo * dataserver_info;
	ds_dead_list.clear();
	int64_t now = TimeUtil::GetTime() - ds_dead_time;
	{
		CRLockGuard read_lock_guard(server_mutex_, true);
		SERVER_MAP_ITER iter = server_map_.begin();
		for ( ; iter != server_map_.end(); iter++)
		{
			if (NULL == iter->second || !(iter->second->is_alive()))
			{
				continue;	
			}
			dataserver_info = iter->second;
			if (dataserver_info->last_update_time_ < now)
			{
				ds_dead_list.insert(dataserver_info->dataserver_id_);
				LOGV(LL_DEBUG, "In push ds_dead_list size : %d", ds_dead_list.size());
			}
		}
	}
	return ds_dead_list.size();
}

int BladeLayoutManager::Foreach(BlockScanner & block_scanner)
{
    int32_t ret = 0;
	int64_t block_id;	

	int block_to_check_size = (int)block_to_check_.Size(); 
	LOGV(LL_DEBUG, "BLOCK_TO_CHECK_SIZE : %d", block_to_check_.Size());

	for (int i = 0; i < block_to_check_size; i++)
	{
		block_to_check_.Pop(block_id);

		ret = block_scanner.Process(block_id);		

		if (SCANNER_NEXT == ret)
		{
			block_to_check_.Push(block_id);
		}
		else if (SCANNER_SKIP == ret)
		{
			return ret;	
		}
	}
	return ret;
}

int BladeLayoutManager::join_ds(DataServerInfo * & ds_stat_info, bool & is_new)
{
	DataServerInfo * dataserver_info = NULL; 
	{
		CWLockGuard write_lock_guard(server_mutex_, true);
		uint64_t server_id = ds_stat_info->dataserver_id_;
		dataserver_info = get_dataserver_info(server_id, is_new);
		LOGV(LL_DEBUG, "ds is :%d",is_new);    
		if (NULL == dataserver_info)
			return BLADE_ERROR;
		dataserver_info->dataserver_metrics_ = ds_stat_info->dataserver_metrics_;
        dataserver_info->load_score_ = ds_stat_info->load_score_;
        dataserver_info->dataserver_id_ = ds_stat_info->dataserver_id_;
        dataserver_info->rack_id_ = ds_stat_info->rack_id_;
        dataserver_info->is_alive_ = ds_stat_info->is_alive_;
        set<int64_t>::iterator iter = ds_stat_info->blocks_hold_.begin();
        for (; iter != ds_stat_info->blocks_hold_.end(); iter++)
        {
           dataserver_info->blocks_hold_.insert(*iter); 
        }
		dataserver_info->last_update_time_ = TimeUtil::GetTime() + 100000000;
	}

	if (is_new)
	{
		if (dataserver_info->is_alive_)
		{
			CWLockGuard write_lock_guard(racks_mutex_, true);
			RACKS_MAP_ITER rack_iter = racks_map_.find(ds_stat_info->rack_id_);
			if (racks_map_.end() == rack_iter)
			{
				set<DataServerInfo *> temp;
				temp.insert(dataserver_info);
				racks_map_.insert(RACKS_MAP::value_type(ds_stat_info->rack_id_, temp)); 
			}
			else
			{
				(rack_iter->second).insert(dataserver_info);
			}
			++alive_ds_size_;
		}
	}
	return BLADE_SUCCESS;
}

bool BladeLayoutManager::Insert(const BladeBlockCollect * block_collect, bool overwrite)
{
	CWLockGuard write_lock_guard(blocks_mutex_, true);
	if (NULL == block_collect)
	{
		return false;
	}
	int64_t block_id = block_collect->block_id_;	
	if (block_id <= 0)
	{
		return false;
	}
	BladeBlockCollect * block_collect_tmp = get_block_collect(block_id);
	if (NULL == block_collect_tmp || overwrite)
	{
		if (NULL != block_collect_tmp)
		{
			delete block_collect_tmp;
		}
		blocks_map_[block_id] = const_cast<BladeBlockCollect *>(block_collect);	
		return true;
	}
	//exist and dont allow overwrite
	return false;	
}

//@notice 锁上在内部
bool BladeLayoutManager::build_block_ds_relation(int64_t block_id, uint64_t ds_id)
{
	//a trick here, 加入的ds的DataServerInfo即使是ds挂掉了也不在列表中删除，只是简单的标记其状态为死亡状态
	DataServerInfo * dataserver_info = NULL;
	{
		CRLockGuard read_lock_guard(server_mutex_, true);
		SERVER_MAP_ITER iter = server_map_.find(ds_id);
		if ((server_map_.end() == iter) || (false == iter->second->is_alive()))
		{
			LOGV(MSG_ERROR, "dataserver : (%s) not found", BladeNetUtil::AddrToString(ds_id).c_str());				
			return false;
		}
		dataserver_info = iter->second;
	}
	{
		CWLockGuard write_lock_guard(blocks_mutex_, true);
		BladeBlockCollect * block_collect = NULL;
		BLOCKS_MAP_ITER iter = blocks_map_.find(block_id);
		if ((blocks_map_.end() == iter))
		{
			blocks_map_[block_id] = new BladeBlockCollect();
			block_collect = blocks_map_[block_id];
		}
		else
		{
			block_collect = iter->second;	
		}
		(block_collect->dataserver_set_).insert(ds_id);
	}
	{
		CWLockGuard write_lock_guard(server_mutex_, true);
		if ((NULL == dataserver_info) || (false == dataserver_info->is_alive()))
		{
			LOGV(MSG_ERROR, "dataserver : (%s) dead", BladeNetUtil::AddrToString(ds_id).c_str());				
			return false;
		}
		(dataserver_info->blocks_hold_).insert(block_id);
	}
	return true;
}

//@notice 锁上在内部
bool BladeLayoutManager::remove_block_ds_relation(int64_t block_id, uint64_t ds_id, bool is_master)
{
	DataServerInfo * dataserver_info = NULL;

	{
		CWLockGuard write_lock_guard(blocks_mutex_, true);
		BLOCKS_MAP_ITER blocks_map_iter;
		BladeBlockCollect * block_collect = NULL; 
		blocks_map_iter = blocks_map_.find(block_id); 
		if (blocks_map_.end() == blocks_map_iter)
		{
			LOGV(MSG_ERROR, "block collect : (%d) not found", block_id);
			return false;
		}
		block_collect = blocks_map_iter->second;
		(block_collect->dataserver_set_).erase(ds_id);
	}

	{
		CWLockGuard write_lock_guard(server_mutex_, true); 
		SERVER_MAP_ITER iter = server_map_.find(ds_id);
		if (server_map_.end() == iter)
		{
			LOGV(MSG_ERROR, "dataserver : (%s) not found", BladeNetUtil::AddrToString(ds_id).c_str());
			return false;
		}
		dataserver_info = iter->second;
		(dataserver_info->blocks_hold_).erase(block_id);
		if (is_master)
		{
			(dataserver_info->blocks_to_remove_).insert(block_id);
		}
	}
	return true;
}

//@notice 锁上在内部
bool BladeLayoutManager::release_ds_relation(uint64_t ds_id, bool is_master)
{
	DataServerInfo * dataserver_info = NULL;
	BladeBlockCollect * block_collect = NULL;
	BLOCKS_MAP_ITER blocks_map_iter;
	std::set<int64_t> blocks;
	{
		CRLockGuard read_lock_guard(server_mutex_, true);
		SERVER_MAP_ITER ds_iter = server_map_.find(ds_id);		
		if (server_map_.end() == ds_iter)
		{
			LOGV(MSG_ERROR, "dataserver : (%s) not found", BladeNetUtil::AddrToString(ds_id).c_str());
			return false;
		}
		blocks = ds_iter->second->blocks_hold_;
        dataserver_info = ds_iter->second;
	}

	std::set<int64_t>::const_iterator iter = blocks.begin();
	for (; iter != blocks.end(); iter++)
	{
		//加入待检测列表
		if (is_master)
		{
			block_to_check_.Push(*iter);	
		}

		CWLockGuard write_lock_guard(blocks_mutex_, true);
		blocks_map_iter = blocks_map_.find(*iter);	
		if (blocks_map_.end() == blocks_map_iter)
		{
			LOGV(MSG_ERROR, "block collect : (%d) not found", *iter);
			continue;
		}
		block_collect = blocks_map_iter->second;
		(block_collect->dataserver_set_).erase(ds_id);
	}

    {
	    CWLockGuard write_lock_guard(server_mutex_, true);	
	    dataserver_info->clear_all();
    }
    return true;
}

//@notice 锁上在内部
int BladeLayoutManager::change_replication_num(int64_t block_id, int16_t replicas_num)
{
	CWLockGuard write_lock_guard(blocks_mutex_, true);
	BladeBlockCollect * block_collect = NULL;
	BLOCKS_MAP_ITER block_iter = blocks_map_.find(block_id);	
	if (blocks_map_.end() == block_iter)
	{
		LOGV(MSG_ERROR, "block : (%d) not found", block_id);
		return BLADE_ERROR;
	}
	else
	{
		block_collect = block_iter->second;
		block_collect->replicas_number_ = replicas_num; 
		block_to_check_.Push(block_id);
	}
	return BLADE_SUCCESS;
}

}//end of namespace nameserver
}//end of namespace bladestore
