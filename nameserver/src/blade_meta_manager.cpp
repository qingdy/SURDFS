#include "blade_meta_manager.h"
#include "nameserver_impl.h"
#include "lease_manager.h"
#include "blade_rwlock.h"
#include "btree_checkpoint.h"
#include "file_system_image.h"
#include "blade_ns_log_manager.h"
#include "blade_log_entry.h"
#include "blade_ns_log_worker.h"
#include "blade_net_util.h"
#include "socket_context.h"
#include "utility.h"
#include "types.h"
#include "replicate_strategy.h"
#include "create_packet.h"
#include "mkdir_packet.h"
#include "add_block_packet.h"
#include "complete_packet.h"
#include "get_block_locations_packet.h"
#include "is_valid_path_packet.h"
#include "get_file_info_packet.h"
#include "get_listing_packet.h"
#include "abandon_block_packet.h"
#include "rename_packet.h"
#include "delete_file_packet.h"
#include "renew_lease_packet.h"
#include "bad_block_report_packet.h"
#include "block_report_packet.h"
#include "lease_expired_packet.h"
#include "register_packet.h"
#include "block_received_packet.h"
#include "leave_ds_packet.h"
#include "stat_manager.h"

using namespace bladestore::common;
using namespace pandora;
using namespace bladestore::ha;
using namespace bladestore::btree;

namespace bladestore
{
namespace nameserver
{

MetaManager::MetaManager(NameServerImpl * nameserver) :  nameserver_(nameserver), layout_manager_(), meta_tree_(layout_manager_)
{
	btree_checkpoint_ = NULL;
	fs_image_ = NULL;
}

MetaManager::~MetaManager()
{
	if (NULL != btree_checkpoint_)
	{
		delete btree_checkpoint_;
	}
	btree_checkpoint_ = NULL;

	if (NULL != fs_image_)
	{
		delete fs_image_;
	}
	fs_image_ = NULL;
}

void MetaManager::Init()
{
	btree_checkpoint_ = new BtreeCheckPoint(meta_tree_);
	fs_image_ = new FileSystemImage(&layout_manager_);
	fs_image_->set_lease_manager(&(nameserver_->get_lease_manager()));
}

FileSystemImage * MetaManager::get_fs_image()
{
	return fs_image_;
}

BtreeCheckPoint * MetaManager::get_btree_checkpoint()
{
	return btree_checkpoint_;
}

int32_t MetaManager::register_ds(BladePacket *packet, BladePacket * &reply)
{
    LOGV(LL_INFO, "***************************in register*******************");
    if (false != is_master())
    {
        packet->Pack();
        int ret = nameserver_->get_blade_ns_log_manager()->GetLogWorker()->BladeRegister(BLADE_LOG_REGISTER, reinterpret_cast <char *>(packet->content()), packet->length());
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "HA ERROR");
			return ret;
		}
    }
    return layout_manager_.register_ds(packet, reply);
}

int32_t MetaManager::update_ds(HeartbeatPacket *packet, HeartbeatReplyPacket * &reply)
{
    return layout_manager_.update_ds(packet, reply);
}

int MetaManager::join_ds(DataServerInfo * dataserver_info, bool & is_new)
{
	//layout_manager_.join_ds 内部持有锁, 主要是dataserverinfo的更新操作
	int ret = layout_manager_.join_ds(dataserver_info, is_new);	
	if (BLADE_SUCCESS != ret)
	{
		LOGV(LL_ERROR, "ds : (%s) join failed", BladeNetUtil::AddrToString(dataserver_info->dataserver_id_).c_str());
		return ret;
	}
	return BLADE_SUCCESS;
}


//dataserver超时退出
int MetaManager::leave_ds(BladePacket * blade_packet)
{
	if (NULL == blade_packet)
	{
		return BLADE_ERROR;
	}
	
	LeaveDsPacket * leave_ds_packet = static_cast<LeaveDsPacket *>(blade_packet);
	if (NULL == leave_ds_packet)
	{
		return BLADE_ERROR;
	}

    if (false != is_master())
    {
		leave_ds_packet->Pack();
        int ret = nameserver_->get_blade_ns_log_manager()->GetLogWorker()->BladeLeaveDs(BLADE_LOG_LEAVE_DS, reinterpret_cast <char *>(leave_ds_packet->content()), leave_ds_packet->length());
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "HA ERROR");
			return ret;
		}
    }

	uint64_t ds_id = leave_ds_packet->get_ds_id();
	//release_ds_relation内部持有锁
	LOGV(LL_DEBUG,"In leave ds, dsid: %s ", BladeNetUtil::AddrToString(ds_id).c_str());
    layout_manager_.release_ds_relation(ds_id, is_master());

	{
		CWLockGuard scoped_write_lock(layout_manager_.server_mutex_, true);
		//@notice 不删除,只是做一个死亡标记, 同时删除rack的相关信息
		layout_manager_.remove_dataserver_info(ds_id);	
	}

	//删除ds_id在rack中的信息
	{
		DataServerInfo * dataserver_info = NULL;	
		layout_manager_.server_mutex_.rlock()->lock(); 
		__gnu_cxx::hash_map<uint64_t, DataServerInfo *>::iterator server_iter = layout_manager_.server_map_.find(ds_id);
		if (layout_manager_.server_map_.end() == server_iter)
		{
			layout_manager_.server_mutex_.rlock()->unlock(); 
			LOGV(LL_ERROR, "dataserver : (%s) not found", BladeNetUtil::AddrToString(ds_id).c_str());
			return BLADE_ERROR;
		}
		dataserver_info = server_iter->second;
		layout_manager_.server_mutex_.rlock()->unlock(); 
		
		layout_manager_.racks_mutex_.wlock()->lock();
		__gnu_cxx::hash_map<int32_t, set<DataServerInfo * > >::iterator rack_iter =  layout_manager_.racks_map_.find(dataserver_info->rack_id_);

		if (layout_manager_.racks_map_.end() ==  rack_iter)
		{
			layout_manager_.racks_mutex_.wlock()->lock();
			LOGV(LL_ERROR, "rack : (%s) not found", dataserver_info->rack_id_);	
			return BLADE_ERROR;
		}

		std::set<DataServerInfo *>::iterator ds_iter = find_if((rack_iter->second).begin(), (rack_iter->second).end(), ServerMatch(ds_id));
		if (ds_iter != (rack_iter->second).end())
		{
			(rack_iter->second).erase(ds_iter);
		}

		layout_manager_.racks_mutex_.wlock()->unlock();		
	}

	return BLADE_SUCCESS;
}

int MetaManager::add_replicate_info(int64_t block_id, int32_t version, uint64_t source_id, uint64_t dest_id)
{
	int ret = BLADE_SUCCESS;
	DataServerInfo * dataserver_info = NULL;
	layout_manager_.server_mutex_.wlock()->lock();
	SERVER_MAP_ITER iter = layout_manager_.server_map_.find(source_id);	
	if (layout_manager_.server_map_.end() == iter)
	{
		ret = BLADE_ERROR;
		layout_manager_.server_mutex_.wlock()->unlock();
	}
	else
	{
		dataserver_info = iter->second;	
	}

	if (BLADE_SUCCESS == ret)
	{
		BlockInfo block;	
        block.set_block_id(block_id);
        block.set_block_version(version);
		dataserver_info->add_blocks_to_replication(block, dest_id);
		LOGV(LL_DEBUG, "add replicate block_id : %ld , dest id : %ld", block_id, dest_id);
		layout_manager_.server_mutex_.wlock()->unlock();
	}
	return ret;
}

int32_t MetaManager::report_bad_block(BladePacket *packet)
{
    LOGV(LL_DEBUG,"In report bad blocks ");
    int ret = BLADE_ERROR;
    BadBlockReportPacket * badpacket = static_cast<BadBlockReportPacket *>(packet);
    uint64_t ds_id = badpacket->ds_id();
    set<int64_t> & blocks = badpacket->bad_blocks_id();
    LOGV(LL_DEBUG, "\ndsid: %s  , report blocks num: %d\n", BladeNetUtil::AddrToString(ds_id).c_str(), blocks.size());
    
    if (false != is_master())
    {
        packet->Pack();
        int ret = nameserver_->get_blade_ns_log_manager()->GetLogWorker()->BladeBadBlockReport(BLADE_LOG_BAD_BLOCK_REPORT, reinterpret_cast <char *>(packet->content()), packet->length());
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "HA ERROR");
			return ret;
		}
    }

    set<int64_t>::iterator baditer;
    {
		CWLockGuard write_lock_guard(layout_manager_.server_mutex_, true);
		SERVER_MAP_ITER dsiter = layout_manager_.server_map_.find(ds_id);
		if ((layout_manager_.server_map_.end() == dsiter) || (false == dsiter->second->is_alive()))
		{
			LOGV(LL_ERROR, "In bad blocks, dataserver : (%s) not found or expire, need do regist fitst!", BladeNetUtil::AddrToString(ds_id).c_str());
			ret = BLADE_NEED_REGISTER;
			return ret;
		}

        DataServerInfo * dsinfo = dsiter->second;
        baditer = blocks.begin();
        for ( ; baditer != blocks.end(); baditer++)
        {
            dsinfo->blocks_hold_.erase(*baditer);
	    if (false != is_master())
	    {
            	dsinfo->blocks_to_remove_.insert(*baditer);
		layout_manager_.block_to_check_.Push(*baditer);
	    }
        }
        
    }

	{
        BLOCKS_MAP_ITER blocks_map_iter;
        CWLockGuard write_lock_guard(layout_manager_.blocks_mutex_, true);
		baditer = blocks.begin();
		for ( ; blocks.end() != baditer ; baditer++)
		{
        	blocks_map_iter = layout_manager_.blocks_map_.find(*baditer); 
        	if (layout_manager_.blocks_map_.end() == blocks_map_iter)
        	{
            	LOGV(LL_ERROR, "block collect : (%d) not found", *baditer);
				continue;
        	}
        	(blocks_map_iter->second->dataserver_set_).erase(ds_id);
            LOGV(LL_DEBUG, "Erase blockcollec hold ds: %s", BladeNetUtil::AddrToString(ds_id).c_str());
        }	
    }
    return BLADE_SUCCESS;

}

int32_t MetaManager::block_received(BladePacket *packet)
{
	int32_t return_code = BLADE_SUCCESS;
	BlockReceivedPacket * block_received_packet = static_cast<BlockReceivedPacket*>(packet);
	if (NULL == block_received_packet)
	{
		if (NULL != packet)
		{
			delete packet;
		}
		return BLADE_ERROR;
	}
	
    if (false != is_master())
    {
        block_received_packet->Pack();
		int ret = nameserver_->get_blade_ns_log_manager()->GetLogWorker()->BladeBlockReceived(BLADE_LOG_BLOCK_RECEIVED, reinterpret_cast <char *>(block_received_packet->content()), block_received_packet->length());
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "HA ERROR");
			return ret;
		}
    }

	return_code = nameserver_->get_check_thread().block_received(block_received_packet);
	return return_code;	
}

//dataserver定期上报块信息
int MetaManager::report_blocks(BladePacket *packet)
{
    LOGV(LL_DEBUG,"**************************In report blocks*************** ");
    BlockReportPacket * report_packet = static_cast<BlockReportPacket *>(packet);
    
    if (false != is_master())
    {
        report_packet->Pack();
        int ret = nameserver_->get_blade_ns_log_manager()->GetLogWorker()->BladeBlockReport(BLADE_LOG_BLOCK_REPORT, reinterpret_cast <char *>(report_packet->content()), report_packet->length());
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "HA ERROR");
			return ret;
		}
    }
    uint64_t ds_id = report_packet->ds_id();
    set<BlockInfo* > blocks = report_packet->report_blocks_info();

    int ret = BLADE_ERROR;
	DataServerInfo * dataserver_info = NULL;
	BladeBlockCollect * block_collect = NULL;
	//do some preprocess
	std::set< int64_t > set_diff;
	std::set< int64_t > blocks_to_report;
	std::set< int64_t >::iterator block_iter, block_inner_iter;

	std::set<BlockInfo * >::iterator block_info_iter; 

	__gnu_cxx::hash_map<uint64_t, std::set< int64_t > > block_toremove_list;
	std::set< int64_t > blocks_toadd_list;

	typedef __gnu_cxx::hash_map<uint64_t, std::set< int64_t > >::iterator BLOCK_TOREMOVE_ITER; 
	BLOCK_TOREMOVE_ITER block_toremove_iter;
	BLOCKS_MAP_ITER blocks_map_iter;
	SERVER_MAP_ITER server_map_iter;

	block_info_iter = blocks.begin();
	for (; block_info_iter != blocks.end(); block_info_iter++)
	{
		blocks_to_report.insert((*block_info_iter)->block_id());	
	}
    LOGV(LL_DEBUG, "dsid: %s  , report blocks num: %d", BladeNetUtil::AddrToString(ds_id).c_str(), blocks.size());

	{
		CWLockGuard write_lock_guard(layout_manager_.server_mutex_, true);
		LOGV(LL_DEBUG, "STEP 1 IN BLOCK_REPORT");
		server_map_iter = layout_manager_.server_map_.find(ds_id);
		if ((layout_manager_.server_map_.end() == server_map_iter) || (false == server_map_iter->second->is_alive()))
		{
			LOGV(LL_ERROR, "dataserver (%s) not found or expire, need to register first", BladeNetUtil::AddrToString(ds_id).c_str());
			ret = BLADE_NEED_REGISTER;
            
            LOGV(LL_DEBUG,"In report blocks ");
			return ret;
		}

		dataserver_info = server_map_iter->second;

        //第一步:找出持有的blocks和上报的blocks之间的差异
        set_difference((dataserver_info->blocks_hold_).begin(), (dataserver_info->blocks_hold_).end(), blocks_to_report.begin(), blocks_to_report.end(), inserter(set_diff, set_diff.begin()));
        block_iter = set_diff.begin();
		for (; block_iter != set_diff.end(); block_iter++)
		{
			block_inner_iter = (dataserver_info->blocks_hold_).find(*block_iter);	
			if (((dataserver_info->blocks_hold_).end() != block_inner_iter) && (false == has_valid_lease(*block_iter)))
			{
				//加入待检测列表
				if (false != is_master())
				{
					layout_manager_.block_to_check_.Push(*block_iter);
				}
				LOGV(LL_DEBUG, "push to check , NOW SIZE IS : %d", layout_manager_.block_to_check_.Size());

				(dataserver_info->blocks_hold_).erase(block_inner_iter);
			}
		}
	}
	
    {
        CWLockGuard write_lock_guard(layout_manager_.blocks_mutex_, true);
		LOGV(LL_DEBUG, "STEP 2 IN BLOCK_REPORT");
		block_iter = set_diff.begin();
		for ( ; set_diff.end() != block_iter ; block_iter++)
		{
        	blocks_map_iter = layout_manager_.blocks_map_.find(*block_iter); 
        	if (layout_manager_.blocks_map_.end() == blocks_map_iter || (true == has_valid_lease(*block_iter)))
        	{
            	LOGV(LL_DEBUG, "block collect : (%d) not found or has valid lease", *block_iter);
				continue;
        	}
        	(blocks_map_iter->second->dataserver_set_).erase(ds_id);
            LOGV(LL_DEBUG, "erase blockcollect hold ds: %s", BladeNetUtil::AddrToString(ds_id).c_str());
		}
	}

	block_info_iter = blocks.begin();
	block_toremove_list.clear();
    LOGV(LL_DEBUG, "STEP 3 IN BLOCK_REPORT");
	for (; block_info_iter != blocks.end(); block_info_iter++)
	{

		{
			CRLockGuard read_lock_guard(layout_manager_.server_mutex_, true);	
			assert(dataserver_info != NULL);
			int64_t search_block_id = (*block_info_iter)->block_id();
			if ((dataserver_info->blocks_to_remove_).find(search_block_id) != (dataserver_info->blocks_to_remove_).end())
			{
				continue;
			}
		}

		CWLockGuard write_lock_guard(layout_manager_.blocks_mutex_, true);	
		blocks_map_iter = layout_manager_.blocks_map_.find((*block_info_iter)->block_id());
        bool block_exist = meta_tree_.block_exist((*block_info_iter)->file_id(), (*block_info_iter)->block_id());
		if (layout_manager_.blocks_map_.end() == blocks_map_iter &&  block_exist)
		{
			LOGV(LL_INFO, "block : (%d) not found, create it" , (*block_info_iter)->block_id());	

			//加入待检测列表
			if (false != is_master())
			{
				layout_manager_.block_to_check_.Push((*block_info_iter)->block_id());
			}
			LOGV(LL_DEBUG, "push to check , NOW SIZE IS : %d", layout_manager_.block_to_check_.Size());

			int64_t fid = (*block_info_iter)->file_id();
			MetaFattr * fattr = meta_tree_.GetFattr(fid);
			if (NULL != fattr)
			{
				(layout_manager_.blocks_map_)[(*block_info_iter)->block_id()] = new BladeBlockCollect();
				(layout_manager_.blocks_map_)[(*block_info_iter)->block_id()]->block_id_ = (*block_info_iter)->block_id(); 
				(layout_manager_.blocks_map_)[(*block_info_iter)->block_id()]->replicas_number_ = fattr->numReplicas;
				(layout_manager_.blocks_map_)[(*block_info_iter)->block_id()]->version_ = (*block_info_iter)->block_version();
				(layout_manager_.blocks_map_)[(*block_info_iter)->block_id()]->dataserver_set_.insert(ds_id);
			}
			continue;
		}
		else if ((false == block_exist) && (layout_manager_.blocks_map_.end() != blocks_map_iter))
		{
			block_collect = blocks_map_iter->second;
			LOGV(LL_INFO, "block : (%d) not found in btree, but found in blocks map!" , (*block_info_iter)->block_id());	
			std::set<uint64_t>::iterator block_ds_list_iter = block_collect->dataserver_set_.begin();
			for (; block_ds_list_iter != block_collect->dataserver_set_.end(); block_ds_list_iter++)
			{
				block_toremove_iter = block_toremove_list.find(*block_ds_list_iter);
				if (block_toremove_list.end() == block_toremove_iter)
				{
					std::set<int64_t> set_tmp;
					block_toremove_list[ds_id] = set_tmp;
				}
				block_toremove_list[ds_id].insert((*block_info_iter)->block_id());
			}
			layout_manager_.blocks_map_.erase(blocks_map_iter);
			continue;
		}
		else if ((false == block_exist) && (layout_manager_.blocks_map_.end() == blocks_map_iter))
		{

			LOGV(LL_INFO, "block : (%d) not found in blocksmap, and not found in btree!" , (*block_info_iter)->block_id());	
			block_toremove_iter = block_toremove_list.find(ds_id);
			if (block_toremove_list.end() == block_toremove_iter)
			{
				std::set<int64_t> set_tmp;
				block_toremove_list[ds_id] = set_tmp; 
			}
			block_toremove_list[ds_id].insert((*block_info_iter)->block_id());
			continue;
		}
	
		if ((*block_info_iter)->block_version() > blocks_map_iter->second->version_)
		{
			//加入待检测列表
			if (false != is_master())
			{
				layout_manager_.block_to_check_.Push((*block_info_iter)->block_id());
			}
			LOGV(LL_DEBUG, "push to check , NOW SIZE IS : %d", layout_manager_.block_to_check_.Size());

			LOGV(LL_INFO, "report block : (%d)  version :%d is bigger than  blocksmap version:%d" , (*block_info_iter)->block_id(), (*block_info_iter)->block_version(),  blocks_map_iter->second->version_);	

			std::set<uint64_t> blocks_map_ds_list = blocks_map_iter->second->dataserver_set_;			
            blocks_map_ds_list.erase(ds_id);

			blocks_map_iter->second->version_ = (*block_info_iter)->block_version();
			std::set<uint64_t>::iterator blocks_map_ds_list_iter = blocks_map_ds_list.begin();
			for ( ; blocks_map_ds_list_iter != blocks_map_ds_list.end(); blocks_map_ds_list_iter++)
			{
				block_toremove_iter = block_toremove_list.find(*blocks_map_ds_list_iter);
				if (block_toremove_list.end() == block_toremove_iter)
				{
					std::set<int64_t> set_tmp;
					block_toremove_list[*blocks_map_ds_list_iter] = set_tmp; 
				}
				block_toremove_list[*blocks_map_ds_list_iter].insert((*block_info_iter)->block_id());
			}
			blocks_map_iter->second->version_ = (*block_info_iter)->block_version();
			blocks_map_iter->second->dataserver_set_.clear();
			blocks_map_iter->second->dataserver_set_.insert(ds_id);
			blocks_toadd_list.insert((*block_info_iter)->block_id());
		}
		else if ((*block_info_iter)->block_version() < blocks_map_iter->second->version_)
		{
			//加入待检测列表
			if (false != is_master())
			{
				layout_manager_.block_to_check_.Push((*block_info_iter)->block_id());
			}
			LOGV(LL_DEBUG, "push to check , NOW SIZE IS : %d", layout_manager_.block_to_check_.Size());

			LOGV(LL_INFO, "report block : (%d)  version :%d is smaller than  blocksmap version:%d" , (*block_info_iter)->block_id(), (*block_info_iter)->block_version(),  blocks_map_iter->second->version_);	
			block_toremove_iter = block_toremove_list.find(ds_id);
			if (block_toremove_list.end() == block_toremove_iter)
			{
				std::set<int64_t> set_tmp;
				block_toremove_list[ds_id] = set_tmp;
			}
			block_toremove_list[ds_id].insert((*block_info_iter)->block_id());
		}
		else /*==*/
		{
			assert(layout_manager_.blocks_map_.end() != blocks_map_iter);
			//加入待检测列表
			if (false != is_master())
			{
				layout_manager_.block_to_check_.Push((*block_info_iter)->block_id());
			}
			LOGV(LL_DEBUG, "push to check , NOW SIZE IS : %d", layout_manager_.block_to_check_.Size());

			if (blocks_map_iter->second->dataserver_set_.find(ds_id) == blocks_map_iter->second->dataserver_set_.end())
			{
				blocks_map_iter->second->dataserver_set_.insert(ds_id);
				blocks_toadd_list.insert((*block_info_iter)->block_id());
			}
		}
	}

	//deal with block_toremove_list
	{
		CWLockGuard write_lock_guard(layout_manager_.server_mutex_, true);
		block_toremove_iter = block_toremove_list.begin();	
		for (; block_toremove_iter != block_toremove_list.end(); block_toremove_iter++)
		{
			server_map_iter = layout_manager_.server_map_.find(block_toremove_iter->first);	
			if (layout_manager_.server_map_.end() != server_map_iter)
			{
				dataserver_info = server_map_iter->second;
				block_iter = (block_toremove_iter->second).begin();
				for (; block_iter != (block_toremove_iter->second).end(); block_iter++)
				{
					if (false != is_master())
					{
						(dataserver_info->blocks_to_remove_).insert(*block_iter);	
					}
					(dataserver_info->blocks_hold_).erase(*block_iter);	
				}
			}
		}

		block_iter = blocks_toadd_list.begin();
		server_map_iter = layout_manager_.server_map_.find(ds_id);	
		for ( ; block_iter != blocks_toadd_list.end() && (layout_manager_.server_map_.end() != server_map_iter); block_iter++)
		{
			(server_map_iter->second->blocks_hold_).insert(*block_iter);
		}
	}
    LOGV(LL_DEBUG,"In report blocks end!!!!!!!!!!!");

	return BLADE_SUCCESS;
}

int MetaManager::get_block_info(int64_t block_id, std::set<uint64_t> & ds_list)
{
	BladeBlockCollect * block_collect = NULL;
	CRLockGuard read_lock_guard(layout_manager_.blocks_mutex_, true);	
	block_collect = layout_manager_.get_block_collect(block_id);
	if (NULL == block_collect)
	{
		LOGV(LL_ERROR, "block : (%d) not found", block_id);
		return BLADE_ERROR;
	}
	ds_list = block_collect->dataserver_set_;
	return BLADE_SUCCESS;
}


//WRITE_THREAD和REPLAY的时候都会调用的函数
int32_t MetaManager::meta_blade_create(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "**********************IN CREATE*********************");
    CreatePacket *packet = static_cast<CreatePacket *>(blade_packet);
	int64_t parent_id = packet->parent_id();
    string path = packet->file_name();
    int8_t oflag = packet->oflag();
    int16_t replication = packet->replication();
    LOGV(LL_INFO, "parent_id: %ld, path: %s   oflag:%d   replication:%d", parent_id, path.c_str(), oflag, replication);

    int16_t ret_code = BLADE_ERROR;
    int64_t new_fid = 0;
	blade_reply_packet = new CreateReplyPacket(ret_code, new_fid);
    CreateReplyPacket *reply_packet = static_cast<CreateReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());

    if ('/' == path[0] || 0 == path.size()) {
        LOGV(LL_ERROR, "path name is invalid!");
        ret_code = RET_INVALID_PARAMS;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
        return BLADE_SUCCESS;
    }
    string father_dir, file_name;
    MetaFattr *dir_attr = NULL;
    if (bladestore::common::split_string(path, '/', father_dir, file_name)) {
        LOGV(LL_INFO, "dir_name:%s   file_name:%s, has to lookuppath", father_dir.c_str(), file_name.c_str());
       dir_attr = meta_tree_.LookupPath(parent_id, father_dir);
    } else { 
       dir_attr = meta_tree_.GetFattr(parent_id);
        file_name = path;
    }
    
    if (NULL == dir_attr || BLADE_FILE == dir_attr->type) 
    {
        LOGV(LL_ERROR, "ERROR DIR");
        ret_code = RET_INVALID_DIR_NAME;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
        return BLADE_SUCCESS;
    }

    if (false != is_master())
    {
        packet->Pack();
        nameserver_->get_blade_ns_log_manager()->GetLogWorker()->BladeCreate(BLADE_LOG_CREATE, reinterpret_cast <char *>(packet->content()), packet->length());
    }

    if (replication < BLADE_MIN_REPLICAS_PER_FILE)
        replication = BLADE_MIN_REPLICAS_PER_FILE;
    else if (replication > BLADE_MAX_REPLICAS_PER_FILE)
        replication = BLADE_MAX_REPLICAS_PER_FILE;

    int err = meta_tree_.Create(dir_attr->fid, file_name, &new_fid, replication ,true);
    if (0 != err)
    {
        if (-EINVAL == err)
        {
            LOGV(LL_ERROR, "Invalid params!");
            ret_code = RET_INVALID_PARAMS;
        }
        else if (-EISDIR == err)
        {
            LOGV(LL_ERROR, "EXIST A DIR NAME!");
            ret_code = RET_NOT_FILE;
        }
        else if (-EBUSY == err)
        {
            LOGV(LL_ERROR, "CREATE FILE ERROR!");
            ret_code = RET_CREATE_FILE_ERROR;
        }
        else if (-EEXIST == err)
        {
            LOGV(LL_ERROR, "FILE EXIST!");
            ret_code = RET_FILE_EXIST;
        }
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
    }
    else 
    {
        LOGV(LL_INFO, "new fid:%ld", new_fid);
        ret_code = RET_SUCCESS;
        reply_packet->set_file_id(new_fid);
        AtomicInc64(&Singleton<StatManager>::Instance().file_nums_);
    }
    reply_packet->set_ret_code(ret_code);
    return BLADE_SUCCESS;
}

int32_t MetaManager::meta_blade_mkdir(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "**********************IN MKDIR*********************");
	MkdirPacket *packet = static_cast<MkdirPacket *>(blade_packet);
    int64_t parent_id = packet->parent_id();
    string path = packet->file_name();
    LOGV(LL_INFO, "parent_id = %ld, path = %s", parent_id, path.c_str());

    int16_t ret_code = BLADE_ERROR;
    int64_t new_fid = 0;
    blade_reply_packet = new MkdirReplyPacket(ret_code, new_fid);
    MkdirReplyPacket *reply_packet = static_cast<MkdirReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());

    if ('/' == path[0] || 0 == path.size()) 
    {
        LOGV(LL_ERROR, "path is invalid!");
        ret_code = RET_INVALID_PARAMS;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
        return BLADE_SUCCESS;
    }
    string father_dir, file_name;
    MetaFattr *dir_attr = NULL;
    if (bladestore::common::split_string(path,'/',father_dir,file_name))
    { //path with "/"
        LOGV(LL_INFO, "parent_dir:%s  file_name:%s ,has to lookupPath", father_dir.c_str(), file_name.c_str());
        dir_attr = meta_tree_.LookupPath(parent_id, father_dir);
    } 
    else 
    {
        dir_attr = meta_tree_.GetFattr(parent_id);
        file_name = path;
    }
    if (NULL == dir_attr || BLADE_FILE == dir_attr->type) 
    {
        LOGV(LL_ERROR, "ERROR DIR");
        ret_code = RET_INVALID_DIR_NAME;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
        return BLADE_SUCCESS;
    }

    if (false != is_master())
    {
        packet->Pack();
        nameserver_->get_blade_ns_log_manager()->GetLogWorker()->BladeCreate(BLADE_LOG_MKDIR, reinterpret_cast <char *>(packet->content()), packet->length());
    }

    int err = meta_tree_.Mkdir(dir_attr->fid, file_name, &new_fid);
    if (0 != err)
    {
        if (-EINVAL == err)
        {
            LOGV(LL_ERROR, "invalid params!");
            ret_code = RET_INVALID_PARAMS;
        }
        else if (-EEXIST == err)
        {
            LOGV(LL_ERROR, "DIR EXIST!");
            ret_code = RET_DIR_EXIST;
        }
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
    }
    else 
    {
        LOGV(LL_INFO, "new_fid:%ld", new_fid);
        reply_packet->set_file_id(new_fid);
        ret_code = RET_SUCCESS;
        AtomicInc64(&Singleton<StatManager>::Instance().dir_nums_);
    }
    reply_packet->set_ret_code(ret_code);
    LOGV(LL_DEBUG, "ret_code %d", reply_packet->ret_code());
    return BLADE_SUCCESS;
}

int32_t MetaManager::do_add_block(BladePacket *blade_packet)
{
    LOGV(LL_INFO, "**********************IN DO_ADDBLOCK*********************");
    LogAddBlockPacket *packet = static_cast<LogAddBlockPacket *>(blade_packet);
    int64_t file_id = packet->file_id();
    int8_t is_safe_write = packet->is_safe_write();
    vector<uint64_t> ds_vector = packet->ds_vector();
    LOGV(LL_DEBUG, "file_id : %ld, ds_num : %d", file_id, ds_vector.size());

    MetaFattr * file_attr = meta_tree_.GetFattr(file_id);
    if (NULL == file_attr || BLADE_DIR == file_attr->type)
    {
        LOGV(LL_ERROR, "file id error!");
        return BLADE_ERROR;
    }
    if (0 != (file_attr->filesize % BLADE_BLOCK_SIZE))
    {
        LOGV(LL_ERROR, "file is completed!");
        return BLADE_ERROR;
    }
    int16_t num_replicas = file_attr->numReplicas;
    int64_t block_id = 0;
    int64_t block_version = 0;
    //分配block_id
    int err = meta_tree_.AllocateChunkId(file_id, file_attr->filesize, &block_id, &block_version, &num_replicas);
    if (0 != err)
    {
        LOGV(LL_ERROR, "allocatechunkid error!");
        return BLADE_ERROR;
    }
    //插入blocks_map
    BladeBlockCollect *block_collect = new BladeBlockCollect();
    block_collect->block_id_ = block_id;
    block_collect->version_ = block_version;
    block_collect->replicas_number_ = num_replicas;
    for (vector<uint64_t>::iterator it = ds_vector.begin(); it != ds_vector.end(); ++it)
    {          
        (block_collect->dataserver_set_).insert(*it);
    }
    bool ret = layout_manager_.Insert(block_collect, true);
    if (false == ret) 
    {    
        LOGV(LL_ERROR, "layout insert error!");
        return BLADE_ERROR;
    }    

    //插入server_map
    layout_manager_.server_mutex_.wlock()->lock();
    for (vector<uint64_t>::size_type ix = 0; ix != ds_vector.size(); ++ix)
    {
        if (NULL != layout_manager_.get_dataserver_info(ds_vector[ix]))
	{
            (layout_manager_.get_dataserver_info(ds_vector[ix])->blocks_hold_).insert(block_id);
	}
    }
    layout_manager_.server_mutex_.wlock()->unlock();

    //add block to B+ tree
    meta_tree_.AssignChunkId(file_id, file_attr->filesize, block_id, block_version);
    //block num of file add 1
    ++(file_attr->chunkcount);

    //add lease 
    nameserver_->get_lease_manager().register_lease(file_id, block_id, block_version, is_safe_write);
    return BLADE_SUCCESS;
}

int32_t MetaManager::meta_blade_add_block(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "**********************IN ADDBLOCK*********************");                                                        
    AddBlockPacket *packet = static_cast<AddBlockPacket *>(blade_packet);

    int64_t file_id = packet->file_id();
    LOGV(LL_INFO, "fid: %ld", file_id);
    int8_t is_safe_write = packet->is_safe_write();
    if (0 != is_safe_write)
    {
        AtomicInc64(&Singleton<StatManager>::Instance().safe_write_times_);
    }

    int16_t ret_code = BLADE_ERROR;
    int64_t block_id = 0;
    int64_t block_version = 0;
    int64_t offset = 0;
    vector<uint64_t> results;
    blade_reply_packet = new AddBlockReplyPacket(ret_code, block_id, block_version, offset, results);
    AddBlockReplyPacket *reply_packet = static_cast<AddBlockReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());

    MetaFattr * file_attr = meta_tree_.GetFattr(file_id);
    if (NULL == file_attr || BLADE_DIR == file_attr->type)
    {
        LOGV(LL_ERROR, "invalid params!");
        ret_code = RET_INVALID_PARAMS;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
        if (0 != is_safe_write)
	{
            AtomicInc64(&Singleton<StatManager>::Instance().safe_write_error_times_);
	}
        return BLADE_SUCCESS;
    }
    if (0 != (file_attr->filesize % BLADE_BLOCK_SIZE))
    {
        LOGV(LL_ERROR, "file is completed!");
        ret_code = RET_FILE_COMPLETED;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
        if (0 != is_safe_write)
	{
            AtomicInc64(&Singleton<StatManager>::Instance().safe_write_error_times_);
	}
        return BLADE_SUCCESS;
    }

    if (0 == layout_manager_.get_alive_ds_size())
    {
        LOGV(LL_ERROR, "ds num = 0!");
        ret_code = RET_NO_DATASERVER;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
        if (0 != is_safe_write)
	{
            AtomicInc64(&Singleton<StatManager>::Instance().safe_write_error_times_);
	}
        return BLADE_SUCCESS;
    }

    //检查文件最后一个块是否持有租约
    if (0 != file_attr->chunkcount && true == has_file_valid_lease(file_id))
    {
        LOGV(LL_ERROR, "last block is writing!");  
        ret_code = RET_LAST_BLOCK_NO_COMPLETE;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
        if (0 != is_safe_write)
	{
            AtomicInc64(&Singleton<StatManager>::Instance().safe_write_error_times_);
	}
        return BLADE_SUCCESS;
    }
    LOGV(LL_INFO, "offset:%ld", file_attr->filesize);
    int16_t num_replicas = file_attr->numReplicas;
    //分配DS
    int port;
    string client_ip = BladeNetUtil::AddrToIP(BladeNetUtil::GetPeerID(packet->endpoint_.GetFd()), &port);
    LOGV(LL_INFO, "client_ip : %s", client_ip.c_str());
    ReplicateStrategy *replica_strategy = new ReplicateStrategy(layout_manager_);
    replica_strategy->choose_target(num_replicas, client_ip, results);
    delete replica_strategy;
    if (0 == results.size()) 
    {
        LOGV(LL_ERROR, "No ds to choose!");
        ret_code = RET_NO_DATASERVER;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
        if (0 != is_safe_write)
	{
            AtomicInc64(&Singleton<StatManager>::Instance().safe_write_error_times_);
	}
        return BLADE_SUCCESS;
    }

    if (false != is_master())
    {
        LogAddBlockPacket *log_packet = new LogAddBlockPacket(file_id, results, is_safe_write);
        log_packet->Pack();
        int ret = nameserver_->get_blade_ns_log_manager()->GetLogWorker()->BladeAddBlock(BLADE_LOG_ADDBLOCK, reinterpret_cast <char *>(log_packet->content()), log_packet->length());
        if (NULL != log_packet)
	{
            delete log_packet;
	}
	if (BLADE_SUCCESS != ret)
	{
	    LOGV(LL_ERROR, "HA ERROR");
	    return ret;
	}
    }

    int err = meta_tree_.AllocateChunkId(file_id, file_attr->filesize, &block_id, &block_version, &num_replicas);
    LOGV(LL_INFO, "num_replicas:%d, block_id:%ld", num_replicas, block_id);
    if (0 != err)
    {
        LOGV(LL_ERROR,"RET_ALOC_BLOCK_ERROR");
        ret_code = RET_ALOC_BLOCK_ERROR;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
        if (0 != is_safe_write)
	{
            AtomicInc64(&Singleton<StatManager>::Instance().safe_write_error_times_);
	}
        return BLADE_SUCCESS;
    }   

    BladeBlockCollect *block_collect = new BladeBlockCollect();
    block_collect->block_id_ = block_id;
    block_collect->version_ = block_version;
    block_collect->replicas_number_ = num_replicas;
    for (vector<uint64_t>::iterator it = results.begin(); it != results.end(); ++it)
    { 
        (block_collect->dataserver_set_).insert(*it);
    }
    bool ret = layout_manager_.Insert(block_collect, true);
    if (false == ret)
    {
        LOGV(LL_ERROR, "layout insert error!");
        ret_code = RET_LAYOUT_INSERT_ERROR;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
        if (0 != is_safe_write)
	{
            AtomicInc64(&Singleton<StatManager>::Instance().safe_write_error_times_);
	}
        return BLADE_SUCCESS;
    }

    layout_manager_.server_mutex_.wlock()->lock();
    for (vector<uint64_t>::size_type ix = 0; ix != results.size(); ++ix)
    {
        if (NULL != layout_manager_.get_dataserver_info(results[ix]))
	{
            (layout_manager_.get_dataserver_info(results[ix])->blocks_hold_).insert(block_id);
	}
    }
    layout_manager_.server_mutex_.wlock()->unlock();

    //add block to B+ tree
    meta_tree_.AssignChunkId(file_id, file_attr->filesize, block_id, block_version);
    //block num of file add 1
    ++(file_attr->chunkcount);
    LOGV(LL_DEBUG, "chunkcount :%ld", file_attr->chunkcount);

    //add lease 
    nameserver_->get_lease_manager().register_lease(file_id, block_id, block_version, is_safe_write);
    ret_code = RET_SUCCESS;
    LOGV(LL_INFO, "block_id:%ld, block_version:%ld, size:%ld", block_id, block_version, file_attr->filesize);
    for (vector<uint64_t>::size_type ix = 0; ix != results.size(); ++ix)
    {
        LOGV(LL_INFO, "**************add_block ds_id : %ld, ip:%s", results[ix], BladeNetUtil::AddrToString(results[ix]).c_str());
    }
    LocatedBlock *located_block = new LocatedBlock(block_id, block_version, file_attr->filesize, 0, results);
    reply_packet->set_ret_code(ret_code);
    reply_packet->set_dataserver_num(results.size());
    reply_packet->set_located_block(*located_block);
    delete located_block;
    return BLADE_SUCCESS;
}

int32_t MetaManager::meta_blade_complete(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "**********************IN COMPLETE*********************");    
	CompletePacket *packet = static_cast<CompletePacket *>(blade_packet);

    int64_t file_id = packet->file_id();
    int64_t block_id = packet->block_id();
    int32_t block_size = packet->block_size();
    LOGV(LL_INFO, "file_id : %ld, blockid:%ld , block_size:%d", file_id, block_id, block_size);

    int16_t ret_code = BLADE_ERROR;
    blade_reply_packet = new CompleteReplyPacket(ret_code);
    CompleteReplyPacket *reply_packet = static_cast<CompleteReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());
    if (block_size <= 0 || block_size > BLADE_BLOCK_SIZE) 
    {
        LOGV(LL_ERROR, "block_size error");
        ret_code = RET_INVALID_PARAMS;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }

    MetaFattr * file_attr = meta_tree_.GetFattr(file_id);
    if (NULL == file_attr)
    {
        LOGV(LL_ERROR, "fid not exist!");
        ret_code = RET_FILE_NOT_EXIST;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }
    
    if (false != is_master())
    {
        packet->Pack();
        int ret = nameserver_->get_blade_ns_log_manager()->GetLogWorker()->BladeComplete(BLADE_LOG_COMPLETE, reinterpret_cast <char *>(packet->content()), packet->length());
	if (BLADE_SUCCESS != ret)
	{
	    LOGV(LL_ERROR, "HA ERROR");
	    return ret;
	}
    }
    
    //remove lease 
    int err1 = nameserver_->get_lease_manager().relinquish_lease(block_id);
    if (0 != err1)
    {
        ret_code = RET_BLOCK_NO_LEASE;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }

    //add file's size                                                                                                               
    file_attr->set_file_size(file_attr->filesize + block_size);
    LOGV(LL_INFO, "filesize : %ld", file_attr->filesize);
    
    ret_code = RET_SUCCESS;
    reply_packet->set_ret_code(ret_code);
    return BLADE_SUCCESS;
}

int32_t MetaManager::meta_blade_get_block_locations(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "*************************IN GET_BLOCK_LOCATIONs*******************************");
    GetBlockLocationsPacket *packet = static_cast<GetBlockLocationsPacket *>(blade_packet);
    int64_t parent_id = packet->parent_id();
    string file_name = packet->file_name();
    int64_t offset = packet->offset();
    int64_t data_length = packet->data_length();
    LOGV(LL_INFO, "parent_id:%ld, file name:%s   offset:%ld   length:%ld", parent_id, file_name.c_str(), offset, data_length);

    int16_t ret_code = BLADE_ERROR;
    int64_t file_id = 0;
    int64_t file_length = 0;
    vector <LocatedBlock> result;
    blade_reply_packet = new GetBlockLocationsReplyPacket(ret_code, file_id, file_length, result);
    GetBlockLocationsReplyPacket *reply_packet = static_cast<GetBlockLocationsReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());

    if (offset < 0 || data_length <= 0 || '/' == file_name[0])
    {
        LOGV(LL_ERROR, "invalid params!");
        ret_code = RET_INVALID_PARAMS;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().read_error_times_);
        return BLADE_SUCCESS;
    }

    MetaFattr * file_attr = NULL;
    if (0 != file_name.empty())
    { 
        file_attr = meta_tree_.GetFattr(parent_id); //parent_id = file_id
    }
    else
    {
        file_attr = meta_tree_.LookupPath(parent_id, file_name);
    }

    if (NULL == file_attr)
    {
        LOGV(LL_ERROR, "RET_LOOKUP_FILE_ERROR");
        ret_code = RET_LOOKUP_FILE_ERROR;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().read_error_times_);
        return BLADE_SUCCESS;
    }
    else if (BLADE_DIR == file_attr->type)
    {
        LOGV(LL_ERROR, "it's a dir name");
        ret_code = RET_NOT_FILE;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().read_error_times_);
        return BLADE_SUCCESS;
    }
    file_id = file_attr->fid;
    file_length = file_attr->filesize;
    LOGV(LL_INFO, "length:%ld , chunkcount:%ld ", file_length, file_attr->chunkcount);
    if(0 == file_length)
    {
        reply_packet->set_ret_code(RET_SUCCESS);
        reply_packet->set_file_id(file_id);
        return BLADE_SUCCESS;
    }
    int64_t start_count = offset / BLADE_BLOCK_SIZE + 1;
    if (start_count > file_attr->chunkcount || offset >= file_length)
    {
        LOGV(LL_ERROR, "invalid offset!");
        ret_code = RET_INVALID_OFFSET;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().read_error_times_);
        return BLADE_SUCCESS;
    }
    int64_t end_count;
    if (0 == (offset + data_length) % BLADE_BLOCK_SIZE)
    {
        end_count = (offset + data_length) / BLADE_BLOCK_SIZE;
    }
    else
    { 
        end_count = (offset + data_length) / BLADE_BLOCK_SIZE + 1;
    }
    if (end_count > file_attr->chunkcount)
    {
        end_count = file_attr->chunkcount;
    }
    LOGV(LL_INFO, "start:%ld, end:%ld", start_count, end_count);
    
    //test if file's last block has lease
    if (0 != file_attr->chunkcount && end_count == file_attr->chunkcount && true == has_file_valid_lease(file_id))
    {
        --end_count;
    }
    
    if (start_count > end_count)
    {
        LOGV(LL_ERROR, "No block to return!");
        ret_code = RET_LAST_BLOCK_NO_COMPLETE;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().read_error_times_);
        return BLADE_SUCCESS;
    }

    vector <MetaChunkInfo *> file_blocks_info;
    meta_tree_.Getalloc(file_attr->fid, file_blocks_info);
    LOGV(LL_DEBUG, "file block size : %d", file_blocks_info.size());
    
    for (int64_t i = start_count; i <= end_count; ++i)
    {
        vector<uint64_t> ds_list;

        layout_manager_.blocks_mutex_.rlock()->lock();
        LOGV(LL_DEBUG, "block id : %ld", file_blocks_info[i-1]->chunkId);
        assert(layout_manager_.get_block_collect(file_blocks_info[i-1]->chunkId));
        set<uint64_t> ds_set =
            layout_manager_.get_block_collect(file_blocks_info[i-1]->chunkId)->dataserver_set_;
        LOGV(LL_DEBUG, "ds list size : %d", ds_set.size());
        for (set<uint64_t>::iterator it = ds_set.begin(); it != ds_set.end(); ++it)
        {
            ds_list.push_back(*it);
	}
        layout_manager_.blocks_mutex_.rlock()->unlock();
        
        int port;
        string client_ip = BladeNetUtil::AddrToIP(BladeNetUtil::GetPeerID(packet->endpoint_.GetFd()), &port);
        ReplicateStrategy *replica_strategy = new ReplicateStrategy(layout_manager_); 
        replica_strategy->get_pipeline(client_ip, ds_list);
        delete replica_strategy;

        int64_t block_length = 0;
        if (0 != ((file_attr->filesize) % BLADE_BLOCK_SIZE) && (i == file_attr->chunkcount))
        { 
            block_length = (file_attr->filesize) % BLADE_BLOCK_SIZE;
	}
        else
	{ 
            block_length = BLADE_BLOCK_SIZE;
	}
        LocatedBlock lb(file_blocks_info[i-1]->chunkId,
        file_blocks_info[i-1]->chunkVersion, file_blocks_info[i-1]->offset,
        block_length, ds_list);
        LOGV(LL_INFO, "block size : %ld", block_length);
        for (vector<uint64_t>::const_iterator cit = ds_list.begin(); cit != ds_list.end(); ++cit)
	{
            LOGV(LL_INFO, "************ds ip : %s", BladeNetUtil::AddrToString(*cit).c_str());
	}
        result.push_back(lb);
    }

    ret_code = RET_SUCCESS;
    reply_packet->set_ret_code(ret_code);
    reply_packet->set_file_id(file_id);
    reply_packet->set_file_length(file_length);
    reply_packet->set_located_block_num(result.size());
    reply_packet->set_v_located_block(result);
    return BLADE_SUCCESS;
}

int32_t MetaManager::meta_blade_is_valid_path(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "***************************************IN ISVALIDPATH*************************************");
    IsValidPathPacket *packet = static_cast<IsValidPathPacket *>(blade_packet);
    int64_t parent_id = packet->parent_id();
    string dir_name = packet->file_name();
    LOGV(LL_INFO, "parent_id:%ld, file_name:%s", parent_id, dir_name.c_str());

    int16_t ret_code = BLADE_ERROR;
    blade_reply_packet = new IsValidPathReplyPacket(ret_code);
    IsValidPathReplyPacket *reply_packet = static_cast<IsValidPathReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id()); 

    if ('/' == dir_name[0])
    {
        LOGV(LL_ERROR, "dir name is begin with /");
        ret_code = RET_INVALID_PARAMS;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().read_error_times_);
        return BLADE_SUCCESS;
    }
    MetaFattr *dir_attr = NULL;
    if ( 0 != dir_name.empty())
    {
        dir_attr = meta_tree_.GetFattr(parent_id); //parent_id = file_id
    }
    else
    {
        dir_attr = meta_tree_.LookupPath(parent_id, dir_name);
    }
    if (NULL == dir_attr)
    {
        LOGV(LL_ERROR, "Dir don't exist");
        ret_code = RET_DIR_NOT_EXIST;
        AtomicInc64(&Singleton<StatManager>::Instance().read_error_times_);
    }
    else if (BLADE_FILE == dir_attr->type)
    {
        LOGV(LL_INFO, "A FILE, NOT DIR");
        ret_code = RET_NOT_DIR;
        AtomicInc64(&Singleton<StatManager>::Instance().read_error_times_);
    }
    else
    { 
        ret_code = RET_SUCCESS;
    }
    
    reply_packet->set_ret_code(ret_code);
    return BLADE_SUCCESS;
}

int32_t MetaManager::meta_blade_get_file_info(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "***************************************IN GETFILEINFO*************************************");
    GetFileInfoPacket *packet = static_cast<GetFileInfoPacket *>(blade_packet);
    int64_t parent_id = packet->parent_id();
    string file_name = packet->file_name();
    LOGV(LL_INFO, "parent_id: %ld, file_name: %s", parent_id, file_name.c_str());

    int16_t ret_code = BLADE_ERROR;
    FileType file_type = BLADE_NONE;
    int64_t file_id = 0;
    struct timeval mt, ct, crt;
    int64_t block_count = 0;
    int16_t num_replicas = 0;
    off_t file_size = 0;
    blade_reply_packet = new GetFileInfoReplyPacket(ret_code, file_type, file_id,
            mt, ct ,crt, block_count, num_replicas, file_size);
    GetFileInfoReplyPacket *reply_packet = static_cast<GetFileInfoReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());

    if ('/' == file_name[0])
    {
        LOGV(LL_ERROR, "file name is begin with /");
        ret_code = RET_INVALID_PARAMS;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().read_error_times_);
        return BLADE_SUCCESS;
    }

    MetaFattr * file_attr = NULL;
    if (0 != file_name.empty())
    {
        file_attr = meta_tree_.GetFattr(parent_id); //parent_id = file_id
    }
    else
    {
        file_attr = meta_tree_.LookupPath(parent_id, file_name);
    }

    if (NULL == file_attr)
    {
        LOGV(LL_ERROR, "file don't exist!");
        ret_code = RET_FILE_NOT_EXIST;
        AtomicInc64(&Singleton<StatManager>::Instance().read_error_times_);
    }
    else 
    {
        ret_code = RET_SUCCESS;
        int8_t file_type = BLADE_FILE_TYPE_UNKNOWN;
        if (file_attr->type == BLADE_FILE)
	{
            file_type = BLADE_FILE_TYPE_FILE;
	}
        else if (file_attr->type == BLADE_DIR)
	{
            file_type = BLADE_FILE_TYPE_DIR;
	}
        FileInfo file_info(file_attr->fid, file_type, file_attr->mtime, file_attr->ctime, 
                file_attr->crtime, file_attr->chunkcount, file_attr->numReplicas,
                file_attr->filesize);
        reply_packet->set_file_info(file_info);
        LOGV(LL_DEBUG, "Get_file_info success!");
    }
    reply_packet->set_ret_code(ret_code);
    return BLADE_SUCCESS;
}

int32_t MetaManager::meta_blade_get_listing(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "***************************************IN GETLISTING*************************************");
    GetListingPacket *packet = static_cast<GetListingPacket *>(blade_packet);
    int64_t parent_id = packet->parent_id();
    string file_name = packet->file_name();
    LOGV(LL_INFO, "parent_id: %ld, file_name: %s size:%ld", parent_id, file_name.c_str(), file_name.size());

    int16_t ret_code = BLADE_ERROR;
    vector<string> files;
    blade_reply_packet = new GetListingReplyPacket(ret_code, files);
    GetListingReplyPacket *reply_packet = static_cast<GetListingReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());

    if ('/' == file_name[0])
    {
        LOGV(LL_ERROR, "file_name is begin with /");
        ret_code = RET_INVALID_PARAMS;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().read_error_times_);
        return BLADE_SUCCESS;
    }

    MetaFattr * file_attr = NULL;
    if (0 != file_name.empty())
    {
        file_attr = meta_tree_.GetFattr(parent_id); //parent_id = file_id
    }
    else
    {
        file_attr = meta_tree_.LookupPath(parent_id, file_name);
    }

    if (NULL == file_attr)
    {   
        LOGV(LL_ERROR, "file don't exist!");
        ret_code = RET_FILE_NOT_EXIST;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().read_error_times_);
        return BLADE_SUCCESS;
    }
    else if (BLADE_FILE == file_attr->type)
    {
        LOGV(LL_INFO, "It's a file");
        ret_code = RET_NOT_DIR;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().read_error_times_);
        return BLADE_SUCCESS;
    }

    vector<MetaDentry *> files_dentry;
    meta_tree_.Readdir(file_attr->fid, files_dentry);

    for (vector<MetaDentry *>::iterator it = files_dentry.begin();
            it != files_dentry.end(); ++it)
    {
        files.push_back((*it)->getName());
    }

    ret_code = RET_SUCCESS;
    reply_packet->set_ret_code(ret_code);
    reply_packet->set_file_num(files.size());
    LOGV(LL_DEBUG, "file num : %ld", files.size());
    reply_packet->set_file_names(files);
    return BLADE_SUCCESS;
}

int32_t MetaManager::meta_blade_expire_lease(BladePacket *blade_packet)
{
    LOGV(LL_INFO, "**********************IN EXPIRE LEASE*********************"); 
    LeaseExpiredPacket *packet = static_cast<LeaseExpiredPacket *>(blade_packet);
    if (false != is_master())
    {
        packet->Pack();
        int ret = nameserver_->get_blade_ns_log_manager()->GetLogWorker()->BladeExpireLease(BLADE_LOG_EXPIRE_LEASE, reinterpret_cast <char *>(packet->content()), packet->length());
	if (BLADE_SUCCESS != ret)
	{
	    LOGV(LL_ERROR, "HA ERROR");
	    return ret;
	}
    }

    int64_t block_id = packet->block_id();
    int64_t fid = packet->file_id();

    int32_t err1 = nameserver_->get_lease_manager().relinquish_lease(block_id);
    if (0 != err1)
    {
        LOGV(LL_ERROR, "erase lease map error!");
        return BLADE_ERROR;
    }
    
    //remove blocks_map and server_map
    layout_manager_.remove_block_collect(block_id, is_master());
    //remove B+ tree
    MetaFattr * file_attr = meta_tree_.GetFattr(fid);
    if (NULL == file_attr)
    {
        LOGV(LL_ERROR, "fid:%ld not exist!", fid);
        return BLADE_ERROR;
    }
    MetaChunkInfo *file_last_block = NULL;
    int32_t err3 = meta_tree_.Getalloc(fid, (file_attr->chunkcount - 1) * BLADE_BLOCK_SIZE, &file_last_block);
    if (0 != err3|| NULL == file_last_block)
    {
        LOGV(LL_ERROR, "/getalloc error!");
        return BLADE_ERROR;
    }
    else if (file_last_block->chunkId != block_id)
    {
        return BLADE_ERROR;
    }
    meta_tree_.delete_block(fid, file_last_block->offset, block_id);
    --(file_attr->chunkcount);
    return BLADE_SUCCESS;
}

int32_t MetaManager::meta_blade_abandon_block(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "**********************IN ABANDONBLOCK*********************");        
	AbandonBlockPacket *packet = static_cast<AbandonBlockPacket *>(blade_packet);
    if (false != is_master())
    {
        packet->Pack();
        int ret = nameserver_->get_blade_ns_log_manager()->GetLogWorker()->BladeAbandonBlock(BLADE_LOG_ABANDONBLOCK, reinterpret_cast <char *>(packet->content()), packet->length());
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "HA ERROR");
			return ret;
		}
    }   

    int64_t fid = packet->file_id();
    int64_t block_id = packet->block_id();
    LOGV(LL_INFO, "block_id: %ld fid :%ld", block_id, fid);

    int16_t ret_code = BLADE_ERROR;
    blade_reply_packet = new AbandonBlockReplyPacket(ret_code);
    AbandonBlockReplyPacket *reply_packet = static_cast<AbandonBlockReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());

    int err1 = nameserver_->get_lease_manager().relinquish_lease(block_id);
    if (0 != err1)
    {
        ret_code = RET_BLOCK_NO_LEASE;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }

    //remove blocks_map and server_map
    layout_manager_.remove_block_collect(block_id, is_master());
    //remove B+ tree
    MetaFattr * file_attr = meta_tree_.GetFattr(fid);
    if (NULL == file_attr)
    {
        LOGV(LL_ERROR, "fid:%ld not exist!", fid);
        ret_code = RET_FILE_NOT_EXIST;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }
    MetaChunkInfo *file_last_block = NULL;
    int32_t err3 = meta_tree_.Getalloc(fid, (file_attr->chunkcount - 1) * BLADE_BLOCK_SIZE, &file_last_block);
    if (0 != err3|| NULL == file_last_block)
    {
        LOGV(LL_ERROR, "getalloc error!");
        ret_code = RET_GETALLOC_ERROR;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }
    else if (file_last_block->chunkId != block_id)
    {
        ret_code = RET_INVALID_BLOCKID;
        return BLADE_SUCCESS;
    }
    meta_tree_.delete_block(fid, file_last_block->offset, block_id);
    --(file_attr->chunkcount);
    ret_code = RET_SUCCESS;
    reply_packet->set_ret_code(ret_code);
    return BLADE_SUCCESS;
}

int32_t MetaManager::meta_blade_rename(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    //TODO
    LOGV(LL_INFO, "**********************IN RENAME************************");        
	RenamePacket *packet = static_cast<RenamePacket *>(blade_packet);
    if (false != is_master())
    {
        packet->Pack();
        int ret = nameserver_->get_blade_ns_log_manager()->GetLogWorker()->BladeRename(BLADE_LOG_RENAME, reinterpret_cast <char *>(packet->content()), packet->length());
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_ERROR, "HA ERROR");
			return ret;
		}
    }   
    
    string src_path = packet->get_src_file_name();
    string dst_path = packet->get_dst_file_name();
    LOGV(LL_INFO, "src_path: %s, dst_path: %s", src_path.c_str(), dst_path.c_str());

    int16_t ret_code = BLADE_ERROR;
    blade_reply_packet = new RenameReplyPacket(ret_code);
    RenameReplyPacket *reply_packet = static_cast<RenameReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());

    MetaFattr *file_attr = meta_tree_.LookupPath(ROOTFID, src_path);
    if (NULL == file_attr)
    {   
        LOGV(LL_ERROR, "file don't exist!");
        ret_code = RET_FILE_NOT_EXIST;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;                                                                                                       
    }
    else if (BLADE_FILE == file_attr->type && true == has_file_valid_lease(file_attr->fid))
    {
        LOGV(LL_ERROR, "last block is writing!");
        ret_code = RET_LAST_BLOCK_NO_COMPLETE;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }
//    else if(file_attr->type == BLADE_FILE)
//    {
//        //test if file is writing!
//        MetaChunkInfo *file_last_block = NULL;
//        if(file_attr->chunkcount != 0)
//        {   
//            int err1 = meta_tree_.getalloc(file_attr->fid, (file_attr->chunkcount - 1) * BLADE_BLOCK_SIZE, &file_last_block);
//            if(err1 != 0 || NULL == file_last_block)
//            {   
//                LOGV(LL_ERROR, "getalloc error!");
//                ret_code = RET_GETALLOC_ERROR;
//                reply_packet->set_ret_code(ret_code);
//                return BLADE_SUCCESS;
//            }   
//            if(true == has_valid_lease(file_last_block->chunkId))
//            {   
//                LOGV(LL_ERROR, "last block is writing!");
//                ret_code = RET_LAST_BLOCK_NO_COMPLETE;
//                reply_packet->set_ret_code(ret_code);
//                return BLADE_SUCCESS;
//            }   
//        }  
//    }
    int64_t dir_id = (meta_tree_.GetDentry(file_attr->fid))->getDir();
    LOGV(LL_INFO, "file_name: %s", ((meta_tree_.GetDentry(file_attr->fid))->getName()).c_str());
    int err = meta_tree_.Rename(dir_id, (meta_tree_.GetDentry(file_attr->fid))->getName(), dst_path, src_path, false);
    if (0 != err)
    {
        if (-ENOENT == err || -ENOTDIR == err || -EINVAL == err || -EISDIR == err)
	{
            ret_code = RET_INVALID_PARAMS;
	}
        else if (-EEXIST == err)
	{
            ret_code = RET_DST_PATH_EXIST;
	}
        else if (-ENOTEMPTY == err)
	{
            ret_code = RET_DIR_NOT_EMPTY;
	}
        else if (-EPERM == err)
	{
            ret_code = RET_NOT_PERMITTED;
	}

    }
    else
    {
        ret_code = RET_SUCCESS;
    }

    reply_packet->set_ret_code(ret_code);
    return BLADE_SUCCESS;
}

int32_t MetaManager::do_delete_file(BladePacket *blade_packet)
{
    LOGV(LL_INFO, "********************IN DO DELETEFILE**********************");
    DeleteFilePacket *packet = static_cast<DeleteFilePacket *>(blade_packet);
    int64_t parent_id = packet->parent_id();
    string fname = packet->file_name();
    string path = packet->path();
    LOGV(LL_INFO, "parent_id:%ld, file_name:%s, path:%s", parent_id, fname.c_str(), path.c_str());

    string file_name;
    int64_t dir_id;
    MetaFattr * file_attr = NULL;
    if (0 != fname.empty())
    {
        MetaDentry *dentry = meta_tree_.GetDentry(parent_id); //parent_id = file_id
        if (0 == dentry)
	{
            LOGV(LL_ERROR, "file don't exist!");
            return BLADE_ERROR;
        }    
        dir_id = dentry->getDir();
        file_name = dentry->getName();
        file_attr = meta_tree_.GetFattr(parent_id);
    }
    else
    {
        string father_dir;
        if (bladestore::common::split_string(fname, '/', father_dir, file_name))
	{
            LOGV(LL_INFO, "father_dir: %s, file_name:%s, has to lookupPath", father_dir.c_str(), file_name.c_str());
            MetaFattr *dir_attr = meta_tree_.LookupPath(parent_id, father_dir);
            if (0 == dir_attr)
	    {
                LOGV(LL_ERROR, "father_dir do not exist!");
                return BLADE_ERROR;
            }    
            dir_id = dir_attr->fid;
        }    
        else
	{
            dir_id = parent_id;
            file_name = fname;
        }    
        file_attr = meta_tree_.Lookup(dir_id, file_name);
    }    
    if (NULL == file_attr)
    {
        LOGV(LL_ERROR, "file don't exist!");
        return BLADE_ERROR;
    }
    if (BLADE_DIR == file_attr->type)
    {
        int err = meta_tree_.Rmdir(dir_id, file_name, path);
        if (0 != err)
	{
            if (-EPERM == err)
	    {
                LOGV(LL_ERROR, "Not permitted!");
            }
	    else if (-ENOENT == err)
	    {
                LOGV(LL_ERROR, "dir not exist!");
            }
	    else if (-ENOTEMPTY == err)
	    {
                LOGV(LL_ERROR, "dir not empty!");
            }
	    else if (-ENOTDIR == err)
	    {
                LOGV(LL_ERROR, "not dir!");
            }
            return BLADE_ERROR;
        }
    }
    else if (BLADE_FILE == file_attr->type)
    {
        //remove from b tree and map!
        int err = meta_tree_.Remove(dir_id, file_name, path, NULL, is_master());
        if (0 != err)
	{
            if (-ENOENT == err)
	    {
                LOGV(LL_ERROR, "file not exist!");
            }
	    else if (-EISDIR == err)
	    {
                LOGV(LL_ERROR, "not file!");
            }
            return BLADE_ERROR;
        }
    } 
    else
    {
        LOGV(LL_ERROR, "error type!");
        return BLADE_ERROR;
    }
    return BLADE_SUCCESS;
}

int32_t MetaManager::meta_blade_delete_file(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "**********************IN DELETEFILE************************");        
	DeleteFilePacket *packet = static_cast<DeleteFilePacket *>(blade_packet);
    int64_t parent_id = packet->parent_id();
    string fname = packet->file_name();
    string path = packet->path();
    LOGV(LL_INFO, "parent_id:%ld, file_name:%s, path:%s", parent_id, fname.c_str(), path.c_str());

    int16_t ret_code = BLADE_ERROR;
    blade_reply_packet = new DeleteFileReplyPacket(ret_code);
    DeleteFileReplyPacket *reply_packet = static_cast<DeleteFileReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());
   
    if ('/' == fname[0])
    {
        LOGV(LL_ERROR, "fname is begin with /");
        ret_code = RET_INVALID_PARAMS;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
        AtomicInc64(&Singleton<StatManager>::Instance().delete_error_times_);
        return BLADE_SUCCESS;
    }
    string file_name;
    int64_t dir_id;
    MetaFattr * file_attr = NULL;
    if (0 != fname.empty())
    {
        MetaDentry *dentry = meta_tree_.GetDentry(parent_id); //parent_id = file_id
        if (0 == dentry)
	{
            LOGV(LL_ERROR, "file don't exist!");
            ret_code = RET_FILE_NOT_EXIST;
            reply_packet->set_ret_code(ret_code);
            AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_); 
            AtomicInc64(&Singleton<StatManager>::Instance().delete_error_times_);
            return BLADE_SUCCESS;
        }
        dir_id = dentry->getDir();
        file_name = dentry->getName();
        file_attr = meta_tree_.GetFattr(parent_id);
    }
    else
    {
        string father_dir;
        if (bladestore::common::split_string(fname, '/', father_dir, file_name)) {
            LOGV(LL_INFO, "father_dir: %s, file_name:%s, has to lookupPath", father_dir.c_str(), file_name.c_str());
            MetaFattr *dir_attr = meta_tree_.LookupPath(parent_id, father_dir);
            if(0 == dir_attr)
	    {
                LOGV(LL_ERROR, "father_dir do not exist!");
                ret_code = RET_DIR_NOT_EXIST;
                reply_packet->set_ret_code(ret_code);
                AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
                AtomicInc64(&Singleton<StatManager>::Instance().delete_error_times_);
                return BLADE_SUCCESS;
            }
            dir_id = dir_attr->fid;
        }
        else
	{
            dir_id = parent_id;
            file_name = fname;
        }
        file_attr = meta_tree_.Lookup(dir_id, file_name);
    }

    if (NULL == file_attr)
    {
        LOGV(LL_ERROR, "file don't exist!");
        ret_code = RET_FILE_NOT_EXIST;
        reply_packet->set_ret_code(ret_code);
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
        AtomicInc64(&Singleton<StatManager>::Instance().delete_error_times_);
        return BLADE_SUCCESS;
    }
    if (BLADE_DIR == file_attr->type)
    {
        if (false != is_master()) {
            packet->Pack();
            int ret = nameserver_->get_blade_ns_log_manager()->GetLogWorker()->BladeDeleteFile(BLADE_LOG_DELETEFILE, reinterpret_cast <char *>(packet->content()), packet->length());
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "HA ERROR");
				return ret;
			}
        }
        int err = meta_tree_.Rmdir(dir_id, file_name, path);
        if (0 != err)
        {
            if (-EPERM == err)
	    {
                ret_code = RET_NOT_PERMITTED;
	    }
            else if (-ENOENT == err)
	    {
                ret_code = RET_DIR_NOT_EXIST;
	    }
            else if (-ENOTEMPTY == err)
	    {
                ret_code = RET_DIR_NOT_EMPTY;
	    }
            else if (-ENOTDIR == err)
    	    {
                ret_code = RET_NOT_DIR;
    	    }

            AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
            AtomicInc64(&Singleton<StatManager>::Instance().delete_error_times_);
        }
        else
	{ 
            ret_code = RET_SUCCESS;
            AtomicDec64(&Singleton<StatManager>::Instance().dir_nums_);
        }
    }
    else if (BLADE_FILE == file_attr->type)
    {
        //test if file's last block is wrting!
        if (true == has_file_valid_lease(file_attr->fid))
        {
            LOGV(LL_ERROR, "last block is writing!");
            ret_code = RET_LAST_BLOCK_NO_COMPLETE;
            reply_packet->set_ret_code(ret_code);
            AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
            AtomicInc64(&Singleton<StatManager>::Instance().delete_error_times_);
            return BLADE_SUCCESS;
        }
        
        if (false != is_master())
	{
            packet->Pack();
            int ret = nameserver_->get_blade_ns_log_manager()->GetLogWorker()->BladeDeleteFile(BLADE_LOG_DELETEFILE, reinterpret_cast <char *>(packet->content()), packet->length());
			if (BLADE_SUCCESS != ret)
			{
				LOGV(LL_ERROR, "HA ERROR");
				return ret;
			}
        }

        //remove from b tree and map! 
        int err = meta_tree_.Remove(dir_id, file_name, path, NULL, is_master());
        if (-ENOENT == err)
	{
            ret_code = RET_FILE_NOT_EXIST;
            AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
            AtomicInc64(&Singleton<StatManager>::Instance().delete_error_times_);
        }
	else if (-EISDIR == err)
	{
            ret_code = RET_NOT_FILE;
            AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_); 
            AtomicInc64(&Singleton<StatManager>::Instance().delete_error_times_);
        }
	else
	{ 
            ret_code = RET_SUCCESS;
            AtomicDec64(&Singleton<StatManager>::Instance().file_nums_);
        }
    }
    else 
    {
        ret_code = RET_ERROR_FILE_TYPE;
        AtomicInc64(&Singleton<StatManager>::Instance().write_error_times_);
        AtomicInc64(&Singleton<StatManager>::Instance().delete_error_times_);
    }
    reply_packet->set_ret_code(ret_code);  
    return BLADE_SUCCESS;
}

int32_t MetaManager::meta_blade_renew_lease(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "**********************************IN RENEWLEASE*************************************");
    RenewLeasePacket *packet = static_cast<RenewLeasePacket *>(blade_packet);
    int64_t block_id = packet->get_block_id();
    LOGV(LL_INFO, "block_id: %ld", block_id);

    int16_t ret_code = BLADE_ERROR;
    blade_reply_packet = new RenewLeaseReplyPacket(ret_code);
    RenewLeaseReplyPacket *reply_packet = static_cast<RenewLeaseReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());

    //RenewLease
    int err = nameserver_->get_lease_manager().RenewLease(block_id);
    if (0 != err)
    {
        LOGV(LL_ERROR, "block don't exist!");
        ret_code = RET_BLOCK_NOT_EXIST;
    }
    else
    { 
        ret_code = RET_SUCCESS;
    }

    reply_packet->set_ret_code(ret_code);
    return BLADE_SUCCESS;
}

bool MetaManager::is_master()
{
	return nameserver_->is_master();
}

bool MetaManager::has_valid_lease(int64_t block_id)
{
	return nameserver_->get_lease_manager().has_valid_lease(block_id);
}

bool MetaManager::has_file_valid_lease(int64_t file_id)
{
    return nameserver_->get_lease_manager().has_file_valid_lease(file_id);
}

}//end of namespace nameserver
}//end of namespace bladestore
