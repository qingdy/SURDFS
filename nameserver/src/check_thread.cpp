#include "check_thread.h"
#include "singleton.h"
#include "blade_net_util.h"
#include "nameserver_impl.h"
#include "checkpoint_packet.h"
#include "leave_ds_packet.h"
#include "blade_nameserver_param.h"
#include "blade_meta_manager.h"

using namespace bladestore::message;

namespace bladestore
{
namespace nameserver
{

CheckThread::CheckThread(NameServerImpl * nameserver_impl, MetaManager & meta_manager) : meta_manager_(meta_manager), replicate_launcher_(meta_manager),redundant_launcher_(meta_manager), scanner_manager_(meta_manager), nameserver_impl_(nameserver_impl) 
{

}

CheckThread::~CheckThread()
{

}

void CheckThread::Run(CThread * thread, void * args)
{
	if ((void *)CHECK_DS_FLAG == args)
	{
		do_check_ds();
	}
	else if ((void *)CHECK_BLOCK_FLAG == args)
	{
		do_check_block();	
	}
}

int CheckThread::do_check_block()
{
    const int32_t CHECK_TASK = 2; 
    time_t last_check_time[CHECK_TASK] = { time(NULL) };
    bool need_check[CHECK_TASK] = { false };
    int32_t check_interval[CHECK_TASK] =
    {
		Singleton<BladeNameServerParam>::Instance().get_replicate_check_interval(),
		Singleton<BladeNameServerParam>::Instance().get_redundant_check_interval()
    };

	time_t last_checkpoint_time = time(NULL);

    std::set<int64_t> replicate_blocks;
    std::set<int64_t> redundant_blocks;
    time_t now_time = 0; 
    bool need_check_block = false;
	int32_t max_replicate_size = 100000; 
	int32_t max_redundant_size = 100000;

	while (false == nameserver_impl_->get_ns_destroy_flag())
	{
		now_time = time(NULL);			

		if (now_time - last_checkpoint_time >= Singleton<BladeNameServerParam>::Instance().get_checkpoint_interval())
		{
			last_checkpoint_time = now_time;
			CheckPointPacket * checkpoint_packet = new CheckPointPacket();
			nameserver_impl_->get_write_packet_queue_thread().Push(checkpoint_packet);
		}

		// calc time interval
		need_check_block = false;

		for (int32_t i = 0; i < CHECK_TASK; ++i)
		{
			need_check[i] = check_task_interval(now_time, last_check_time[i], check_interval[i]);
			if (need_check[i])
			{
				need_check_block = true;
			}
		}
		
		if (need_check_block)
		{
			LOGV(LL_DEBUG, "**************IN REPLICATE && REDUNDANT********************");
			//clear block set first
			replicate_blocks.clear();
			redundant_blocks.clear();
			BlockScannerManager::Scanner replicate_scanner(need_check[0], max_replicate_size, replicate_launcher_, replicate_blocks);
			BlockScannerManager::Scanner redundant_scanner(need_check[1], max_redundant_size, redundant_launcher_, redundant_blocks);

			scanner_manager_.add_scanner(0, &replicate_scanner);
			scanner_manager_.add_scanner(1, &redundant_scanner);
			nameserver_impl_->get_check_mutex().Lock();
			scanner_manager_.Run();
			nameserver_impl_->get_check_mutex().Unlock();
		}
		
	//	sleep(BladeNameServerParam::check_block_interval_);
		sleep(5);
	}
	return BLADE_SUCCESS;
}

int CheckThread::do_check_ds()
{
	while (false == nameserver_impl_->get_ns_destroy_flag())
	{
		nameserver_impl_->get_check_mutex().Lock();
		check_ds(TimeUtil::GetTime());
		nameserver_impl_->get_check_mutex().Unlock();
		sleep(2);
	}
	return BLADE_SUCCESS;
}

int CheckThread::check_ds(int64_t now)
{

    LOGV(LL_DEBUG,"In check ds");

	std::set<uint64_t> ds_dead_list;
	std::set<uint64_t>::iterator ds_dead_list_iter;
	int64_t expire_time = now - BladeNameServerParam::ds_dead_time_*4;

	//@notice read lock is acquired inside this function
	meta_manager_.get_layout_manager().check_ds(BladeNameServerParam::ds_dead_time_, ds_dead_list);
	LOGV(LL_DEBUG, "DS DEAD LIST SIZE : %d", ds_dead_list.size());
	uint64_t ds_id = 0;
	DataServerInfo * dataserver_info = NULL;
	ds_dead_list_iter = ds_dead_list.begin();
	for ( ; ds_dead_list_iter != ds_dead_list.end(); ds_dead_list_iter++)
	{
		ds_id = *ds_dead_list_iter;
		meta_manager_.get_layout_manager().get_server_mutex().rlock()->lock();
		dataserver_info = meta_manager_.get_layout_manager().get_dataserver_info(ds_id);	
		if (NULL == dataserver_info)
		{
			meta_manager_.get_layout_manager().get_server_mutex().rlock()->unlock();
			continue;
		}
		LOGV(LL_DEBUG, "TIME last_update_time : %ld , expire_time : %ld", dataserver_info->last_update_time_, expire_time);
		if (dataserver_info->last_update_time_ > expire_time /* && test_server_alive(ds_id)*/)
		{
			meta_manager_.get_layout_manager().get_server_mutex().rlock()->unlock();
			continue;
		}
		meta_manager_.get_layout_manager().get_server_mutex().rlock()->unlock();

		//@notice leave_ds 在内部加写锁, 为防止死锁, 在进入之前须释放锁 
		LeaveDsPacket * leave_ds_packet = new LeaveDsPacket();
		leave_ds_packet->set_ds_id(ds_id);
		nameserver_impl_->get_write_packet_queue_thread().Push(leave_ds_packet);
		//meta_manager_.leave_ds(ds_id);
	}
	return BLADE_SUCCESS;	
}

bool CheckThread::check_task_interval(time_t now_time, time_t& last_check_time, time_t interval)
{
	LOGV(LL_DEBUG, "*******************EXTRACT RESULT : %d INTERVAL : %d ************", now_time - last_check_time, interval );
	if (now_time - last_check_time >= interval)
	{    
		last_check_time = now_time;
		return true;
	}
	return false;
}

int CheckThread::block_received(BlockReceivedPacket * block_received_packet)
{
	int ret = BLADE_SUCCESS;
	int64_t block_id = block_received_packet->block_info().block_id();
	uint64_t ds_id = block_received_packet->ds_id();
	LOGV(LL_DEBUG, "block received , block id : %ld ,ds_id: %s", block_id, BladeNetUtil::AddrToString(ds_id).c_str());
	ret = replicate_launcher_.handle_replicate_complete(ds_id, block_id);	
	return ret;
}

}//end of namespace nameserver
}//end of namespace bladestore
