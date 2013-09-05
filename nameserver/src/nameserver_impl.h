/*
 *
 *version : 1.0
 *author  : chen yunyun, funing
 *date    : 2012-4-8
 *
 */
#ifndef BLADESTORE_NAMESERVER_NAMESERVER_IMPL_H
#define BLADESTORE_NAMESERVER_NAMESERVER_IMPL_H

#include "blade_packet_queue_thread.h"
#include "heartbeat_manager.h"
#include "blade_nameserver_param.h"
#include "blade_thread_buffer.h"
#include "cthread.h"
#include "thread_mutex.h"
#include "check_thread.h"
#include "blade_data_buffer.h"
#include "blade_fetch_runnable.h"
#include "blade_ns_fetch_thread.h"
#include "blade_ns_log_replay.h"
#include "blade_check_runnable.h"
#include "blade_common_define.h"

using namespace bladestore::common;
using namespace pandora;
using namespace bladestore::message;
using namespace bladestore::ha;
using namespace bladestore::nameserver;

namespace bladestore
{
namespace ha
{

class BladeRoleMgr;
class BladeNsLogWorker;
class BladeNsLogManager;
class BladeSlaveMgr;

}//end of namespace ha
}//end of namespace bladestore

using namespace bladestore::ha;

namespace bladestore
{
namespace nameserver
{

extern AmFrame::EndPointOptions global_options;

class MonitorManager; 
class MetaManager;
class LeaseManager;
class NameServerTaskProcess;
class BladeClientManager;

//NameServer是server端的主要处理逻辑类
class NameServerImpl
{
public:
	NameServerImpl();

	virtual ~NameServerImpl();

	//nameserver初始化操作
	int Initialize();
	int Start();
	int Wait();
	int Stop();

	bool is_master();

	//checkpoint操作	
	int do_check_point(const uint64_t ckpt_id);
	int recover_from_check_point(const int server_status, const uint64_t ckpt_id);

	//对外暴露的get接口
	int get_server_status();
	virtual bool get_ns_destroy_flag();
	CThreadMutex & get_check_mutex();
	BladePacketQueueThread & get_read_packet_queue_thread();
    NameServerTaskProcess * get_nameserver_task_process(){return nameserver_task_process_;}
	BladePacketQueueThread & get_write_packet_queue_thread();
	BladePacketQueueThread & get_log_packet_queue_thread();
	HeartbeatManager & get_heartbeat_manager_thread();
    MetaManager & meta_manager();
	BladeNsLogManager * get_blade_ns_log_manager();
	LeaseManager & get_lease_manager();
	BladeClientManager * get_blade_client_manager();
	CheckThread & get_check_thread();

private:
	DISALLOW_COPY_AND_ASSIGN(NameServerImpl);

	int start_as_master();
	int start_as_slave();
	int start_as_standalone();

	int slave_register(BladeFetchParam & fetch_param);

private:	
	//租约管理线程	
	LeaseManager lease_manager_;
	MetaManager meta_manager_;
    MonitorManager * monitor_manager_thread_;

	//读写和slave日志任务队列
	NameServerTaskProcess * nameserver_task_process_;
	BladePacketQueueThread read_packet_queue_thread_;
	BladePacketQueueThread write_packet_queue_thread_;
	BladePacketQueueThread log_packet_queue_thread_;

	//心跳管理类
	HeartbeatManager heartbeat_manager_thread_;

	//检测block数目, DS状态的线程	
	CThread check_ds_thread_;
	CThread check_block_thread_;
	CheckThread  check_thread_;

	//主备nameserver检测vip
	BladeCheckRunnable blade_check_runnable_;

	BladeNsFetchThread blade_fetch_thread_;
	BladeNsLogReplay log_replay_thread_; 
	
	BladeNsLogManager * blade_ns_log_manager_;	
	BladeSlaveMgr * blade_slave_mgr_;
	BladeRoleMgr * blade_ns_role_mgr_;
	BladeClientManager * blade_client_manager_;

private:

	CThreadMutex check_mutex_;
	uint64_t ns_master_;
	std::string ns_master_str_;
	uint64_t self_addr_;
	int server_status_;
	const static char * FS_IMAGE_EXT;
	const static char * BTREE_EXT;

	bool is_registered_;

	//nameserver状态标记
	bool ns_destroy_flag_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
