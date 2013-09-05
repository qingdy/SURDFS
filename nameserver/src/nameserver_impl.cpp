#include "nameserver_impl.h"
#include "lease_manager.h"
#include "nameserver_task_process.h"
#include "blade_meta_manager.h"
#include "monitor_manager.h"
#include "blade_nameserver_handler.h"
#include "blade_client_manager.h"
#include "blade_log_entry.h"
#include "blade_net_util.h"
#include "blade_ns_log_worker.h"
#include "blade_ns_log_manager.h"
#include "blade_slave_mgr.h"
#include "time_util.h"
#include "blade_role_mgr.h"
#include "stat_manager.h"
#include "singleton.h"
#include "utility.h"
#include "btree_checkpoint.h"
#include "file_system_image.h"


using namespace bladestore::message;

namespace bladestore
{
namespace nameserver
{

AmFrame::EndPointOptions global_options;

const char * NameServerImpl::FS_IMAGE_EXT = "fsimage";
const char * NameServerImpl::BTREE_EXT = "btree";

NameServerImpl::NameServerImpl() : lease_manager_(this), meta_manager_(this), heartbeat_manager_thread_(meta_manager_), check_thread_(this, meta_manager_), ns_destroy_flag_(false)
{
	blade_ns_log_manager_ = NULL;
	blade_slave_mgr_ = NULL;
	blade_ns_role_mgr_ = NULL;
	blade_client_manager_ = NULL;
	nameserver_task_process_ = NULL;
	global_options.set_max_packet_length(5*1024*1024L);
	::pandora::Log::logger_.SetLogPrefix("../log", "nameserver_log");
    ::pandora::Log::logger_.set_max_file_size(100000000);
    ::pandora::Log::logger_.set_max_file_count(20);
}

NameServerImpl::~NameServerImpl()
{
	if (NULL != blade_ns_log_manager_)
	{	
		delete blade_ns_log_manager_;
	}
	blade_ns_log_manager_ = NULL;
	
	if (NULL != blade_slave_mgr_)
	{
		delete blade_slave_mgr_;
	}
	blade_slave_mgr_ = NULL;

	if (NULL != blade_ns_role_mgr_)
	{
		delete blade_ns_role_mgr_;
	}
	blade_ns_role_mgr_ = NULL;
	
	if (NULL != blade_client_manager_)
	{
		delete blade_client_manager_;
	}
	blade_client_manager_ = NULL;

	if (NULL != monitor_manager_thread_)
	{
		delete monitor_manager_thread_;
	}
	monitor_manager_thread_ = NULL;

	if (NULL != nameserver_task_process_)
	{
		delete nameserver_task_process_;
	}
	nameserver_task_process_ = NULL;
}

int NameServerImpl::Initialize()
{
	int ret = BLADE_SUCCESS;

    monitor_manager_thread_ = new MonitorManager(this->meta_manager().get_layout_manager());
	blade_ns_log_manager_ = new BladeNsLogManager();
	blade_slave_mgr_ = new BladeSlaveMgr();
	blade_ns_role_mgr_ = new BladeRoleMgr();
	blade_client_manager_ = new BladeClientManager();
	nameserver_task_process_ = new NameServerTaskProcess(this, meta_manager_);

	nameserver_task_process_->Init(blade_ns_log_manager_, blade_slave_mgr_);
	
	meta_manager_.Init();

    monitor_manager_thread_->Start();

	if (BLADE_SUCCESS == ret) 
	{
		ret = blade_client_manager_->Initialize();
	}

	std::string vip_str;

	if (BLADE_SUCCESS == ret)
	{
		vip_str = Singleton<BladeNameServerParam>::Instance().get_vip_str();
		LOGV(LL_DEBUG, "vip is : %s", vip_str.c_str());
		if (vip_str.length() == 0) 
		{
			LOGV(LL_ERROR, "vip of nameserver is not set" );
			ret = BLADE_INVALID_ARGUMENT;
		}
	}

	uint32_t vip = BladeNetUtil::GetAddr(vip_str.c_str());

	LOGV(LL_DEBUG , "vip is : %d", vip);
	if (BLADE_SUCCESS == ret)
	{
		std::string local_addr = Singleton<BladeNameServerParam>::Instance().get_local_addr();
		self_addr_ = BladeNetUtil::StringToAddr(local_addr.c_str());
	}

	if (BLADE_SUCCESS == ret)
	{
		int64_t log_sync_timeout = Singleton<BladeNameServerParam>::Instance().get_log_sync_timeout();
		int64_t lease_interval = Singleton<BladeNameServerParam>::Instance().get_lease_interval();
		int64_t lease_reserved_time = Singleton<BladeNameServerParam>::Instance().get_lease_reserved_time();
		ret = blade_slave_mgr_->Init(vip, blade_client_manager_, log_sync_timeout, lease_interval, lease_reserved_time);
		if (BLADE_SUCCESS != ret)
		{
			LOGV(LL_WARN, "failed to init slave manager, err=%d", ret);
		}
	}

	if (BLADE_SUCCESS == ret)
	{
		void* args = reinterpret_cast<void*>(READ_THREAD_FLAG);
		int read_thread_count = Singleton<BladeNameServerParam>::Instance().get_read_thread_size();
		read_packet_queue_thread_.set_thread_parameter(read_thread_count, nameserver_task_process_, args);
        LOGV(LL_DEBUG, "read_thread_count : %d", read_thread_count);

		args = reinterpret_cast<void*>(WRITE_THREAD_FLAG);
		write_packet_queue_thread_.set_thread_parameter(1, nameserver_task_process_, args); 

		args = reinterpret_cast<void*>(LOG_THREAD_FLAG);
		log_packet_queue_thread_.set_thread_parameter(1, nameserver_task_process_, args);
	}

	if (BLADE_SUCCESS == ret)
	{
		if (BladeNetUtil::IsLocalAddr(vip))
		{
			LOGV(LL_INFO, "I am holding the VIP, set role to MASTER");
			blade_ns_role_mgr_->SetRole(BladeRoleMgr::MASTER);
            Singleton<StatManager>::Instance().stat_ = "MASTER";

		}
		else
		{
			LOGV(LL_INFO, "I am not holding the VIP, set role to SLAVE");
			blade_ns_role_mgr_->SetRole(BladeRoleMgr::SLAVE);
            Singleton<StatManager>::Instance().stat_ = "SLAVE";
		}

		int32_t port = Singleton<BladeNameServerParam>::Instance().get_ns_port();

		ns_master_ = BladeNetUtil::IPToAddr(vip, port);
		ns_master_str_ = BladeNetUtil::AddrToString(ns_master_);

		int64_t vip_check_period_us = Singleton<BladeNameServerParam>::Instance().get_vip_check_peroid(); 

		ret = blade_check_runnable_.Init(blade_ns_role_mgr_, vip, vip_check_period_us, blade_client_manager_, ns_master_, self_addr_);
	}

    Singleton<StatManager>::Instance().Start();

	LOGV(LL_INFO, "nameserver worker init, ret=%d", ret);
	return ret;
}

//HA需要实现的函数
int NameServerImpl::Start()
{
	int ret = BLADE_ERROR;

	BladeRoleMgr::Role role = blade_ns_role_mgr_->GetRole();

	if (BladeRoleMgr::MASTER == role)
	{
		ret = start_as_master();
	}
	else if (BladeRoleMgr::SLAVE == role)
	{
		ret = start_as_slave();
	}
	else
	{
		ret = BLADE_INVALID_ARGUMENT;
		LOGV(LL_ERROR, "unknow role: %d, nameserver start failed", role);
	}
	return ret;
}

//作为master启动
int NameServerImpl::start_as_master()
{
	int ret = BLADE_ERROR;

	LOGV(LL_INFO, "[NOTICE] master start step1");
	ret = blade_ns_log_manager_->Init(this, blade_slave_mgr_);
	if (BLADE_SUCCESS == ret)
	{
		// try to replay log
		LOGV(LL_INFO, "[NOTICE] master start step2");
		ret = blade_ns_log_manager_->ReplayLog();
		if (ret != BLADE_SUCCESS)
		{
			LOGV(LL_ERROR, "[NOTICE] master replay log failed, err=%d", ret);
		}
	}


	if (BLADE_SUCCESS == ret)
	{
		LOGV(LL_INFO, "[NOTICE] master start step3");
		int64_t listen_fd = (BladeCommonData::amframe_).AsyncListen(ns_master_str_.c_str(), new BladeNsListenHandler(*this), global_options);
		if (listen_fd < 0)
		{
			LOGV(LL_ERROR, "listen error errno = %d", listen_fd);
			exit(0);
		}

		void* args = reinterpret_cast<void*>(CHECK_DS_FLAG);
		check_ds_thread_.Start(&check_thread_, args);
		args = reinterpret_cast<void*>(CHECK_BLOCK_FLAG);
		check_block_thread_.Start(&check_thread_, args);
		int64_t network_timeout_ = Singleton<BladeNameServerParam>::Instance().get_network_timeout();

		blade_ns_role_mgr_->SetState(BladeRoleMgr::ACTIVE);

    		lease_manager_.Start();

		read_packet_queue_thread_.Start();
		write_packet_queue_thread_.Start();
		blade_check_runnable_.Start();

		LOGV(LL_INFO, "[NOTICE] master start-up finished");

		// wait finish
		for (;;)
		{
			if (BladeRoleMgr::STOP == blade_ns_role_mgr_->GetState() || BladeRoleMgr::ERROR == blade_ns_role_mgr_->GetState())
			{
				LOGV(LL_INFO, "role manager change state, stat=%d", blade_ns_role_mgr_->GetState());
				break;
			}
			usleep(10 * 1000); // 10 ms
		}
	}

	LOGV(LL_INFO, "[NOTICE] going to quit");
	Stop();

	return ret;
}

//作为slave启动
int NameServerImpl::start_as_slave()
{
	int err = BLADE_SUCCESS;
	
	std::string local_addr = Singleton<BladeNameServerParam>::Instance().get_local_addr();
	ListenSocketHandler * listen_socket_handler = new BladeNsListenHandler(*this);
	int64_t listen_fd = (BladeCommonData::amframe_).AsyncListen(local_addr.c_str(), listen_socket_handler, global_options);
	if (listen_fd < 0)
	{
		LOGV(LL_ERROR, "listen error errno = %d", listen_fd);
		exit(0);
	}
	AmFrame::EndPoint listen_endpoint = listen_socket_handler->end_point();

	log_packet_queue_thread_.Start();

	err = blade_ns_log_manager_->Init(this , blade_slave_mgr_);

	BladeFetchParam fetch_param;

	if (BLADE_SUCCESS == err)
	{
		err = slave_register(fetch_param);
	}

	if (BLADE_SUCCESS == err)
	{
		std::string vip_str = Singleton<BladeNameServerParam>::Instance().get_vip_str();
		err = blade_fetch_thread_.Init(vip_str, blade_ns_log_manager_->GetLogDirPath(), fetch_param, blade_ns_role_mgr_, &log_replay_thread_);
		if (BLADE_SUCCESS == err)
		{
			int64_t log_limit = Singleton<BladeNameServerParam>::Instance().get_log_limit();
			blade_fetch_thread_.SetLimitRate(log_limit);
			blade_fetch_thread_.AddCkptExt(FS_IMAGE_EXT); // add fsimage checkpoint file
			blade_fetch_thread_.AddCkptExt(BTREE_EXT); // add btree checkpoint file
			blade_fetch_thread_.LogManager(blade_ns_log_manager_);
			blade_fetch_thread_.Start();

			if (fetch_param.fetch_ckpt_)
			{
				err = blade_fetch_thread_.WaitRecoverDone();
			}
		}
		else
		{
			LOGV(LL_ERROR, "failed to init fetch log thread");
		}
	}

	if (BLADE_SUCCESS == err)
	{
		blade_ns_role_mgr_->SetState(BladeRoleMgr::ACTIVE);
	}

	if (BLADE_SUCCESS == err)
	{
		int64_t replay_wait_time = Singleton<BladeNameServerParam>::Instance().get_replay_wait_time();
		err = log_replay_thread_.Init(blade_ns_log_manager_->GetLogDirPath(), fetch_param.min_log_id_, blade_ns_role_mgr_, replay_wait_time);
		if (BLADE_SUCCESS == err)
		{
			log_replay_thread_.SetLogManager(blade_ns_log_manager_);
			log_replay_thread_.Start();
			LOGV(LL_INFO, "slave log_replay_thread started");
		}
		else
		{
			LOGV(LL_ERROR, "failed to start log replay thread");
		}
	}

	if (BLADE_SUCCESS == err)
	{
		blade_check_runnable_.Start();
		LOGV(LL_INFO, "slave check_thread started");

		while (BladeRoleMgr::SWITCHING != blade_ns_role_mgr_->GetState())
		{
			if (BladeRoleMgr::STOP == blade_ns_role_mgr_->GetState() || BladeRoleMgr::ERROR == blade_ns_role_mgr_->GetState())            
			{
				break;
			}
			usleep(10 * 1000); // 10 ms
		}

		if (BladeRoleMgr::SWITCHING == blade_ns_role_mgr_->GetState())
		{
			(BladeCommonData::amframe_).CloseEndPoint(listen_endpoint);
			listen_socket_handler = NULL;

			int64_t listen_fd = (BladeCommonData::amframe_).AsyncListen(ns_master_str_.c_str(), new BladeNsListenHandler(*this), global_options);
			if (listen_fd < 0)
			{
				LOGV(LL_ERROR, "listen error errno = %d", listen_fd);
				exit(0);
			}

			LOGV(LL_WARN, "nameserver slave begin switch to master");

			blade_ns_role_mgr_->SetRole(BladeRoleMgr::MASTER);
			LOGV(LL_INFO, "[NOTICE] set role to master");
            Singleton<StatManager>::Instance().stat_ = "MASTER";

			log_packet_queue_thread_.Stop(true);
			log_packet_queue_thread_.Wait();

			// log replay thread will stop itself when
			// role switched to MASTER and nothing
			// more to replay
			blade_fetch_thread_.Stop();
			blade_fetch_thread_.Wait();
			log_replay_thread_.Stop();
			log_replay_thread_.Wait();

			read_packet_queue_thread_.Start();
			write_packet_queue_thread_.Start();

			void* args = reinterpret_cast<void*>(CHECK_DS_FLAG);
			check_ds_thread_.Start(&check_thread_, args);
			args = reinterpret_cast<void*>(CHECK_BLOCK_FLAG);
			check_block_thread_.Start(&check_thread_, args);

			blade_ns_role_mgr_->SetState(BladeRoleMgr::ACTIVE);
			LOGV(LL_INFO, "set stat to ACTIVE");

    		lease_manager_.Start();

			is_registered_ = false;
			LOGV(LL_WARN, "nameserver slave switched to master");

			for (;;)
			{
				if (BladeRoleMgr::STOP == blade_ns_role_mgr_->GetState() || BladeRoleMgr::ERROR == blade_ns_role_mgr_->GetState())
				{
					LOGV(LL_INFO, "role manager change state, stat=%d", blade_ns_role_mgr_->GetState());
					break;
				}
				usleep(10 * 1000);
			}
		}
	}
	LOGV(LL_INFO, "[NOTICE] going to quit");
	Stop();
	return err;
}

//测试时使用
int NameServerImpl::start_as_standalone()
{
	int err = BLADE_SUCCESS;
	// start read and write thread
	if (BLADE_SUCCESS == err)
	{    
		read_packet_queue_thread_.Start();
		write_packet_queue_thread_.Start();
	}    

	// modify standalone status
	if (BLADE_SUCCESS == err) 
	{
		blade_ns_role_mgr_->SetState(BladeRoleMgr::ACTIVE);
	}

	LOGV(LL_INFO, "start service, role=standalone, err=%d", err);
	// wait finish
	if (BLADE_SUCCESS == err) 
	{
		read_packet_queue_thread_.Wait();
		write_packet_queue_thread_.Wait();
	}

	return err; 
}

int NameServerImpl::Wait()
{

}

int NameServerImpl::Stop()
{
	int ret = BLADE_SUCCESS;
	ns_destroy_flag_ = true;
	exit(1);
	return ret;
}

int NameServerImpl::do_check_point(const uint64_t ckpt_id)
{
	int ret = BLADE_SUCCESS;
	int err = 0; 

	//step1: construct btree checkpoint file	
	char filename[BLADE_MAX_FILENAME_LENGTH];
	const char* log_dir = blade_ns_log_manager_->GetLogDirPath();
	err = snprintf(filename, BLADE_MAX_FILENAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, BTREE_EXT);
	if (err < 0 || err >= BLADE_MAX_FILENAME_LENGTH)
	{    
		LOGV(LL_ERROR, "generate btree checkpoint file name [%s] failed, error: %s", filename, strerror(errno));
		ret = BLADE_ERROR;
	}

	LOGV(LL_INFO, "generate btree checkpoint file %s, start time :%ld", filename, TimeUtil::GetTime());
	if (BLADE_SUCCESS == ret)
	{
		ret = (meta_manager_.get_btree_checkpoint())->do_cp(filename);
		if (ret != BLADE_SUCCESS)
		{
			LOGV(LL_ERROR, "write btree to file [%s] failed, err=%d", filename, ret);
		}
	}

	err = snprintf(filename, BLADE_MAX_FILENAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, FS_IMAGE_EXT);
	if (err < 0 || err >= BLADE_MAX_FILENAME_LENGTH)
	{    
		LOGV(LL_ERROR, "generate fsimage file name [%s] failed, error: %s", filename, strerror(errno));
		ret = BLADE_ERROR;
	}

	if (BLADE_SUCCESS == ret)
	{
		ret = (meta_manager_.get_fs_image())->save_image(filename);
		if (ret != BLADE_SUCCESS)
		{
			LOGV(LL_ERROR, "write fsimage list to file [%s] failed, err=%d", filename, ret);
		}
	}
	LOGV(LL_INFO, "generate btree checkpoint file %s, end time :%ld", filename, TimeUtil::GetTime());
	return ret;
}

int NameServerImpl::recover_from_check_point(const int server_status, const uint64_t ckpt_id)
{
	int ret = BLADE_SUCCESS;

	server_status_ = server_status;
	LOGV(LL_INFO, "server status recover from check point is %d", server_status_);

	const char* log_dir = blade_ns_log_manager_->GetLogDirPath();
	char filename[BLADE_MAX_FILENAME_LENGTH];

	int err = 0; 
	err = snprintf(filename, BLADE_MAX_FILENAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, BTREE_EXT);
	if (err < 0 || err >= BLADE_MAX_FILENAME_LENGTH)
	{
		LOGV(LL_ERROR, "generate btree checkpoint file name [%s] failed, error: %s", filename, strerror(errno));
		ret = BLADE_ERROR;
	}
	LOGV(LL_INFO, "recover btree checkpoint file %s, start time :%ld", filename, TimeUtil::GetTime());

	if (BLADE_SUCCESS == ret)
	{
		ret = (meta_manager_.get_btree_checkpoint())->Rebuild(filename, 3);
		if (ret != BLADE_SUCCESS)
		{    
			LOGV(LL_ERROR, "recover btree from file [%s] failed, err=%d", filename, ret);
		}
	}

	err = snprintf(filename, BLADE_MAX_FILENAME_LENGTH, "%s/%lu.%s", log_dir, ckpt_id, FS_IMAGE_EXT);
	if (err < 0 || err >= BLADE_MAX_FILENAME_LENGTH)
	{
		LOGV(LL_ERROR, "generate fsimage file name [%s] failed, error: %s", filename, strerror(errno));
		ret = BLADE_ERROR;
	}

	if (BLADE_SUCCESS == ret)
	{                                                   
		ret = (meta_manager_.get_fs_image())->recovery_from_image(filename);
		if (ret != BLADE_SUCCESS)
		{
			LOGV(LL_ERROR, "recover fsiamge from file [%s] failed, err=%d", filename, ret);
		}
	}
	LOGV(LL_INFO, "recover btree checkpoint file %s, end time :%ld", filename, TimeUtil::GetTime());

	LOGV(LL_INFO, "recover finished with ret: %d, ckpt_id: %d", ret, ckpt_id);
	return ret;
}

int NameServerImpl::slave_register(BladeFetchParam & fetch_param)
{
	int err = BLADE_SUCCESS;
	int64_t num_times = Singleton<BladeNameServerParam>::Instance().get_register_times();
	int64_t timeout = Singleton<BladeNameServerParam>::Instance().get_register_timeout();

	err = BLADE_RESPONSE_TIME_OUT;
	for (int64_t i = 0; BladeRoleMgr::STOP != blade_ns_role_mgr_->GetState() && BLADE_RESPONSE_TIME_OUT == err && i < num_times; ++i)
	{
		// slave register
		err = blade_client_manager_->slave_register(ns_master_, self_addr_, fetch_param, 20000000);
		if (BLADE_RESPONSE_TIME_OUT == err)
		{
			LOGV(LL_INFO, "slave register timeout, i=%ld, err=%d", i, err);
		}
	}

	if (BladeRoleMgr::STOP == blade_ns_role_mgr_->GetState())
	{
		LOGV(LL_INFO, "the slave is stopped manually.");
		err = BLADE_ERROR;
	}
	else if (BLADE_RESPONSE_TIME_OUT == err)
	{
		LOGV(LL_WARN, "slave register timeout, num_times=%ld, timeout=%ldus", num_times, timeout);
		err = BLADE_RESPONSE_TIME_OUT;
	}
	else if (BLADE_SUCCESS != err)
	{
		LOGV(LL_WARN, "Error occurs when slave register, err=%d", err);
	}

	if (BLADE_SUCCESS == err)                             
	{
		int64_t renew_lease_timeout = 1000 * 1000L;
		blade_check_runnable_.SetRenewLeaseTimeout(renew_lease_timeout);
	}

	LOGV(LL_INFO, "slave register, self=[%s], min_log_id=%ld, max_log_id=%ld, ckpt_id=%lu, err=%d", BladeNetUtil::AddrToString(self_addr_).c_str(), fetch_param.min_log_id_, fetch_param.max_log_id_, fetch_param.ckpt_id_, err);

	if (BLADE_SUCCESS == err)
	{
		is_registered_ = true;
	}

	return err;
}

bool NameServerImpl::is_master()
{
	return (blade_ns_role_mgr_->GetRole() == BladeRoleMgr::MASTER)&&(blade_ns_role_mgr_->GetState() == BladeRoleMgr::ACTIVE);
}

int NameServerImpl::get_server_status()
{
	server_status_ = blade_ns_role_mgr_->GetRole();
	return server_status_;
}

bool NameServerImpl::get_ns_destroy_flag()
{
	return ns_destroy_flag_;
}

CThreadMutex & NameServerImpl::get_check_mutex()
{
	return check_mutex_;	
}

BladePacketQueueThread & NameServerImpl::get_read_packet_queue_thread()
{
	return read_packet_queue_thread_;
}

BladePacketQueueThread & NameServerImpl::get_write_packet_queue_thread()
{
	return write_packet_queue_thread_;
}

BladePacketQueueThread & NameServerImpl::get_log_packet_queue_thread()
{
	return log_packet_queue_thread_;
}

HeartbeatManager & NameServerImpl::get_heartbeat_manager_thread()
{
	return heartbeat_manager_thread_;
}

MetaManager & NameServerImpl::meta_manager()
{
	return meta_manager_;
}

LeaseManager & NameServerImpl::get_lease_manager()
{
	return lease_manager_;
}

BladeNsLogManager * NameServerImpl::get_blade_ns_log_manager()
{
	return blade_ns_log_manager_;	
}

BladeClientManager * NameServerImpl::get_blade_client_manager()
{
	return blade_client_manager_;
}

CheckThread & NameServerImpl::get_check_thread()
{
	return check_thread_;
}

}//end of namespace nameserver
}//end of namespace bladestore
