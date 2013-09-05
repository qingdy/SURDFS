/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-4-12
 *
 */
#ifndef BLADESTORE_NAMESERVER_PARAM_H
#define BLADESTORE_NAMESERVER_PARAM_H
#include <string>
#include <stdint.h>

#include "config.h"
#include "log.h"

namespace bladestore
{
namespace nameserver
{
using namespace pandora;
//定义nameserver的一些具体属性
class BladeNameServerParam
{
public:
	BladeNameServerParam();
	~BladeNameServerParam();

	int Init();

	//没有给出要加载的文件的时候， 从默认的config_file_name_中读取配置文件	
	int Load();

	int load_config(std::string filename);

	int64_t get_log_file_max_size()
	{
		return log_file_max_size_;
	}

	int64_t get_log_sync_type()
	{
		return log_sync_type_;
	}

	string get_log_dir()
	{
		return	log_dir_; 
	}
	
	string get_local_addr()
	{
		return local_addr_;
	}

	int get_trace_log_level()
	{
		return trace_log_level_;
	}
	
	int get_read_thread_size()
	{
		return read_thread_size_;	
	}

	std::string get_work_dir()
	{
		return work_dir_;
	}

	std::string get_dev_name()
	{
		return dev_name_;
	}
	
	int64_t get_log_sync_timeout()
	{
		return log_sync_timeout_;	
	}

	int64_t get_lease_interval()
	{
		return lease_interval_;
	}

	int64_t get_lease_reserved_time()
	{
		return lease_reserved_time_;
	}
	
	std::string get_vip_str()
	{
		return vip_str_;
	}
	
	int32_t get_ns_port()
	{
		return ns_port_;
	}
	
	int64_t get_vip_check_peroid()
	{
		return vip_check_peroid_;
	}

	int64_t get_network_timeout()
	{
		return network_timeout_;
	}

	int64_t get_log_limit()
	{
		return log_limit_;
	}

	int64_t get_replay_wait_time()
	{
		return replay_wait_time_;
	}

	int64_t get_register_times()
	{
		return register_times_;
	}

	int64_t get_register_timeout()
	{
		return register_timeout_;
	}

	int64_t get_checkpoint_interval()
	{
		return checkpoint_interval_;
	}

	int64_t get_lease_on()
	{
		return lease_on_;
	}

	int64_t get_replicate_check_interval()
	{
		return replicate_check_interval_;
	}
	
	int64_t get_redundant_check_interval()
	{
		return redundant_check_interval_;
	}

	int64_t get_block_check_interval()
	{
		return check_block_interval_;	
	}

    std::string stat_log()
    {
        return stat_log_;
    }

    int64_t stat_check_interval()
    {
        return stat_check_interval_;
    }

public:
	//读写线程相关定义
	const static int read_thread_packet_queue_size_ = 10000;
	const static int write_thread_packet_queue_size_ = 10000;
	const static int64_t ds_dead_time_ = 20000000;
	const static int64_t heart_interval_ = 10000000;

	
	const static int min_replication_ = 3;
	const static int max_replication  = 10;	
	const static int replicate_max_time_ = 300;
	const static int replicate_max_count_per_server_ = 20;
	const static int trace_log_level_ = LL_DEBUG;
	const static std::string config_file_name_;
	const static std::string address_;
	
private:
	Config blade_config_;

	int read_thread_size_ ;
	int64_t log_sync_timeout_;
	int64_t lease_interval_;
	int64_t lease_reserved_time_;
	int64_t lease_on_;

	std::string work_dir_;
	std::string vip_str_;
	std::string dev_name_;
	int32_t ns_port_;
	int64_t vip_check_peroid_;
	int64_t network_timeout_;
	int64_t log_limit_;
	std::string log_dir_;
	std::string local_addr_;
	int64_t log_file_max_size_;
	int64_t log_sync_type_;
	int64_t replay_wait_time_;
	int64_t register_times_;
	int64_t register_timeout_;
	int64_t checkpoint_interval_;

	int replicate_check_interval_;
	int redundant_check_interval_;
	int check_block_interval_;

    //for stat monitor
    std::string stat_log_;
    int64_t stat_check_interval_;

};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
