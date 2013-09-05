#include "blade_nameserver_param.h"
#include "blade_common_define.h"

using namespace bladestore::common;

namespace bladestore
{
namespace nameserver
{

const std::string  BladeNameServerParam::config_file_name_ = "../conf/nameserver.conf";
const std::string  BladeNameServerParam::address_= "127.0.0.1:9000";

BladeNameServerParam::BladeNameServerParam()
{
	Init();
}

BladeNameServerParam::~BladeNameServerParam()
{

}

int BladeNameServerParam::Init()
{
	int ret = BLADE_SUCCESS;
	ret = Load();
	if (BLADE_SUCCESS == ret)
	{
		read_thread_size_ = blade_config_.GetInt("nameserver", "read_thread_size", 10);
		if (read_thread_size_ <= 0 || read_thread_size_ > 100)
		{
			fprintf(stderr, "NameServer config file error: read_thread_size is illegal : %d, default is: 10, recommend between: 3 - 20.\n", read_thread_size_);
			ret = BLADE_ERROR;
		}
		
        log_file_max_size_ = blade_config_.GetInt("nameserver", "log_file_max_size", 64000000);
		if (log_file_max_size_ <= 0)
		{
			fprintf(stderr, "NameServer config file error: log_file_max_size is illegal <= 0 : %ld, default is: 640000000, recommend between:32000000 - 128000000 \n", log_file_max_size_);
			ret = BLADE_ERROR;
		}
		
        log_dir_ = blade_config_.GetString("nameserver", "log_dir", "./ns_log");
		
        log_sync_timeout_ = blade_config_.GetInt("nameserver", "log_sync_timeout", 500*1000);
		if (log_sync_timeout_ <= 0)
		{
			fprintf(stderr, "NameServer config file error: log_sync_timeout is illegal : %ld, default is: 500000, recommend between: 100000 - 1000000.\n", log_sync_timeout_);
			ret = BLADE_ERROR;
		}


		log_sync_type_ = blade_config_.GetInt("nameserver", "log_sync_type", 1);
		if (1 != log_sync_type_ && 0 != log_sync_type_)
		{
			fprintf(stderr, "NameServer config file error: log_sync_type is illegal : %ld, default is: 1, recommend is 0 or 1.\n", log_sync_type_);
			ret = BLADE_ERROR;
		}

		lease_interval_ = blade_config_.GetInt("nameserver", "lease_interval_us", 8*1000*1000);
		if (lease_interval_ <= 0)
		{
			fprintf(stderr, "NameServer config file error: lease_interval_us is illegal : %ld, default is: 8000000, recommend between: 2000000 - 20000000.\n", lease_interval_);
			ret = BLADE_ERROR;
		}


		lease_reserved_time_ = blade_config_.GetInt("nameserver", "lease_reserved_time_us", 5*1000*1000);
		if (lease_reserved_time_ <= 0)
		{
			fprintf(stderr, "NameServer config file error: lease_reserved_time_us is illegal : %ld, default is: 5000000, recommend between: 1000000 - 10000000.\n", lease_reserved_time_);
			ret = BLADE_ERROR;
		}
		
        vip_str_ = blade_config_.GetString("nameserver", "vip", "");
		if (0 == vip_str_.size())
		{	
			fprintf(stderr, "NameServer config file error: vip is not valid!\n");
			ret = BLADE_ERROR;
		}

		vip_check_peroid_ = blade_config_.GetInt("nameserver", "vip_check_period_us", 5*1000*1000);
		if (vip_check_peroid_ <= 0)
		{
			fprintf(stderr, "NameServer config file error: vip_check_period_us is illegal : %ld, default is: 5000000, recommend between: 1000000 - 10000000.\n", vip_check_peroid_);
			ret = BLADE_ERROR;
		}
		
        work_dir_ = blade_config_.GetString("nameserver", "work_dir", "./");
		
        register_times_ = blade_config_.GetInt("nameserver", "register_times", 10);
		if (register_times_ <= 0)
		{
			fprintf(stderr, "NameServer config file error: register_times is illegal : %ld, default is: 10, recommend between: 3 - 20.\n", register_times_);
			ret = BLADE_ERROR;
		}

		register_timeout_ = blade_config_.GetInt("nameserver", "register_timeout_us", 2000000);
		if (register_timeout_ <= 0)
		{
			fprintf(stderr, "NameServer config file error: register_timeout_us is illegal <= 0 : %ld, default is: 2000000, recommend between: 1000000 - 10000000\n", register_timeout_);
			ret = BLADE_ERROR;
		}

		checkpoint_interval_ = blade_config_.GetInt("nameserver", "checkpoint_interval", 3600);
		if (checkpoint_interval_ <= 0)
		{
			fprintf(stderr, "NameServer config file error: checkpoint_interval is illegal <= 0 : %ld, default is 3600, recommend between: 1800 - 7200\n", checkpoint_interval_);
			ret = BLADE_ERROR;
		}

		network_timeout_ = blade_config_.GetInt("nameserver", "network_timeout", 1000000);
		if (network_timeout_ <= 0)
		{
			fprintf(stderr, "NameServer config file error: network_timeout is illegal <= 0 : %ld, default is 1000000, recommend between: 1000000 - 5000000.\n", network_timeout_);
			ret = BLADE_ERROR;
		}
		
        ns_port_ = blade_config_.GetInt("nameserver", "ns_port", 2500);
		if (ns_port_ <= 1000 || ns_port_ >= 65535)
		{
			fprintf(stderr, "NameServer config file error: ns_port is illegal : %d, default is: 2500, recommend between: 1000 - 50000.\n", ns_port_);
			ret = BLADE_ERROR;
		}

		lease_on_ = blade_config_.GetInt("nameserver", "lease_on", 1);
		if (0 != lease_on_ &&  1 != lease_on_)
		{
			fprintf(stderr, "NameServer config file error: lease_on should be 0 or 1.\n");
			ret = BLADE_ERROR;
		}

		local_addr_ = blade_config_.GetString("nameserver", "local_addr", "");
		if (0 == local_addr_.size())
		{	
			fprintf(stderr, "NameServer config file error: local_addr is not valid.\n");
			ret = BLADE_ERROR;
		}

		replicate_check_interval_ = blade_config_.GetInt("nameserver", "replicate_check_interval_second", 20);
		if (replicate_check_interval_ <= 0)
		{
			fprintf(stderr, "NameServer config file error: replicate_check_interval_second is ilegal <= 0 : %d, default is 3600, recommend between: 1800 - 18000.\n", replicate_check_interval_);
			ret = BLADE_ERROR;
		}

		redundant_check_interval_ = blade_config_.GetInt("nameserver", "redundant_check_interval_second", 20);
		if (redundant_check_interval_ <= 0)
		{
			fprintf(stderr, "NameServer config file error: redundant_check_interval_second is illegal <= 0 : %d, default is 3600, recommend between: 1800 - 18000.\n", redundant_check_interval_);
			ret = BLADE_ERROR;
		}

		check_block_interval_ = blade_config_.GetInt("nameserver", "check_block_interval_second", 20);
		if (check_block_interval_ <= 0)
		{
			fprintf(stderr, "NameServer config file error: check_block_interval_second is illegal <= 0 : %d, default is 3600, recommend between: 1800 - 18000.\n", check_block_interval_);
			ret = BLADE_ERROR;
		}

        stat_log_ = blade_config_.GetString("nameserver", "stat_log", "");
        
        stat_check_interval_ = blade_config_.GetInt("nameserver", "stat_check_interval_second", 300);
        if (stat_check_interval_ <= 0 )
        {
			fprintf(stderr, "NameServer config file error: stat_check_interval_second is illegal <= 0 : %ld, default is 300, recommend between: 300 - 3000.\n", stat_check_interval_);
			ret = BLADE_ERROR;
        }
	}

    if (BLADE_ERROR == ret)
    {
        exit(1);
    }
	return ret;
}

int BladeNameServerParam::Load()
{
	return load_config(config_file_name_);
}

int BladeNameServerParam::load_config(std::string file_name)
{
	bool ret = blade_config_.Load(file_name);
	if (false == ret)
	{
		fprintf(stderr, "load config from file fail: (%s) is not exist.\n", file_name.c_str());
		exit(1);
	}
	return BLADE_SUCCESS;
}

}//end of namespace nameserver
}//end of namespace bladestore
