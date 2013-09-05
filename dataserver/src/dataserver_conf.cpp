#include <cstdlib>
#include <cassert>
#include "log.h"
#include "config.h"
#include "dataserver_conf.h"

namespace bladestore
{
namespace dataserver
{
using std::string;
using namespace pandora;

DataServerConfig::DataServerConfig() : is_init_(false) 
{
    Init();
}

DataServerConfig::~DataServerConfig() 
{
}


void DataServerConfig::Init()
{
    CThreadGuard guard(&mutex_);
    if (true == is_init_) 
    {
        LOGV(LL_INFO,"dataserverconfig is already inited!");
    }
    else 
    {
        pandora::Config config;
        string load_path("../conf/dataserver.conf");
        bool ret = config.Load(load_path);
        assert(ret == true);
        //get ip_port info for dataserver and nameserver
        char tmp[256];
        string ip = config.GetString("dataserver", "ds_ip", "");
        assert(ip.size()!= 0);
        int port = config.GetInt("dataserver", "ds_port", 0);
        assert(port != 0);
        snprintf(tmp, 256, "%s:%d", ip.c_str(), port);
        dataserver_name_ = tmp;
        LOGV(LL_INFO,"ds_name:%s", dataserver_name_.c_str());
    
        ip = config.GetString("nameserver", "ns_ip", "");
        assert(ip.size() != 0);
        port = config.GetInt("nameserver", "ns_port", 0);
        assert(port != 0);
        snprintf(tmp, 256, "%s:%d", ip.c_str(), port);
        nameserver_name_ = tmp;
    
        rack_id_ = config.GetInt("dataserver", "rack_id", 0);
        assert(rack_id_ != 0);

        block_report_time_hour_ = config.GetInt("dataserver", 
                                "block_report_time_hour", 3);
        assert(block_report_time_hour_ < 24);
        block_report_time_minite_ = config.GetInt("dataserver", 
                                "block_report_time_minite", 0);
        assert(block_report_time_minite_ < 60);

        //monitor init
        stat_log_ = config.GetString("dataserver", "stat_log", "");
        assert(stat_log_.size() != 0);
        stat_check_interval_ = config.GetInt("dataserver", 
                                "stat_check_interval", 300);
        if (0 >= stat_check_interval_)
        {
            fprintf(stderr, "dataserver config file error: \
                    stat_check_interval_second is illegal <= 0 : %ld, \
                    default is 300, recommend between: 300 - 3000.\n", 
                    stat_check_interval_);
        }
        assert(stat_check_interval_ > 0);

        thread_num_for_client_ = config.GetInt("thread_num", "thread_num_for_client", 0);
        assert(thread_num_for_client_ > 0);
        //thread_num_for_ns must >= 2 to avoid sync block situation
        thread_num_for_ns_ = config.GetInt("thread_num", "thread_num_for_ns", 2);
        assert(thread_num_for_ns_ > 1);

        thread_num_for_sync_ = config.GetInt("thread_num", "thread_num_for_sync", 0);
        assert(thread_num_for_sync_ > 0);
        //get queue_size
        client_queue_size_ = config.GetInt("queue_size", "client_queue_size", 100);
        if (100 > client_queue_size_)
            client_queue_size_ = 100;
        ns_queue_size_ = config.GetInt("queue_size", "ns_queue_size", 100);
        if (50 > ns_queue_size_)
            ns_queue_size_ = 50;
        sync_queue_size_ = config.GetInt("queue_size", "sync_queue_size", 100);
        if (50 > sync_queue_size_)
            sync_queue_size_ = 50;
    
        //get file system config: 
        block_storage_directory_ = config.GetString("storage", "block_storage_directory", "");
        assert(block_storage_directory_.size() != 0);

        temp_directory_ = config.GetString("storage", "temp_directory", "");
        assert(temp_directory_.size() != 0);
    
        //get logging config
        log_path_ = config.GetString("log", "log_path", "");
        assert(log_path_.size() != 0);
        log_prefix_ = config.GetString("log", "log_prefix", "");
        //assert(log_prefix_.size() != 0);
        log_level_ = config.GetInt("log", "log_level", 1);
        log_max_size_ = config.GetInt("log", "log_max_size", 0);

        //if log_max_size has invalid character, result is very small
        //here should be max and least value 
        if (100*1024 > log_max_size_) 
        {
            log_max_size_ = 100*1024*1024;
            fprintf(stderr, "log_max_size is set too small!!\n");
        }
        log_file_number_ = config.GetInt("log", "log_file_number", 0);
        if (2 > log_file_number_) 
        {
            log_file_number_ = 10;
            fprintf(stderr, "log_file_number is set too small!!\n");
        }
        is_init_ = true;
    }
    return;
}

} //end of namespace dataserver
} //end of namespace bladestore
