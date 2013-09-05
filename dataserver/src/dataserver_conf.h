/* Sohu R&D 2012
 * 
 * File   name: dataserver_conf.h
 * Description: This file defines config details;
 * 
 * Version : 1.0
 * Author  : @YYY
 * Date    : 2012-06-03
 */

#ifndef BLADESTORE_DATASERVER_CONFIG_H
#define BLADESTORE_DATASERVER_CONFIG_H 

#include <string>
#include "blade_net_util.h"
#include "blade_common_define.h"
#include "thread_mutex.h" 

namespace bladestore
{
namespace dataserver
{
using std::string;
using namespace bladestore::common;

class DataServerConfig
{
public:
    DataServerConfig(); 
    ~DataServerConfig();
    void Init();
    //get items 
    const char * dataserver_name() const { return dataserver_name_.c_str(); }
    string ip_port() const { return dataserver_name_; }
    const char * nameserver_name() const { return nameserver_name_.c_str(); }
    int32_t rack_id() const { return rack_id_;}
    int32_t block_report_time_hour() const { return block_report_time_hour_;}
    int32_t block_report_time_minite() const { return block_report_time_minite_;}
    string stat_log() const {return stat_log_;}
    int64_t stat_check_interval() const {return stat_check_interval_;}
    uint64_t ds_id() const { return BladeNetUtil::StringToAddr(dataserver_name_); }
    uint64_t ns_id() const { return BladeNetUtil::StringToAddr(nameserver_name_); }

    int32_t thread_num_for_client() const { return thread_num_for_client_; }
    int32_t thread_num_for_ns() const { return thread_num_for_ns_; }
    int32_t thread_num_for_sync() const { return thread_num_for_sync_; }

    int32_t client_queue_size() const {return client_queue_size_;}
    int32_t ns_queue_size() const {return ns_queue_size_;}
    int32_t sync_queue_size() const {return sync_queue_size_;}

    const string block_storage_directory() const { return block_storage_directory_; }
    const string temp_directory() const { return temp_directory_; }

    const string& log_path() const{return log_path_;}
    const string& log_prefix()const {return log_prefix_;}
    int32_t log_level() const { return log_level_; }
    int32_t log_max_size() const { return log_max_size_; }
    int32_t log_file_number() const { return log_file_number_; }
private:
    //ip_port dataserver & nameserver
    string dataserver_name_;
    string nameserver_name_;
    int32_t rack_id_;
    int32_t block_report_time_hour_;
    int32_t block_report_time_minite_;
    //for monitor use
    string stat_log_;
    int64_t stat_check_interval_;

    int32_t thread_num_for_client_;        //the init size of thread
    int32_t thread_num_for_ns_;        //the init size of thread
    int32_t thread_num_for_sync_;        //the init size of thread
    int32_t client_queue_size_;
    int32_t ns_queue_size_;
    int32_t sync_queue_size_;
    //file system config
    string block_storage_directory_;   //local storage directory
    string temp_directory_;   //temp directory for replicate use
    //logging config
    string log_path_;
    string log_prefix_;
    int32_t log_level_;
    int32_t log_max_size_;
    int32_t log_file_number_;

    pandora::CThreadMutex mutex_; 
    bool is_init_;

    DISALLOW_COPY_AND_ASSIGN(DataServerConfig);
};

}//end of namespace dataserver
}//end of namespace bladestore

#endif

