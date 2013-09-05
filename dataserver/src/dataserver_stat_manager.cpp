#include <iostream>
#include <errno.h>
#include <stdlib.h>

#include "time_util.h"
#include "blade_common_define.h"
#include "utility.h"
#include "dataserver_conf.h"
#include "fsdataset.h"
#include "dataserver_impl.h"
#include "dataserver_stat_manager.h"
//#include "singleton.h"

namespace bladestore
{
namespace dataserver
{
using namespace std;
using namespace bladestore::common; 

DataServerStatManager::DataServerStatManager() 
{
    is_run_ = false;    
    stat_log_ = Singleton<DataServerConfig>::Instance().stat_log(); 
    stat_check_interval_ = Singleton<DataServerConfig>::Instance().stat_check_interval(); 
    ip_port_ = Singleton<DataServerConfig>::Instance().ip_port(); 
    
    AtomicSet64(&request_connection_num_, 0);
    AtomicSet64(&courrent_connection_num_, 0);
    AtomicSet64(&client_queue_length_, 0);
    AtomicSet64(&ns_queue_length_, 0);
    AtomicSet64(&sync_queue_length_, 0);
    AtomicSet64(&block_nums_, 0);
    ds_impl_ = NULL;
    Clear();
}

DataServerStatManager::~DataServerStatManager()
{
}
void DataServerStatManager::Clear()
{
    AtomicSet64(&request_connection_num_, 0);

    AtomicSet64(&read_bytes_, 0);
    AtomicSet64(&read_packet_times_, 0);
    AtomicSet64(&read_packet_error_times_, 0);

    AtomicSet64(&write_bytes_, 0);
    AtomicSet64(&write_packet_times_, 0);
    AtomicSet64(&write_packet_error_times_, 0);

    AtomicSet64(&normal_write_block_times_, 0);
    AtomicSet64(&normal_write_block_complete_times_, 0);

    AtomicSet64(&safe_write_block_times_, 0);
    AtomicSet64(&safe_write_block_complete_times_, 0);

    AtomicSet64(&delete_block_times_, 0);
    AtomicSet64(&delete_block_error_times_, 0);

    AtomicSet64(&transfer_block_times_, 0);
    AtomicSet64(&transfer_block_error_times_, 0);

    AtomicSet64(&replicate_block_times_, 0);
    AtomicSet64(&replicate_block_complete_times_, 0);

    AtomicSet64(&replicate_packet_times_, 0);
    AtomicSet64(&replicate_packet_error_times_, 0);


}

bool DataServerStatManager::Start(DataServerImpl * ds_impl)
{
    bool ret = true;
    if (false == is_run_)
    {
        ds_impl_ = ds_impl;
        int ret_code = stat_check_.Start(this, NULL);
        if (0 != ret_code) 
        {
            return false;
        }
        is_run_ = true;
    }
    LOGV(LL_INFO, "Stat manager start!");
    return ret;
}
void DataServerStatManager::StatPrint()
{
    outfile_.open(stat_log_.c_str(), ostream::app);
    int stat = outfile_.fail() ? -EIO : 0;
    if (0 == stat)
    {
        outfile_ << "\"" << ip_port_ << "\"" << '/';

        const char *fs_path = Singleton<DataServerConfig>::
                        Instance().block_storage_directory().c_str();
        int32_t ret_code = GetDiskUsage(fs_path, &used_storage_, 
                                        &total_storage_);
        if (BLADE_FILESYSTEM_ERROR == ret_code) 
        {
            LOGV(LL_ERROR, "get disk usage error.");
            used_storage_ = -1;
            total_storage_ = -1;
        }
        outfile_ << total_storage_ << '/';
        outfile_ << used_storage_ << '/';
        //blocks numbers
        outfile_ << ds_impl_->dataset()->GetBlockNum() << '/';

        outfile_ << AtomicGet64(&request_connection_num_) << '/'; 
        outfile_ << ds_impl_->num_connection() << '/';//当前时间点的链接数 courrent_connection_num

        outfile_ << ds_impl_->ClientQueueSize() << '/'; 
        outfile_ << ds_impl_->NameServerQueueSize() << '/'; 
        outfile_ << ds_impl_->SyncQueueSize() << '/'; 
    
        outfile_ << AtomicGet64(&read_bytes_) << '/';
        outfile_ << AtomicGet64(&read_packet_times_) << '/';
        outfile_ << AtomicGet64(&read_packet_error_times_) << '/';
    
        outfile_ << AtomicGet64(&write_bytes_) << '/';
        outfile_ << AtomicGet64(&write_packet_times_) << '/';
        outfile_ << AtomicGet64(&write_packet_error_times_) << '/';
    
        outfile_ << AtomicGet64(&normal_write_block_times_) << '/';
        outfile_ << (AtomicGet64(&normal_write_block_times_) - 
                AtomicGet64(&normal_write_block_complete_times_)) << '/';//输出普通写未完成提交的block数量
    
        outfile_ << AtomicGet64(&safe_write_block_times_) << '/';
        outfile_ << AtomicGet64(&safe_write_block_complete_times_) << '/';
        outfile_ << (AtomicGet64(&safe_write_block_times_) - 
                AtomicGet64(&safe_write_block_complete_times_)) << '/';//输出a安全写未完成提交的block数量
    
        outfile_ << AtomicGet64(&delete_block_times_) << '/';
        outfile_ << AtomicGet64(&delete_block_error_times_) << '/';
    
        outfile_ << AtomicGet64(&transfer_block_times_) << '/';
        outfile_ << AtomicGet64(&transfer_block_error_times_) << '/';
    
        outfile_ << AtomicGet64(&replicate_packet_times_) << '/';
        outfile_ << AtomicGet64(&replicate_packet_error_times_) << '/';
    
        outfile_ << AtomicGet64(&replicate_block_times_) << '/';
        outfile_ << (AtomicGet64(&replicate_block_times_) - 
                AtomicGet64(&replicate_block_complete_times_)) << '/';//输出复制未完成提交的block数量
    

        outfile_ << '\n';
        outfile_.close();
    }
    Clear();
}

void DataServerStatManager::Run(CThread * thread,void * arg)
{
    while(1)
    {
        StatPrint();
        sleep(stat_check_interval_);
    }
}

}//end of namespace dataserver
}//end of namespace bladestore
