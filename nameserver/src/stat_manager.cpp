#include <iostream>
#include <errno.h>
#include <stdlib.h>

#include "stat_manager.h"
#include "time_util.h"
#include "blade_common_define.h"
#include "blade_nameserver_param.h"
//#include "singleton.h"

namespace bladestore
{
namespace nameserver
{
using namespace std;
using namespace bladestore::common; 

StatManager::StatManager() 
{
    is_run_ = false;    
    stat_log_ = Singleton<BladeNameServerParam>::Instance().stat_log();
    stat_check_interval_ = Singleton<BladeNameServerParam>::Instance().stat_check_interval();
    ip_port_ = Singleton<BladeNameServerParam>::Instance().get_local_addr();
    vip_ = Singleton<BladeNameServerParam>::Instance().get_vip_str();
    stat_ = "";
    
    AtomicSet64(&read_queue_length_, 0);
    AtomicSet64(&write_queue_length_, 0);
    AtomicSet64(&read_times_, 0);
    AtomicSet64(&read_error_times_, 0);
    AtomicSet64(&write_times_, 0);
    AtomicSet64(&write_error_times_, 0);
    AtomicSet64(&safe_write_times_, 0);
    AtomicSet64(&safe_write_error_times_, 0);
    AtomicSet64(&delete_times_, 0);
    AtomicSet64(&delete_error_times_, 0);
    AtomicSet64(&copy_times_, 0);
    AtomicSet64(&copy_error_times_, 0);
    AtomicSet64(&dir_nums_, 0);
    AtomicSet64(&file_nums_, 0);
    AtomicSet64(&block_nums_, 0);
}

StatManager::~StatManager()
{
}
void StatManager::Clear()
{
    AtomicSet64(&read_times_, 0);
    AtomicSet64(&read_error_times_, 0);
    AtomicSet64(&write_times_, 0);
    AtomicSet64(&write_error_times_, 0);
    AtomicSet64(&safe_write_times_, 0);
    AtomicSet64(&safe_write_error_times_, 0);
    AtomicSet64(&delete_times_, 0);
    AtomicSet64(&delete_error_times_, 0);
    AtomicSet64(&copy_times_, 0);
    AtomicSet64(&copy_error_times_, 0);
}

void StatManager::Start()
{
    if (false == is_run_)
    {
        stat_check_.Start(this,NULL);
        is_run_ = true;
    }
    LOGV(LL_INFO, "Stat manager start!");
}
void StatManager::StatPrint()
{
    outfile_.open(stat_log_.c_str(), ostream::app);
    int stat = outfile_.fail() ? -EIO : 0;
    if (0 == stat)
    {
        outfile_ << "\"" << ip_port_<< "\"" << '/';
        outfile_ << "\"" << vip_ << "\"" << '/';
        outfile_ << "\"" << stat_ << "\"" << '/';
        outfile_ << AtomicGet64(&read_queue_length_) << '/';
        outfile_ << AtomicGet64(&write_queue_length_) << '/';
        outfile_ << AtomicGet64(&read_times_) << '/'; 
        outfile_ << AtomicGet64(&read_error_times_) << '/';
        outfile_ << AtomicGet64(&write_times_) << '/';
        outfile_ << AtomicGet64(&write_error_times_) << '/';
        outfile_ << AtomicGet64(&safe_write_times_) << '/';
        outfile_ << AtomicGet64(&safe_write_error_times_) << '/';
        outfile_ << AtomicGet64(&delete_times_) << '/';
        outfile_ << AtomicGet64(&delete_error_times_) << '/';
        outfile_ << AtomicGet64(&copy_times_) << '/';
        outfile_ << AtomicGet64(&copy_error_times_) << '/';
        outfile_ << AtomicGet64(&dir_nums_) << '/';
        outfile_ << AtomicGet64(&file_nums_ )<< '/';
        outfile_ << AtomicGet64(&block_nums_) << '/';
        outfile_ << '\n';
        outfile_.close();
    }
    Clear();
}

void StatManager::Run(CThread * thread,void * arg)
{
    while(1)
    {
        StatPrint();
	sleep(stat_check_interval_);
    }
}

}//end of namespace nameserver
}//end of namespace bladestore
