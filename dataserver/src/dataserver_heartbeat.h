/*
 * Sohu R&D  2012
 *
 * File Description:
 *      The implementation class of dataserver heartbeat
 *
 * Author   : @
 * Version  : 1.0
 * Date     : 2012-06-01
 */
#ifndef DATASERVER_HEARTBEAT_H
#define DATASERVER_HEARTBEAT_H

#ifdef GTEST
#define private public
#define protected public
#endif

#include <stdint.h>
#include <set>
#include "cthread.h"
#include "blade_common_define.h"
#include "blade_common_data.h"

namespace bladestore
{
namespace dataserver
{
using std::set;
using bladestore::common::DataServerMetrics;  
using bladestore::common::BlockInfo;  

class  DataServerImpl;
class  BlockInfo;

class DSHeartbeatManager : public Runnable
{
public:
    DSHeartbeatManager();    
    DSHeartbeatManager(DataServerImpl * ds_impl);
    ~DSHeartbeatManager();

    bool Start();    
    bool Init();    
    void Join();    
    void Run(CThread * thread, void * arg);

    bool DSRegister();
    bool DSBlockReport(set<BlockInfo *> &);
    bool DSReceivedBlockReport(BlockInfo b);

    int32_t  rack_id() const {return rack_id_;}
    uint64_t ds_id() const {return ds_id_;}
    float    cpu_total() const {return cpu_total_;}
    float    cpu_user() const {return cpu_user_;}
    int32_t  cpu_usage() const {return cpu_usage_;}
    int64_t  last_report() const {return last_report_;}
    void     set_last_report(int64_t last_report) {last_report_ = last_report;}
private:
    int32_t  rack_id_;
    uint64_t ds_id_;
    float    cpu_total_;
    float    cpu_user_;
    int32_t  cpu_usage_;
    int64_t  last_report_ ;
    char     cpu_file_buffer_[512];
    
    CThread     ds_heartbeat_;

    DataServerMetrics  * ds_metrics_;
    DataServerImpl     * ds_impl_;
    
    DataServerConfig   * config_;
    int32_t GetCpuLoad(); 
    int32_t GetMemInfo();
    
    DISALLOW_COPY_AND_ASSIGN(DSHeartbeatManager);
};//end of class DSHeartbeatManager

}//end of namespace dataserver
}//end of namespace bladestore
#endif
