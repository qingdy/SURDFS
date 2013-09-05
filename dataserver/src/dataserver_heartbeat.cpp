#include <fcntl.h> 
#include "singleton.h"
#include "time_util.h"
#include "utility.h"
#include "fsdataset.h"
#include "dataserver_net_handler.h"
#include "dataserver_net_server.h"
#include "blade_common_data.h"
#include "dataserver_impl.h"
#include "dataserver_conf.h"
#include "register_packet.h"
#include "heartbeat_packet.h"
#include "block_report_packet.h"
#include "bad_block_report_packet.h"
#include "block_received_packet.h"
#include "dataserver_response.h"
#include "socket_context.h"
#include "dataserver_heartbeat.h"

namespace bladestore
{
namespace dataserver
{

using namespace pandora;
using common::GetDiskUsage;

DSHeartbeatManager::DSHeartbeatManager():rack_id_(0), ds_id_(0), 
    cpu_total_(0), cpu_user_(0), cpu_usage_(0), ds_metrics_(NULL), 
    ds_impl_(NULL)
{
    config_ = &(Singleton<DataServerConfig>::Instance()); 
    rack_id_ = config_->rack_id();
    ds_id_ = config_->ds_id();
}

DSHeartbeatManager::DSHeartbeatManager(DataServerImpl * ds_impl):
    rack_id_(0), ds_id_(0), cpu_total_(0), cpu_user_(0), cpu_usage_(0),
    ds_impl_(ds_impl)
{
    config_ = &(Singleton<DataServerConfig>::Instance()); 
    rack_id_ = config_->rack_id();
    ds_id_ = config_->ds_id();
    ds_metrics_ = NULL;
}

DSHeartbeatManager::~DSHeartbeatManager()
{
    if (NULL != ds_metrics_)
        delete ds_metrics_;
}

bool DSHeartbeatManager::Start()
{
    bool ret = Init();
    if (false == ret) 
    {
        LOGV(LL_ERROR, "heartbeat init failed");
        return false;
    }
    int ret_code = ds_heartbeat_.Start(this, NULL);
    if (0 == ret_code)
        return true;
    else 
        return false;
}

bool DSHeartbeatManager::Init()
{
    ds_metrics_ = new DataServerMetrics();
    if (NULL == ds_metrics_)
        return false;
    memset(ds_metrics_, 0, sizeof(*ds_metrics_));
    return true;
}

void DSHeartbeatManager::Join()
{
    ds_heartbeat_.Join();
}

void DSHeartbeatManager::Run(CThread * thread, void * arg)
{
    //1.register
    int32_t error_register = 0;
    struct tm *time_now;
    time_t lt;
    lt = time(NULL);
    time_now = localtime(&lt);
    srand((unsigned)time(NULL));
    int64_t time_next = ((config_->block_report_time_hour()
                - time_now->tm_hour + 24) % 24) * 3600000000 +
                ((config_->block_report_time_minite()
                - time_now->tm_min + 60) % 60) * 60000000 +
                //加上一个随机数避免所有DS在同一时间进行block report
                rand() % 120 * 60000000;
    if (0 == time_next)
        time_next = LAST_REPORT_INTERVAL;
    last_report_ = TimeUtil::GetTime() + time_next;
    //try register untill success
    while (false == DSRegister()) 
    {
        //register fail 5 times, try connect to NS
        if (4 < error_register) 
        {
            if (ds_impl_->net_server()->ConnectToNS())
                error_register = 0;
            else
                LOGV(LL_ERROR, "Can not connect to NameServer");
        }
        LOGV(LL_ERROR, "ds_register return error");
        ++error_register;
    }
    LOGV(LL_DEBUG, "ds_register return success");

    int32_t err_times = 0;
    const char *fs_path = config_->block_storage_directory().c_str();
    while (1) 
    { 
        //2. send heartbeat every 10(default) sec
        int32_t ret_code = GetDiskUsage(fs_path, &ds_metrics_->used_space_, 
                                        &ds_metrics_->total_space_);
        if (BLADE_FILESYSTEM_ERROR == ret_code) 
        {
            LOGV(LL_ERROR, "get disk usage error.");
            ds_metrics_->used_space_ = -1;
            ds_metrics_->total_space_ = -1;
        }
        int32_t used_pecent = 0;
        int32_t total_space_log = ds_metrics_->total_space_ / (1024*1024*1024);
        if (0 != ds_metrics_->total_space_) 
        {
            used_pecent = (ds_metrics_->used_space_)*100/ds_metrics_->total_space_;
        }
        int32_t ret_cpu_load = GetCpuLoad();
        if (0 > ret_cpu_load) 
        {
            LOGV(LL_ERROR, "get cpu load error.");
            ret_cpu_load = -1;
        }
        ds_metrics_->cpu_load_ = ret_cpu_load;
        ds_metrics_->num_connection_ = ds_impl_->num_connection();      
        
        //send heartbeat
        HeartbeatPacket * p = new HeartbeatPacket(rack_id_, ds_id_, *ds_metrics_);
        if (NULL == p) 
        {
            LOGV(LL_ERROR, "new Heartbeatpacket error.");
            continue;
        }
        int32_t ret_pack = p->Pack();
        if (BLADE_SUCCESS != ret_pack) 
        {
            delete p;
            LOGV(LL_ERROR, "Heartbeatpacket packet pack error");
        }
        
        uint64_t ns_id = config_->ns_id();
        int64_t  socket_fd = ds_impl_->net_server()->stream_handler()->end_point().GetFd();
        uint64_t ns_id_use = BladeNetUtil::GetPeerID(socket_fd);
        if (ns_id_use != ns_id) 
        {
            LOGV(LL_ERROR, "Error in the connection with NS,ns_id:%ld \
                    ns_id_use:%ld", ns_id, ns_id_use);
            sleep(DS_RECONNECT_NS_TIME);
            ds_impl_->net_server()->ConnectToNS();
            delete p;
            continue;
        }
        int32_t error = Singleton<AmFrame>::Instance().SendPacket(
                ds_impl_->net_server()->stream_handler()->end_point(), 
                p, true, NULL, NS_OP_TIMEOUT);
        if (0 == error) 
        {
            err_times = 0;   
            LOGV(LL_DEBUG, "heartbeat success.CPU:%d/total:%dGB/used:%d%/num:%d", 
                 ds_metrics_->cpu_load_, total_space_log, used_pecent,
                 ds_metrics_->num_connection_);
        } 
        else 
        {
            LOGV(LL_ERROR, "send heartbeat error.");
            ++err_times; 
            if (4 < err_times) 
            {
                if (ds_impl_->net_server()->ConnectToNS())
                    err_times = 0;
                else
                    LOGV(LL_ERROR, "Can not connect to NameServer");
            }
            delete p;
        }

        //3.block report if needed
        if (last_report_ <= TimeUtil::GetTime()) 
        {
            set<BlockInfo * > report;
            ds_impl_->dataset()->GetBlockReport(report);
            if (false == DSBlockReport(report)) 
            {
                LOGV(LL_ERROR, "ds block report error");    
            }
            else 
            {
                LOGV(LL_INFO, "ds block report success.");
            }
        }
        //4.sleep
        sleep(HEARTBEAT_INTERVAL_SECS);
    }
}

bool DSHeartbeatManager::DSRegister()
{
    LOGV(LL_DEBUG, "Now in register");
    RegisterPacket * p = new RegisterPacket(rack_id_, ds_id_);
    if (NULL == p) 
    {
        LOGV(LL_ERROR, "new RegisterPacket error.");
        return false;
    }
    SyncResponse *sync_register = new SyncResponse(p->operation());
    if (NULL == sync_register) 
    {
        LOGV(LL_ERROR, "new SyncResponse error.");
        delete p;
        return false;
    }
    int32_t ret_pack = p->Pack();
    if (BLADE_SUCCESS != ret_pack) 
    {
        delete p;
        LOGV(LL_ERROR, "registerpacket packet pack error");
        return false;
    }
    
    uint64_t ns_id = config_->ns_id();
    int64_t  socket_fd = ds_impl_->net_server()->stream_handler()->end_point().GetFd();
    uint64_t ns_id_use = BladeNetUtil::GetPeerID(socket_fd);
    if (ns_id_use != ns_id) 
    {
        LOGV(LL_ERROR, "Error in the connection with NS,ns_id:%ld ns_id_use:%ld", 
                ns_id, ns_id_use);
        sleep(DS_RECONNECT_NS_TIME);
        if (!(ds_impl_->net_server()->ConnectToNS()))
        {
            delete p;
            return false;
        }
    }
    int32_t ret = Singleton<AmFrame>::Instance().SendPacket(
            ds_impl_->net_server()->stream_handler()->end_point(), p, true,
            (void *)sync_register, REGISTER_TIMEOUT);
    if (0 == ret) 
    {
        LOGV(LL_DEBUG, "Register Send packet OK");   
        sync_register->cond().Lock();
        while (true == sync_register->wait()) 
        {
            sync_register->cond().Wait(DS_REGISTER_WAIT);
        }
        sync_register->cond().Unlock();
        sync_register->set_wait(true);

        if (sync_register->response_ok()) 
        {
            LOGV(LL_INFO, "register response OK.");
            delete sync_register;
            return true;
        } 
        else 
        {
            delete sync_register;
            LOGV(LL_ERROR, 
                 "registerpacket packet already send, error in response");
            return false;
        }
    } 
    else 
    {
        delete sync_register;
        delete p;
        LOGV(LL_ERROR, "Register Send packet error");   
        return false;
    }
}

bool DSHeartbeatManager::DSBlockReport(set<BlockInfo *> &report_block)
{
    LOGV(LL_DEBUG, "report blocks num: %d", report_block.size());   
    BlockReportPacket * p = new BlockReportPacket(ds_id_, report_block);
    if (NULL == p) 
    {
        LOGV(LL_ERROR, "new BlockReportPacket error.");
        return false;
    }
    int32_t ret_pack = p->Pack();
    if (BLADE_SUCCESS != ret_pack) 
    {
        delete p;
        LOGV(LL_ERROR, "BlockReportpacket packet pack error");
        return false;
    }
    
    uint64_t ns_id = config_->ns_id();
    int64_t  socket_fd = ds_impl_->net_server()->stream_handler()->end_point().GetFd();
    uint64_t ns_id_use = BladeNetUtil::GetPeerID(socket_fd);
    if (ns_id_use != ns_id) 
    {
        LOGV(LL_ERROR, "Error in the connection with NS,ns_id:%ld ns_id_use:%ld", 
                ns_id, ns_id_use);
        sleep(DS_RECONNECT_NS_TIME);
        if (!(ds_impl_->net_server()->ConnectToNS())) 
        {
            delete p;
            return false;
        }
    }
    int32_t error = Singleton<AmFrame>::Instance().SendPacket(
                    ds_impl_->net_server()->stream_handler()->end_point(), 
                    p, true, NULL, BLOCKREPORT_TIMEOUT);
    if (0 != error) 
    {
        delete p;
        LOGV(LL_ERROR, "BlockReport Send packet error: %d", error);
        return false;
    } 
    else 
    {
        return true;
    }
}

bool DSHeartbeatManager::DSReceivedBlockReport(BlockInfo received)
{
    BlockReceivedPacket * p = new BlockReceivedPacket(ds_id_, received);
    if (NULL == p) 
    {
        LOGV(LL_ERROR, "new BlockReceivedPacket error.");
        return false;
    }
    int32_t ret_pack = p->Pack();
    if (BLADE_SUCCESS != ret_pack) 
    {
        delete p;
        LOGV(LL_ERROR, "BlockReceivedpacket pack error");
        return false;
    }
    
    uint64_t ns_id = config_->ns_id();
    int64_t  socket_fd = ds_impl_->net_server()->stream_handler()->end_point().GetFd();
    uint64_t ns_id_use = BladeNetUtil::GetPeerID(socket_fd);
    if (ns_id_use != ns_id) 
    {
        LOGV(LL_ERROR, "Error in the connection with NS,ns_id:%ld ns_id_use:%ld", 
                ns_id, ns_id_use);
        sleep(DS_RECONNECT_NS_TIME);
        if (!(ds_impl_->net_server()->ConnectToNS())) 
        {
            delete p;
            return false;
        }
    }
    int32_t error = Singleton<AmFrame>::Instance().SendPacket(
                        ds_impl_->net_server()->stream_handler()->end_point(), 
                        p, false, NULL, NS_OP_TIMEOUT); 

    if (0 == error) 
    {
        LOGV(LL_INFO, "Block Received Send packet ok");
        return true;
    } 
    else 
    {
        delete p;
        LOGV(LL_ERROR, "Block Received Send packet error");
        return false;
    }
}

int32_t DSHeartbeatManager::GetCpuLoad()  
{ 
    FILE  * cpu_file;
    size_t  cpu_file_buf_len;
    float   usage = cpu_usage_;  
    float   user = 0, nice = 0, sys = 0, idle = 0, iowait = 0;  
    
    cpu_file = fopen("/proc/stat","r");
    if (NULL == cpu_file) 
    {
        LOGV(LL_ERROR, "Open /proc/stat error");
        return -1;
    }
    memset(cpu_file_buffer_, 0, sizeof(cpu_file_buffer_));
    cpu_file_buf_len = fread(cpu_file_buffer_, 1, sizeof(cpu_file_buffer_), cpu_file);
    fclose(cpu_file);
    if (0 == cpu_file_buf_len) 
    {
        LOGV(LL_ERROR, "Read /proc/stat error");
        return -1;
    }
    cpu_file_buffer_[cpu_file_buf_len] = '\0';
    sscanf(cpu_file_buffer_, "cpu %f %f %f %f %f", &user, &nice, &sys, &idle, &iowait);  
    if (idle <= 0)
        idle = 0; 
    if (user - cpu_user_ == 0 || user + nice + sys + idle - cpu_total_ == 0) 
    {
        return cpu_usage_; 
    }
    usage = 100 * (user - cpu_user_) / (user + nice + sys + idle - cpu_total_);  
    if (usage > 100)  
        usage = 100; 
    usage = usage > 0 ? usage : -usage;
    
    cpu_total_ = user + nice + sys + idle;
    cpu_user_ = user;    
    cpu_usage_ = (int32_t)usage;
    
    return cpu_usage_;
} 

//XXX not used yet, may use later
int32_t GetMemInfo() 
{
    return 0;
}

}//end of namespace dataserver
}//end of namespace bladestore
