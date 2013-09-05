#include "monitor_manager.h"
#include "nameserver_impl.h"

namespace bladestore
{
namespace nameserver
{
MonitorManager::MonitorManager(BladeLayoutManager & lay): layout_(lay)
{
    Init();
}

MonitorManager::~MonitorManager()
{
    delete log_;
}

void MonitorManager::Init()
{
    log_ = new Log("../log", "monitor_log", LL_DEBUG);
    log_->set_need_process_id(false);
    log_->set_need_thread_id(false);
}
void MonitorManager::Start()
{
    ns_monitor_.Start(this, NULL);
}

void MonitorManager::Run(CThread * thread, void * arg)
{
    while(1)
    {
        monitor_ns();
        sleep(MONITOR_INTERVAL_SECS);
    }
}

void MonitorManager::monitor_ns()
{
    monitor_ns_racks_map();
    monitor_ns_server_map();
    monitor_ns_blocks_map();
}

void MonitorManager::monitor_ns_racks_map()
{
    CRLockGuard read_lock_guard(layout_.racks_mutex_, true);
    int32_t rack_size = layout_.racks_map_.size();
    RACKS_MAP_ITER iter =  layout_.racks_map_.begin();
    log_->Write(LL_INFO, "RacksMap   Num: %d ", rack_size);
    if (0 == rack_size)
    {
        return;
    }
    log_->Write(LL_INFO, "RackID     DSNum      DSID");
    for ( ;iter != layout_.racks_map_.end(); iter++ )
    {
        string ds;
        set<DataServerInfo* >::iterator dsiter = iter->second.begin();
        for ( ; dsiter != iter->second.end(); dsiter++)
        {
            ds = ds + "   " + BladeNetUtil::AddrToString( (*dsiter)->dataserver_id_);
        }
        log_->Write(LL_INFO, "%d     %d     %s", iter->first, iter->second.size(), ds.c_str());
    }
    log_->Write(LL_INFO, "\n\n");
}
void MonitorManager::monitor_ns_server_map()
{
    CRLockGuard read_lock_guard(layout_.server_mutex_, true);
    int32_t ds_size = layout_.server_map_.size();
    SERVER_MAP_ITER iter =  layout_.server_map_.begin();
    log_->Write(LL_INFO, "DataServerMap   Num: %d ", ds_size);
    int32_t alive_ds_size = layout_.get_alive_ds_size();
    log_->Write(LL_INFO, "Alive DS_NUM : %d", alive_ds_size);
    if (0 == ds_size)
    {
        return;
    }
    log_->Write(LL_INFO, "DataServerID     Alive      LoadScore       Block_hold      Block_toremove");
    for ( ;iter != layout_.server_map_.end(); iter++ )
    {
        log_->Write(LL_INFO, "%s     %d     %f     %d      %d", (BladeNetUtil::AddrToString(iter->first)).c_str(), iter->second->is_alive()? 1: 0, iter->second->load_score_, iter->second->blocks_hold_.size(), iter->second->blocks_to_remove_.size());
    }
    log_->Write(LL_INFO, "\n\n");
}

void MonitorManager::monitor_ns_blocks_map()
{
    CRLockGuard read_lock_guard(layout_.blocks_mutex_, true);
    int32_t b_size = layout_.blocks_map_.size();
    BLOCKS_MAP_ITER iter =  layout_.blocks_map_.begin();
    log_->Write(LL_INFO, "BlocksMap   Num: %d ", b_size);
    if (0 == b_size)
    {
        return ;
    }
    log_->Write(LL_INFO, "StartBlockID    version    replicas    dsnum");
    //for ( ; iter != layout_.blocks_map_.end(); iter++)
    //{
    //   log_->Write(LL_INFO, "%d     %d     %d      %d", iter->first, iter->second->version_, iter->second->replicas_number_, iter->second->dataserver_set_.size());
    //}
    
    for (int64_t i = 0; i < 1000000 ; i +=1000)
    {
        if ((iter = layout_.blocks_map_.find(i)) != layout_.blocks_map_.end())
        {
            log_->Write(LL_INFO, "%d     %d     %d      %d", iter->first, iter->second->version_, iter->second->replicas_number_, iter->second->dataserver_set_.size());
        }
    }
}

}
}
