/*
 * Sohu R&D  2012
 *
 * File Description:
 *      The implementation class of dataserver
 *
 * Author   : @landyliu
 * Version  : 1.0
 * Date     : 2012-06-01
 */

#ifndef BLADESTORE_DATASERVER_IMPL_H
#define BLADESTORE_DATASERVER_IMPL_H 1

#include <string>
#include <map>
#include <set>
#include <stdbool.h>
#include "mempool.h"
#include "blade_common_define.h"
#include "blade_common_data.h"
#include "blade_packet.h"
#include "task_queue.h"

#ifdef GTEST 
#define private public
#define protected public
#endif

namespace pandora 
{
class Log;
}

using namespace bladestore::common;
using namespace bladestore::message;
using namespace pandora;
using namespace std;

namespace bladestore
{
namespace dataserver
{

class ClientTaskProcessor;
class NSTaskProcessor;
class SyncTaskProcessor;
class DataNetServer;
class ClientTask;
class NSTask;
class FSDataSet;
class FSDataInterface;
class DSHeartbeatManager;
class DataServerConfig;

class DataServerImpl
{
public:
    //explicit DataServerImpl(); 
    DataServerImpl(); 
    ~DataServerImpl();

    bool Init();
    bool Start();
    bool Stop();

    //net IO handle: push and pop packet into queues
    bool PushClientQueue(ClientTask *task, bool wait = false);
    bool PushNameServerQueue(NSTask *task, bool wait = false);
    bool PushSyncQueue(map<BlockInfo, set<uint64_t> > &task, bool wait = false);
    bool PopClientQueue(ClientTask *&task);
    bool PopNameServerQueue(NSTask *&task);
    bool PopSyncQueue(map<BlockInfo, set<uint64_t> > &task);
    // XXX these three function is used for monitor use
    uint32_t ClientQueueSize() {return client_queue_->Size();}
    uint32_t NameServerQueueSize(){return ns_queue_->Size();}
    uint32_t  SyncQueueSize(){return  sync_queue_->Size();}
    
    void NumConnectionAdd() { AtomicAdd(&(num_connection_), 1);}
    void NumConnectionSub() { AtomicSub(&(num_connection_), 1);}

    volatile bool is_running()     { return is_running_; }
    int32_t       num_connection() { return AtomicGet(&num_connection_);}
    Log                 * log()                  { return log_; }
    DataServerConfig    * config()               { return config_; }
    ClientTaskProcessor * client_processor()     { return client_processor_;};
    DataNetServer       * net_server()           { return net_server_; }
    FSDataSet           * dataset()              { return dataset_;}
    FSDataInterface     * fs_interface()         { return fs_interface_;}
    DSHeartbeatManager  * ds_heartbeat_manager() { return ds_heartbeat_manager_; }
    
private:
    bool InitNetServer();
    bool StartThreads();

    volatile bool                 is_running_;
    pandora::atomic_t             num_connection_;
    Log                         * log_;
    TaskQueue<ClientTask  *>    * client_queue_;
    TaskQueue<NSTask *>         * ns_queue_;    //queue for nameserver
    TaskQueue<map<BlockInfo, set<uint64_t > > > * sync_queue_;
    ClientTaskProcessor         * client_processor_;
    NSTaskProcessor             * ns_processor_;
    SyncTaskProcessor           * sync_processor_;
    DataNetServer               * net_server_;
    DSHeartbeatManager          * ds_heartbeat_manager_;//send heartbeat

    FSDataSet                   * dataset_;
    FSDataInterface             * fs_interface_;
    DataServerConfig            * config_;
    DISALLOW_COPY_AND_ASSIGN(DataServerImpl);
};

} //end of namespace
}

#endif
