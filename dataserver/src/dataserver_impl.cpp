#include "log.h"
#include "singleton.h"
#include "task_queue.h"
#include "mempool.h"
#include "fsdataset.h"
#include "task_process.h"
#include "fs_interface.h"
#include "dataserver_net_server.h"
#include "dataserver_conf.h"
#include "dataserver_heartbeat.h"
#include "dataserver_stat_manager.h"
#include "dataserver_impl.h"

namespace bladestore 
{
namespace dataserver 
{
DataServerImpl::DataServerImpl() : is_running_(false)
{
    client_queue_ = NULL;
    ns_queue_ = NULL;
    sync_queue_ = NULL;

    client_processor_ = NULL;
    ns_processor_ = NULL;
    sync_processor_ = NULL;

    net_server_ = NULL;
    ds_heartbeat_manager_= NULL;
    dataset_ = NULL;
    fs_interface_ = NULL;
}

DataServerImpl::~DataServerImpl()
{
    if (NULL != client_queue_) 
    { 
        delete client_queue_;
        client_queue_ = NULL;
    }
    if (NULL != ns_queue_) 
    {
        delete ns_queue_;
        ns_queue_ = NULL;
    }
    if (NULL != sync_queue_) 
    {
        delete sync_queue_;
        sync_queue_ = NULL;
    }

    if (NULL != client_processor_) 
    {
        delete client_processor_;
        client_processor_ =  NULL;
    }
    if (NULL != ns_processor_) 
    {
        delete ns_processor_;
        ns_processor_ = NULL;
    }
    if (NULL != sync_processor_) 
    {
        delete sync_processor_;
        sync_processor_ = NULL;
    }

    if (NULL != net_server_) 
    {
        delete net_server_;
        net_server_ = NULL;
    }
    if (NULL != ds_heartbeat_manager_) 
    {
        delete ds_heartbeat_manager_;
        ds_heartbeat_manager_ = NULL;
    }
    if (NULL != dataset_) 
    {
        delete dataset_;
        dataset_ = NULL;
    }
    if (NULL != fs_interface_) 
    {
        delete fs_interface_;
        fs_interface_ = NULL;
    }

    MemPoolDestory();
}

bool DataServerImpl::Init()
{
    is_running_ = true;
    AtomicSet(&num_connection_, 0);

    config_ = &(Singleton<DataServerConfig>::Instance()); 

    bool ret = MemPoolInit();
    assert(ret == true);
    //init client_queue
    client_queue_ = new TaskQueue<ClientTask *>;
    assert(client_queue_);
    ret = client_queue_->Init(config_->client_queue_size());
    assert(ret == true);
    //init ns queue
    ns_queue_ = new TaskQueue<NSTask *>;
    assert(ns_queue_);
    ret = ns_queue_->Init(config_->ns_queue_size());
    assert(ret == true);
    //init sync queue
    sync_queue_ = new TaskQueue<map<BlockInfo, set<uint64_t> > >;
    assert(sync_queue_);
    ret = sync_queue_->Init(config_->sync_queue_size());
    assert(ret == true);

    // create processor
    client_processor_ = new ClientTaskProcessor(this);
    assert(client_processor_);

    ns_processor_ = new NSTaskProcessor(this);
    assert(ns_processor_);

    sync_processor_ = new SyncTaskProcessor(this);
    assert(sync_processor_);

    net_server_ = new DataNetServer(this);
    assert(net_server_);

    ds_heartbeat_manager_ = new DSHeartbeatManager(this);
    assert(ds_heartbeat_manager_);

    //init local storage
    string block_storage_directory = config_->block_storage_directory();
    string temp_directory = config_->temp_directory();
    dataset_ = new FSDataSet();
    assert(dataset_);
    dataset_->Init(block_storage_directory, temp_directory);

    fs_interface_ = new FSDataInterface(dataset_);
    assert(fs_interface_);

    LOGV(LL_DEBUG, "dataserver_impl init done!!");
    return true;
}

bool DataServerImpl::Start()
{
    bool ret;
    //start net server
    ret = net_server_->Start();
    if (false == ret) 
    {
        LOGV(LL_FATAL, "start net server failed");
        return ret;
    }

    //start process  threads
    ret = StartThreads();
    if (false == ret) 
    {
        LOGV(LL_FATAL, "start process threads failed");
        return ret;
    }

    //start monitor thread
    ret = Singleton<DataServerStatManager>::Instance().Start(this);
    if (false == ret) 
    {
        LOGV(LL_FATAL, "start stat_manager thread failed");
        return ret;
    }

    //start heartbeat thread
    ret = ds_heartbeat_manager_->Start();
    if (false == ret) 
    {
        LOGV(LL_FATAL, "start heartbeat thread failed");
        return ret;
    }
    
    return true;
}

bool DataServerImpl::StartThreads()
{
    /*###start client threads
    ###num of thread started successfully must be more than  or equal to half
    the worker_number */
    int threads_number_for_client = config_->thread_num_for_client();
    int threads_number_for_ns = config_->thread_num_for_ns();
    int threads_number_for_sync = config_->thread_num_for_sync();
    CThread *thread;

    int i;//TODO for dubug use
    int j = 0;// the num of threads started success

    //start work_threads that handle packets from client or ds
    for (i = 0; i < threads_number_for_client; ++i) 
    {
        thread = new CThread();
        assert(thread != NULL);
        if (!(thread->Start(client_processor_, this))) 
        {
            ++j;
            LOGV(LL_INFO, "start client thread %d success", i);
        } 
        else 
            LOGV(LL_ERROR, "start client thread %d failed", i);
    }

    int threads_number_least = threads_number_for_client / 2;
    if (j < threads_number_least) 
    {
        LOGV(LL_FATAL, "client process thread started:%d successfully is less"
             "than half the threads_number:%d", j, threads_number_least);
        return false;
    } 
    else
        LOGV(LL_INFO, "start client threads success,num:%d", j);

    //start work_threads that handle packets from nameserver
    j = 0;
    for (i = 0; i < threads_number_for_ns; ++i) 
    {
        thread = new CThread();
        assert(thread != NULL);
        if (!(thread->Start(ns_processor_, this))) 
        {
            ++j;
            LOGV(LL_INFO, "start ns thread %d success", i);
        } else 
            LOGV(LL_ERROR, "start ns thread %d failed", i);
    }

    threads_number_least = threads_number_for_ns / 2;
    if (j < threads_number_least) 
    {
        LOGV(LL_FATAL, "ns process thread started:%d successfully is less"
             "than half the threads_number:%d", j, threads_number_least);
        return false;
    } 
    else
        LOGV(LL_INFO, "start ns threads success,num:%d", j);
    
    //start work_threads that handle packets that need sync
    j = 0;
    for (i = 0; i < threads_number_for_sync; ++i) 
    {
        thread = new CThread();
        assert(thread != NULL);
        if (!(thread->Start(sync_processor_, this))) 
        {
            ++j;
            LOGV(LL_INFO, "start sync thread %d success", i);
        } 
        else 
            LOGV(LL_ERROR, "start sync thread %d failed", i);
    }

    threads_number_least = threads_number_for_sync / 2;
    if (j < threads_number_least) 
    {
        LOGV(LL_FATAL, "sync process thread started:%d successfully is less"
             "than half the threads_number:%d", j, threads_number_least);
        return false;
    } 
    else
        LOGV(LL_INFO, "start sync threads success,num:%d", j);
    
    return true;
}

bool DataServerImpl::Stop()
{
    is_running_ = false;
    return true;
}

bool DataServerImpl::PushClientQueue(ClientTask *task,bool wait)
{
    // define the timeout in config file or  common define
    bool ret;
    if (true == wait) 
    {
        ret = client_queue_->WaitTimePush(task, DS_PUSH_TIME);
    } 
    else 
    {
        ret = client_queue_->WaitTimePush(task, 0);
    }
    return ret;
}

bool DataServerImpl::PushNameServerQueue(NSTask *task, bool wait)
{
    bool ret;
    if (true == wait) 
    {
        ret = ns_queue_->WaitTimePush(task, DS_PUSH_TIME);
    } 
    else 
    {
        ret = ns_queue_->WaitTimePush(task, 0);
    }
    return ret;
}

bool DataServerImpl::PushSyncQueue(map<BlockInfo, 
                set<uint64_t> > &task, bool wait)
{
    bool ret;
    if (true == wait) 
    {
        ret = sync_queue_->WaitTimePush(task, DS_PUSH_TIME);
    } 
    else 
    {
        ret = sync_queue_->WaitTimePush(task, 0);
    }
    return ret;
}

bool DataServerImpl::PopClientQueue(ClientTask *&task)
{
    bool ret = client_queue_->WaitTillPop(task);
    return ret;
}

bool DataServerImpl::PopNameServerQueue(NSTask *&task)
{
    bool ret = ns_queue_->WaitTillPop(task);
    return ret;
}

bool DataServerImpl::PopSyncQueue(map<BlockInfo, set<uint64_t> > &task)
{
    bool ret = sync_queue_->WaitTillPop(task);
    return ret;
}

}//end of namespace  dataserver
}//end of namespace bladestore
