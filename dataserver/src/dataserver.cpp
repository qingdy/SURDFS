#include <cassert>
#include "log.h"
#include "singleton.h"
#include "dataserver_conf.h"
#include "dataserver_impl.h"
#include "dataserver.h"

namespace bladestore
{
namespace dataserver
{
DataServer::DataServer() 
{
    config_ = NULL;
    ds_impl_ = NULL;
}

DataServer::~DataServer()
{
    if (NULL != ds_impl_) 
        delete ds_impl_;
}

bool DataServer::Init()
{
    //init config & logging  init logging in config
    config_ = &(Singleton<DataServerConfig>::Instance()); 
    assert(config_);

    //init Log::logger_
    Log::logger_.set_log_level(config_->log_level());
    Log::logger_.set_max_file_size(config_->log_max_size());
    Log::logger_.set_max_file_count(config_->log_file_number());
    Log::logger_.SetLogPrefix((config_->log_path()).c_str(), 
                                (config_->log_prefix()).c_str());
    ds_impl_ = new DataServerImpl();
    assert(ds_impl_ != NULL);
    //init dataserver_impl
    bool ret = ds_impl_->Init();

    return ret;
}

bool DataServer::Start()
{
    bool ret = ds_impl_->Start();
    return ret;
}

bool DataServer::Stop()
{
    bool ret = ds_impl_->Stop();
    return ret;
}

}//end of namespace dataserver
}//end of namespace bladestore
