#include "singleton.h"
#include "amframe.h"
#include "log.h"
#include "dataserver_impl.h"
#include "dataserver_conf.h"
#include "dataserver_net_handler.h"
#include "dataserver_net_server.h"

namespace bladestore
{
namespace dataserver
{

DataNetServer::DataNetServer(DataServerImpl * impl)
{
    listen_handler_ = NULL;
    stream_handler_ = NULL;
    config_ = NULL;
    ds_impl_ = impl;
}

DataNetServer::DataNetServer()
{
    listen_handler_ = NULL;
    stream_handler_ = NULL;
    config_ = NULL;
    ds_impl_ = NULL;
}

DataNetServer::~DataNetServer()
{
    if (NULL != stream_handler_) 
    {
        delete stream_handler_;
        stream_handler_ = NULL;
    }
    if (NULL != listen_handler_) 
    {
        delete listen_handler_;
        listen_handler_ = NULL;
    }
}

bool DataNetServer::Init()
{
    //set max_packet_length to 5M to surport  blockreport all blocks in ds to ns.
    end_point_options_.set_max_packet_length(5*1024*1024);
    config_ = &(Singleton<DataServerConfig>::Instance()); 
    listen_handler_ = 
        new DataServerListenSocketHandler(Singleton<AmFrame>::Instance(), 
                                          ds_impl_);
    if (NULL == listen_handler_) 
        return false;

    return true;
}

bool DataNetServer::Start()
{
    bool ret = Init();
    if (false == ret) 
    {
        LOGV(LL_FATAL, "net server init failed");
        return false;
    }

    ret = Listen();
    if (false == ret) 
    {
        LOGV(LL_FATAL, "listen on the %s failed", config_->dataserver_name());
        return false;
    }
    LOGV(LL_DEBUG, "listen on the %s success", config_->dataserver_name());

    ret = ConnectToNS();
    if (false == ret) 
    {
        LOGV(LL_FATAL, "connect to ns %s failed", config_->nameserver_name());
        return false;
    }
    LOGV(LL_DEBUG, "connect to ns %s success", config_->nameserver_name());

    return true;
}

bool DataNetServer::Listen()
{
    int ret = Singleton<AmFrame>::Instance().AsyncListen(
                       config_->dataserver_name(), listen_handler_);
    if (0 < ret)
        return true;
    else
        return false;
}

bool DataNetServer::ConnectToNS()
{
    LOGV(LL_INFO,"begin connect to NS");
    stream_handler_ = 
        new DataServerStreamSocketHandler(Singleton<AmFrame>::Instance(), 
                                          ds_impl_);
    if (NULL == stream_handler_) 
        return false;
    int64_t ret = Singleton<AmFrame>::Instance().AsyncConnect(
            config_->nameserver_name(), 
            stream_handler_, end_point_options_);
    if (0 < ret) 
    {
        return true;
    } 
    else 
    {
        LOGV(LL_ERROR," error connect to NS");
        return false;
    }
}

DataServerStreamSocketHandler * DataNetServer::ConnectToDS(uint64_t  ds_id)
{
    string ip_port_dest = BladeNetUtil::AddrToString(ds_id);

    DataServerStreamSocketHandler * stream_handler = new 
                DataServerStreamSocketHandler(Singleton<AmFrame>::Instance(), ds_impl_);

    int64_t ret_code = Singleton<AmFrame>::Instance().AsyncConnect(
                     ip_port_dest.c_str(), stream_handler);  
    if (0 >= ret_code) 
    {
        LOGV(LL_ERROR," error connect to next ds:%s", ip_port_dest.c_str());
        delete stream_handler;
        return NULL;
    }
    else 
    {
        LOGV(LL_INFO," success connect to next ds:%s", ip_port_dest.c_str());
        return stream_handler;
    }
}

}//end of namespace dataserver
}//end of namespace bladestore
