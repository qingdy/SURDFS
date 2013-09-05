/* Sohu R&D 2012
 * 
 * File   name: dataserver.h
 * Description: This file defines  how dataserver runs.
 * 
 * Version : 1.0
 * Author  : @yyy
 * Date    : 2012-06-28
 */
#include "gtest/gtest.h"
#include "log.h"
#include "singleton.h"
#include "amframe.h"
#include "dataserver_net_handler.h"
#include "dataserver_net_server.h"
#include "dataserver_conf.h"
#include "dataserver_impl.h"
#include "blade_net_util.h"

using namespace bladestore::common;

namespace pandora
{
class AmFrame;
}

namespace bladestore
{
namespace dataserver
{
class FakeDataServerImpl;

class FakeDataNetServer :public DataNetServer
{
public:
    FakeDataNetServer(DataServerImpl * impl):DataNetServer(impl) {}
    bool Start(bool ret1, bool ret2, bool ret3);
    DataServerStreamSocketHandler   * ConnectToDS(uint64_t ds_id, int64_t);
    bool ConnectToNS(int64_t ret);
    bool Init();
    bool Listen(int ret);
};

bool FakeDataNetServer::Init()
{
    listen_handler_ = 
        new DataServerListenSocketHandler(Singleton<AmFrame>::Instance(), 
                                          ds_impl_);
    if (!listen_handler_) 
        return false;

    return true;
}

bool FakeDataNetServer::Start(bool ret1, bool ret2, bool ret3)
{
//    bool ret = Init();
    if (!ret1) {
        ds_impl_->log()->Write(MSG_FATAL, "net server init failed");
        return false;
    }

 //   ret = Listen();
    if (ret2 == false) {
//        ds_impl_->log()->Write(MSG_FATAL, "listen on the %s failed",
//                DataServerConfig::Instance()->dataserver_name());
        return false;
    }
  //  ds_impl_->log()->Write(MSG_DEBUG, "listen on the %s success",
    //        DataServerConfig::Instance()->dataserver_name());

//    ret = ConnectToNS();
    if (ret3 == false) {
//        ds_impl_->log()->Write(MSG_FATAL, "connect to ns %s failed",
//                DataServerConfig::Instance()->nameserver_name());
        return false;
    }
//    ds_impl_->log()->Write(MSG_DEBUG, "connect to ns %s success",
  //          DataServerConfig::Instance()->nameserver_name());

    return true;
}

bool FakeDataNetServer::Listen(int ret)
{
//    int ret = Singleton<AmFrame>::Instance().AsyncListen(
//            DataServerConfig::Instance()->dataserver_name(), 
//            listen_handler_);
    if (ret > 0)
        return true;
    else
        return false;
}

bool FakeDataNetServer::ConnectToNS(int64_t  ret)
{
    ds_impl_->log()->Write(MSG_INFO,"begin connect to NS");
    stream_handler_ = 
        new DataServerStreamSocketHandler(Singleton<AmFrame>::Instance(), 
                                          ds_impl_);
    if (!stream_handler_) 
        return false;
//    int ret = Singleton<AmFrame>::Instance().AsyncConnect(
//            DataServerConfig::Instance()->nameserver_name(), 
//            stream_handler_);
//    end_point_ = stream_handler_->end_point();
    if (ret > 0)
        return true;
    else
        return false;
}

DataServerStreamSocketHandler * FakeDataNetServer::ConnectToDS(uint64_t  ds_id,int64_t  ret_code)
{
    string ip_port_dest = BladeNetUtil::addr_to_string(ds_id);
    DataServerStreamSocketHandler * stream_handler = new 
                DataServerStreamSocketHandler(Singleton<AmFrame>::Instance(), ds_impl_);
//    int64_t ret_code = Singleton<AmFrame>::Instance().AsyncConnect(
//                     ip_port_dest.c_str(), stream_handler);  
    if (0 >= ret_code) {
        ds_impl_->log()->Write(MSG_ERROR," error connect to next ds: %s", 
                               ip_port_dest.c_str());
        return NULL;
    }
    else {
        ds_impl_->log()->Write(MSG_INFO," success connect to next ds: %s",
                               ip_port_dest.c_str());
        return stream_handler;
    }
}

class FakeDataServerImpl:public DataServerImpl
{
public:
    FakeDataServerImpl():ds_net_server_(NULL)
    {
        ds_net_server_ = new FakeDataNetServer(this);
    }
    ~FakeDataServerImpl()
    {
        if (ds_net_server_) {
            delete ds_net_server_;
            ds_net_server_ = NULL;
        }
    }
    FakeDataNetServer * ds_net_server_;
};

class TestDataServerNetServer : public ::testing::Test
{
protected:
    static void SetUpTestCase()
    {
        config_ = &(Singleton<DataServerConfig>::Instance()); 
        assert(config_);
        Log *log_ = new pandora::Log(config_->log_path().c_str(), 
                    config_->log_prefix().c_str(), LL_DEBUG); 
        assert(log_);
        log_->set_max_file_size(config_->log_max_size());
        log_->set_max_file_count(config_->log_file_number());

        ds_impl_ = new FakeDataServerImpl();
        assert(ds_impl_ != NULL);

        net_server_ = new FakeDataNetServer(ds_impl_) ;
    }

    static void TearDownTestCase()
    {
        if (net_server_) {
            delete net_server_;
            net_server_ = NULL;
        }
        if (ds_impl_) {
            delete ds_impl_;
            ds_impl_ = NULL;
        }
        if (log_) {
            delete log_;
            log_ = NULL;
        }
    }
    static DataServerConfig * config_;
    static Log * log_;
    static FakeDataServerImpl * ds_impl_;
    static FakeDataNetServer *  net_server_;

};

DataServerConfig * TestDataServerNetServer::config_ = NULL;
Log              * TestDataServerNetServer::log_= NULL;
FakeDataServerImpl   * TestDataServerNetServer::ds_impl_ = NULL;
FakeDataNetServer    * TestDataServerNetServer::net_server_ = NULL;
 
TEST_F(TestDataServerNetServer, Init)
{
    EXPECT_TRUE(net_server_->Init());
}
 
TEST_F(TestDataServerNetServer, Start)
{
    EXPECT_FALSE(net_server_->Start(false, true, true));
    EXPECT_FALSE(net_server_->Start(false, false, true));
    EXPECT_FALSE(net_server_->Start(false, true, false));
    EXPECT_FALSE(net_server_->Start(false, false, false));

    EXPECT_TRUE(net_server_->Start(true, true, true));
    EXPECT_FALSE(net_server_->Start(true, false, true));
    EXPECT_FALSE(net_server_->Start(true, true, false));
    EXPECT_FALSE(net_server_->Start(true, false, false));
}
 
TEST_F(TestDataServerNetServer, Listen)
{
    EXPECT_TRUE( net_server_->Listen(1));

    EXPECT_FALSE( net_server_->Listen(0));
    EXPECT_FALSE( net_server_->Listen(-1));
}

 
TEST_F(TestDataServerNetServer, ConnectToNS)
{
    EXPECT_TRUE( net_server_->ConnectToNS(1));
    EXPECT_FALSE( net_server_->ConnectToNS(0));
    EXPECT_FALSE( net_server_->ConnectToNS(-1));

}

TEST_F(TestDataServerNetServer, ConnectToDS)
{
    EXPECT_TRUE( net_server_->ConnectToDS(1, 1));
    EXPECT_FALSE( net_server_->ConnectToDS(1, 0));
    EXPECT_FALSE( net_server_->ConnectToDS(1, -1));
}

}//end of namespace
}//end of namespace
