/* Sohu R&D 2012
 * 
 * File   name: dataserver_net_handler.cpp
 * Description: This file defines gtest for dataserver_net_handler.
 * 
 * Version : 1.0
 * Author  : @yyy
 * Date    : 2012-06_29
 */
#include "gtest/gtest.h"
#include "log.h"
#include "amframe.h"
#include "singleton.h"
#include "dataserver_impl.h"
#include "dataserver_conf.h"
#include "dataserver_net_handler.h"


using namespace bladestore::common;
using namespace bladestore::message;
using namespace pandora;

namespace bladestore
{
namespace dataserver
{

class TestDataServerNetHandler : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
        config_ = &(Singleton<DataServerConfig>::Instance()); 
        log_ = new pandora::Log(config_->log_path().c_str(), 
                config_->log_prefix().c_str(), LL_DEBUG); 
        assert(log_);
        log_->set_max_file_size(config_->log_max_size());
        log_->set_max_file_count(config_->log_file_number());

        ds_impl_ = new DataServerImpl();
        stream_handler_ = new 
                DataServerStreamSocketHandler(Singleton<AmFrame>::Instance(),
                                              ds_impl_);
        listen_handler_ = new 
                DataServerListenSocketHandler(Singleton<AmFrame>::Instance(), 
                                              ds_impl_);
        //ds_impl_->Init();
    }

    virtual void TearDown()
    {
        if (listen_handler_) {
            delete listen_handler_;
            listen_handler_ = NULL;
        }
        if (stream_handler_) {
            delete stream_handler_;
            stream_handler_ = NULL;
        }
      //  if (ds_impl_) {
      //      delete ds_impl_;
      //      ds_impl_ = NULL;
      //  }
        if (log_) {
            delete log_;
            log_ = NULL;
        }
        ::MemPoolDestory();                                      
    }
    Log * log_;
    DataServerConfig * config_;
    DataServerStreamSocketHandler * stream_handler_; 

    DataServerListenSocketHandler * listen_handler_; 

    DataServerImpl * ds_impl_;

};

//TEST_F(TestDataServerNetHandler, StreamOnReceived)
//{
//
//    EXPECT_TRUE(ds_impl_->Init());
//}

TEST_F(TestDataServerNetHandler, StreamOnConnected)
{
    EXPECT_TRUE(ds_impl_->Init());
    EXPECT_EQ(0,ds_impl_->num_connection());
    stream_handler_->OnConnected();
    EXPECT_EQ(1,ds_impl_->num_connection());
    stream_handler_->OnConnected();
    EXPECT_EQ(2,ds_impl_->num_connection());
    stream_handler_->OnClose(0);
    EXPECT_EQ(1,ds_impl_->num_connection());

}

//TEST_F(TestDataServerNetHandler, StreamOnClose)
//{
//
//}
//
//TEST_F(TestDataServerNetHandler, StreamCreatePacket)
//{
//
//}
//
//TEST_F(TestDataServerNetHandler, ListenOnClose)
//{
//}
//
//TEST_F(TestDataServerNetHandler, ListenOnConnected)
//{
//
//}

}//end of namespace
}//end of namespace
