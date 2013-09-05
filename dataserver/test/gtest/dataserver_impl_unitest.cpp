/* Sohu R&D 2012
 * 
 * File   name: dataserver_impl_unitest.cpp
 * Description: This file defines gtest for dataserver_impl.
 * 
 * Version : 1.0
 * Author  : @yyy
 * Date    : 2012-06_30
 */
#include "gtest/gtest.h"
#include "log.h"
#include "amframe.h"
#include "singleton.h"
#include "dataserver_conf.h"
#include "dataserver_impl.h"


using namespace bladestore::common;
using namespace bladestore::message;

namespace bladestore
{
namespace dataserver
{

class TestDataServerImpl : public ::testing::Test
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
    }

    virtual void TearDown()
    {
        if (ds_impl_) {
            delete ds_impl_;
            ds_impl_ = NULL;
        }
        if (log_) {
            delete log_;
            log_ = NULL;
        }
    }
    Log * log_;
    DataServerConfig * config_;
    DataServerImpl * ds_impl_;

};

TEST_F(TestDataServerImpl, Init)
{
    EXPECT_TRUE(ds_impl_->Init());
}

TEST_F(TestDataServerImpl, Start)
{
    //gtest好像测试不了多个线程
    //EXPECT_TRUE(ds_impl_->Start());
//    ds_impl_->Init();

}

TEST_F(TestDataServerImpl, StartThreads)
{
    //EXPECT_TRUE(ds_impl_->StartThreads());
//    ds_impl_->Init();

}

TEST_F(TestDataServerImpl, Stop)
{
    EXPECT_TRUE(ds_impl_->Stop());
//    ds_impl_->Init();
}

}//end of namespace
}//end of namespace
