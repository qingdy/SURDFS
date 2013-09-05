#include "client_conf.h"
#include "gtest/gtest.h"

namespace bladestore
{

namespace client
{

class ClientConfTest : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
        client_conf_ = new ClientConf();
    }

    virtual void TearDown()
    {
        delete client_conf_;
        client_conf_ = NULL;
    }
    ClientConf * client_conf_;
};

TEST_F(ClientConfTest, GetWorkingDir)
{
    EXPECT_STREQ("/root", client_conf_->working_dir().c_str());
}

TEST_F(ClientConfTest, GetNameserverAddr)
{
    EXPECT_STREQ("10.10.72.116:9000",
                 client_conf_->nameserver_addr().c_str());
}

TEST_F(ClientConfTest, GetHomePath)
{
    EXPECT_STREQ("/root", client_conf_->home_path().c_str());
}

TEST_F(ClientConfTest, GetLogDirPath)
{
    EXPECT_STREQ("../log", client_conf_->log_dir_path().c_str());
}

TEST_F(ClientConfTest, GetCacheMaxSize)
{
    EXPECT_EQ(512, client_conf_->cache_max_size());
}

}
}
