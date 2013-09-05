/* Sohu R&D 2012
 * 
 * File   name: dataserver.h
 * Description: This file defines  how dataserver runs.
 * 
 * Version : 1.0
 * Author  : @yyy
 * Date    : 2012-06-26
 */
#include "gtest/gtest.h"
#include "blade_net_util.h"
#include "singleton.h"
#include "blade_common_define.h"
#include "dataserver_conf.h"

using std::string;
using namespace bladestore::common;
using namespace pandora;

namespace bladestore
{
namespace dataserver
{
class TestDataServerConfig : public ::testing::Test
{
protected:
    static void SetUpTestCase()
    {
        ds_config_ = &(Singleton<DataServerConfig>::Instance()); 
    }

    static void TearDownTestCase()
    {
        
    }
    static DataServerConfig *  ds_config_;

};
DataServerConfig  *  TestDataServerConfig::ds_config_;
TEST_F(TestDataServerConfig, TestDataServerName)
{
    EXPECT_STREQ("10.10.72.115:8877", ds_config_->dataserver_name());
}

TEST_F(TestDataServerConfig, TestNameserverName)
{
    EXPECT_STREQ("10.10.72.116:9000", ds_config_->nameserver_name());
}

TEST_F(TestDataServerConfig, TestRackId)
{
    EXPECT_EQ(1, ds_config_->rack_id());
}

TEST_F(TestDataServerConfig, TestStatLog)
{
    EXPECT_STREQ("10.10.72.115_8877.log", ds_config_->stat_log().c_str());
}

TEST_F(TestDataServerConfig, TestStatCheckInterval)
{
    EXPECT_EQ(300, ds_config_->stat_check_interval());
}

TEST_F(TestDataServerConfig, TestDSId)
{
    EXPECT_STREQ("10.10.72.115:8877", (BladeNetUtil::addr_to_string(ds_config_->ds_id())).c_str());
}

TEST_F(TestDataServerConfig, TestNSId)
{
    EXPECT_STREQ("10.10.72.116:9000", (BladeNetUtil::addr_to_string(ds_config_->ns_id())).c_str());
}

TEST_F(TestDataServerConfig, TestThreadNumForClient)
{
    EXPECT_EQ(20, ds_config_->thread_num_for_client());
}

TEST_F(TestDataServerConfig, TestThreadNumForNS)
{
    EXPECT_EQ(10, ds_config_->thread_num_for_ns());
}

TEST_F(TestDataServerConfig, TestThreadNumForSync)
{
    EXPECT_EQ(5, ds_config_->thread_num_for_sync());
}

TEST_F(TestDataServerConfig, TestClientQueueSize)
{
    EXPECT_EQ(1000, ds_config_->client_queue_size());
}

TEST_F(TestDataServerConfig, TestNSQueueSize)
{
    EXPECT_EQ(100, ds_config_->ns_queue_size());
}

TEST_F(TestDataServerConfig, TestSyncQueueSize)
{
    EXPECT_EQ(100, ds_config_->sync_queue_size());
}

TEST_F(TestDataServerConfig, TestBlockStorageDirectory)
{
    EXPECT_STREQ("../storage/data", ds_config_->block_storage_directory().c_str());
}

TEST_F(TestDataServerConfig, TestTempDirectory)
{
    EXPECT_STREQ("../storage/tmp", ds_config_->temp_directory().c_str());
}

TEST_F(TestDataServerConfig, TestLogPath)
{
    EXPECT_STREQ("../log/", ds_config_->log_path().c_str());
}

TEST_F(TestDataServerConfig, TestLogPrefix)
{
    EXPECT_STREQ("dataserver", ds_config_->log_prefix().c_str());
}

TEST_F(TestDataServerConfig, TestLogLevel)
{
    EXPECT_EQ(5, ds_config_->log_level());
}

TEST_F(TestDataServerConfig, TestLogMaxSize)
{
    EXPECT_EQ(100000000, ds_config_->log_max_size());
}

TEST_F(TestDataServerConfig, TestLogFileNumber)
{
    EXPECT_EQ(10, ds_config_->log_file_number());
}

}//end of namespace
}//end of namespace
