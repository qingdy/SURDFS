#include <iostream>
#include "log.h"
#include "fsdir.h"
#include "gtest/gtest.h"
#include "helper.h"
#include "block_meta_info.h"
#include "int_to_string.h"

namespace bladestore
{
namespace dataserver
{
class FSDirTest : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
        fsdir_ = new FSDir("/opt/bladestore/fstest");
    }

    virtual void TearDown()
    {
        if(NULL != fsdir_)
        {
            delete fsdir_;
        }
        fsdir_ = NULL;
    }
    FSDir * fsdir_;

};

TEST_F(FSDirTest, AddBlockAndDelBlock)
{   
    int32_t i;
    for(i = 0; i < 200; ++i)
    {
        string add_block_return = fsdir_->AddBlock();
        EXPECT_STREQ("/opt/bladestore/fstest", add_block_return.substr(0,strlen("/opt/bladestore/fstest")).c_str());
        LOGV(LL_INFO,"%d:block path return by add block:%s",i,add_block_return.c_str());
    }

    string subdir_path;
    for(i = 0; i < 4; ++i)
    {
        subdir_path = string("/opt/bladestore/fstest") + kSubDirName + Int32ToString(i) + "/";
        EXPECT_NO_THROW(fsdir_->DelBlock(subdir_path.c_str()));
    }
   
}

TEST_F(FSDirTest, GetBlockmap)
{
    hash_map<int64_t,BlockMetaInfo>  block_map;
    EXPECT_NO_THROW(fsdir_->GetBlockMap(block_map));
    hash_map<int64_t,BlockMetaInfo>::iterator iter = block_map.begin();
    LOGV(LL_INFO,"block map size:%d",block_map.size());
    for(;iter!=block_map.end();iter++)
    {
        EXPECT_GE(iter->second.block_id(),0);
        EXPECT_GE(iter->second.block_version(),0);
        EXPECT_STREQ("/opt/bladestore/fstest",iter->second.block_folderpath().substr(0,12).c_str());
        LOGV(LL_INFO,"block_map:%ld_%d_%ld %s",iter->second.block_id(),iter->second.block_version(),iter->second.file_id(),iter->second.block_folderpath().c_str());
    }
}
}
}
