#include <iostream>
#include <vector>
#include "log.h"
#include "fsdir.h"
#include "gtest/gtest.h"
#include "helper.h"
#include "block_meta_info.h"

namespace bladestore
{
namespace dataserver
{
class HelperTest : public ::testing::Test
{
protected:
    virtual void SetUp(){}

    virtual void TearDown(){}
};

TEST_F(HelperTest, IsDirName)
{
    EXPECT_EQ(CHelper::IsDirName("subdiraaa"),-1);
    EXPECT_EQ(CHelper::IsDirName("subdir123"),0);
    EXPECT_EQ(CHelper::IsDirName("sasdfadir1"),-1);
    EXPECT_EQ(CHelper::IsDirName("subdir44"),0);
    EXPECT_EQ(CHelper::IsDirName("subdir0"),0);
    EXPECT_EQ(CHelper::IsDirName("subdir63"),0);
    EXPECT_EQ(CHelper::IsDirName("subdir512"),-1);
}

TEST_F(HelperTest, GetBlockMetaInfoFromMetaPath)
{
    BlockMetaInfo block_meta_info;
    EXPECT_NO_THROW(CHelper::GetBlockMetaInfoFromMetaPath("/opt/bladestore/fstest/asdfasdf",block_meta_info));
    EXPECT_NO_THROW(CHelper::GetBlockMetaInfoFromMetaPath("blk_12_2_1312341234123412.meta",block_meta_info));

    EXPECT_EQ(block_meta_info.block_id(),12);
    EXPECT_EQ(block_meta_info.block_version(),2);
    EXPECT_EQ(block_meta_info.file_id(),1312341234123412); 
}

TEST_F(HelperTest, GetSubdirIndex)
{
    EXPECT_EQ(CHelper::GetSubdirIndex("subdir63"),63);
    EXPECT_EQ(CHelper::GetSubdirIndex("subdir0"),0);
    EXPECT_NO_THROW(CHelper::GetSubdirIndex("asdasdasdf"));
}

TEST_F(HelperTest, IsBlockFile)
{
    EXPECT_NO_THROW(CHelper::IsBlockFile("blk_aaa"));
    EXPECT_EQ(CHelper::IsBlockFile("asdfasdfasdf"),-1);
    EXPECT_EQ(CHelper::IsBlockFile("blk_111"),0);
    EXPECT_EQ(CHelper::IsBlockFile("blk_aaa"),-1);
    EXPECT_EQ(CHelper::IsBlockFile("blk_aaa_111"),-1);
}

TEST_F(HelperTest, IsMetaFile)
{
    EXPECT_NO_THROW(CHelper::IsMetaFile("asdasdfasdfasdf"));
    EXPECT_NO_THROW(CHelper::IsMetaFile("blk_111"));
    EXPECT_EQ(CHelper::IsMetaFile("blk_111_111_111.meta"),0);
    EXPECT_EQ(CHelper::IsMetaFile("blk_2_1_50.meta"),0);
    EXPECT_EQ(CHelper::IsMetaFile("blk_aaa"),-1);
}

TEST_F(HelperTest, GetSubdirIndexVector)
{
    char *block_folderpath = "/opt/bladestore/fstest/subdir99/";
    vector<int32_t> index;

    EXPECT_NO_THROW(CHelper::GetSubdirIndexVector("/opt/bladestore/fstest/", block_folderpath, index));
    EXPECT_EQ(index.size(),1);
    EXPECT_EQ(index[0],99);
   
}

TEST_F(HelperTest, DirCheckAndCreate)
{
    char *block_folderpath = "/opt/bladestore/fstest/subdir000";
    EXPECT_EQ(CHelper::DirCheckAndCreate(block_folderpath),0);
}
}
}
