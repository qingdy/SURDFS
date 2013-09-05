#include "fsdataset.h"
#include "gtest/gtest.h"
#include "log.h"
#include "file_utils.h"
 
namespace bladestore
{

namespace dataserver
{

class FSDataSetTest : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
        //log_ = new Log(".","",LL_DEBUG);
        fsdataset_ = new FSDataSet();
        fsdataset_->Init("/opt/bladestore/fstest","/opt/bladestore/fstest_tmp");
        block_num_test = 100;
        buf_size = 64*1024*1024;
    }

    virtual void TearDown()
    {
        delete fsdataset_;
        fsdataset_ = NULL;
    }
    FSDataSet   *fsdataset_;
    int32_t     block_num_test;
    int64_t     buf_size;
};
TEST_F(FSDataSetTest, Construct)
{
    hash_map<int64_t, BlockMetaInfo> block_map;
    fsdataset_->GetBlockMap(block_map);
}

TEST_F(FSDataSetTest,WriteBlockAndMetaFd)
{
    WriteBlockFd *writeblockfd;
    FileUtils file_utils;
    char *buf;
    buf = (char *)malloc(buf_size);
    memset(buf,'s',buf_size);
    for(int32_t i = 1;i <= block_num_test;i++)
    {
        Block b(i,0,1,50);
        EXPECT_NO_THROW(writeblockfd = fsdataset_->WriteBlockAndMetaFd(b));
        EXPECT_EQ(writeblockfd->return_code_,1);
        EXPECT_GE(writeblockfd->block_fd_,0);
        EXPECT_GE(writeblockfd->meta_fd_,0);
        EXPECT_EQ(ftruncate(writeblockfd->meta_fd_, 16*1024), 0);
        if(writeblockfd->return_code_ == 1)
        {
            file_utils.set_fd(writeblockfd->block_fd_);
            file_utils.write(buf,buf_size);
        }
        fsdataset_->CloseFile(b,writeblockfd->block_fd_,writeblockfd->meta_fd_,kWriteCompleteMode);
    }
}

TEST_F(FSDataSetTest,PrepareReplicate)
{
    WriteBlockFd *writeblockfd;
    FileUtils file_utils;
    char *buf;
    buf = (char *)malloc(buf_size);
    memset(buf,'s',buf_size);
    for(int32_t i = block_num_test+1;i <= block_num_test*2;i++)
    {
        Block b(i,0,1,50);
        EXPECT_NO_THROW(writeblockfd = fsdataset_->PrepareReplicate(b));
        EXPECT_EQ(writeblockfd->return_code_,1);
        EXPECT_GE(writeblockfd->block_fd_,0);
        EXPECT_GE(writeblockfd->meta_fd_,0);
        EXPECT_EQ(ftruncate(writeblockfd->meta_fd_, 16*1024), 0);
        if(writeblockfd->return_code_ == 1)
        {
            file_utils.set_fd(writeblockfd->block_fd_);
            file_utils.write(buf,buf_size);
        }
        fsdataset_->CloseFile(b,writeblockfd->block_fd_,writeblockfd->meta_fd_,kReplicateCompleteMode);
    }
}

TEST_F(FSDataSetTest, ReadBlockAndMetaFile)
{
    Block *b = new Block(2);
    b->set_block_version(1);

    ReadBlockInfo *read_block_info;
    EXPECT_NO_THROW(read_block_info = fsdataset_->ReadBlockAndMetaFile(*b));
    EXPECT_EQ(read_block_info->return_code_,1);
    EXPECT_GE(read_block_info->block_fd_,0);
    EXPECT_GE(read_block_info->meta_fd_,0);
    EXPECT_EQ(read_block_info->block_length_,buf_size);
}

TEST_F(FSDataSetTest,GetBlockLength)
{
    for(int32_t i = 1; i < block_num_test*2; i++)
    {
        Block b(i,0,1);
        EXPECT_EQ(fsdataset_->GetBlockLength(b),buf_size);
    }
}

TEST_F(FSDataSetTest,GetBlockReport)
{
    set<BlockInfo *> block_info_set;
    for(int32_t i = 1;i <= block_num_test*2;i++)
    {
        BlockInfo *block_info = new BlockInfo(i,1);
        block_info_set.insert(block_info);
    }
    EXPECT_NO_THROW(fsdataset_->GetBlockReport(block_info_set));
    hash_map<int64_t, BlockMetaInfo> block_map;
    EXPECT_NO_THROW(fsdataset_->GetBlockMap(block_map));
    EXPECT_EQ(block_map.size(),block_num_test*2);
}
TEST_F(FSDataSetTest,RemoveBlocks)
{
    set<int64_t> block_id_set;
    for(int32_t i = 1; i <= block_num_test*2;i++)
    {
        block_id_set.insert(i);
    }
    EXPECT_NO_THROW(fsdataset_->RemoveBlocks(block_id_set));
}
}
}
