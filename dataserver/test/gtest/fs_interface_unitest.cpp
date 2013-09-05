/* Sohu R&D 2012
 * 
 * File   name: fs_interface_unitest.h
 * Description: unitest for fs_interface
 * 
 * Version : 1.0
 * Author  : @yyy
 * Date    : 2012-7-5
 */
#include "gtest/gtest.h"
#include "mempool.h"
#include "singleton.h"

#include "fsdataset.h"
#include "fs_interface.h"
#include "blade_net_util.h"
#include "dataserver_conf.h"
#include "bladestore_ops.h"

using namespace bladestore::common;
using namespace bladestore::message;

namespace bladestore
{
namespace dataserver
{
class TestFSDataInterface : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
        bool ret = MemPoolInit();
        assert(ret == true);
        config_ = &(Singleton<DataServerConfig>::Instance()); 
        log_ = new pandora::Log(config_->log_path().c_str(), 
                config_->log_prefix().c_str(), LL_DEBUG); 
        assert(log_);
        log_->set_max_file_size(config_->log_max_size());
        log_->set_max_file_count(config_->log_file_number());

        string block_storage_directory = config_->block_storage_directory();
        string temp_directory = config_->temp_directory();
        dataset_ = new FSDataSet();
        dataset_->Init(block_storage_directory, temp_directory);
        assert(dataset_);

        fs_interface_ = new FSDataInterface(dataset_);
        assert(fs_interface_);
        //准备两个block便于测试读
        Block a(3, 0, 1);
        BlockStatusPtr status = fs_interface_->PrepareWrite(a, kWriteMode);
        dataset_->CloseFile(a, status->blockfile_fd() , status->metafile_fd(),kWriteCompleteMode);

	    BLOCK_CACHE_ITER iter = fs_interface_->block_cache_.find(a.block_id());	
	    if (fs_interface_->block_cache_.end() != iter) {
			    fs_interface_->block_cache_.erase(iter);
        }

        Block b(30, 0, 1);
        status = fs_interface_->PrepareWrite(b, kWriteMode);
        dataset_->CloseFile(b, status->blockfile_fd() , status->metafile_fd(),kWriteCompleteMode);

	    iter = fs_interface_->block_cache_.find(b.block_id());	
	    if (fs_interface_->block_cache_.end() != iter) {
			    fs_interface_->block_cache_.erase(iter);
        }

        char *buffer = new char[512]; 
        getcwd(buffer, 512); 
        printf( "The   current   directory   is:%s",   buffer); 
        string s1(buffer);
        string s2("/storage/data/subdir0/blk_30_1_0.meta");
        printf( "s1:%s,\ns2:%s\n", s1.c_str(), s2.c_str()); 
        string  s3= s1.substr(0, s1.find_last_of("/")) + s2;
        printf( "s3:%s", s3.c_str()); 
        delete[] buffer;
        remove(s3.c_str());

    }

    virtual void TearDown()
    {
        dataset_->RemoveBlock(3);
       // dataset_->RemoveBlock(7);
        dataset_->RemoveBlock(30);
        MemPoolDestory();
        if (dataset_) {
            delete dataset_;
            dataset_ = NULL;
        }
        if (fs_interface_) {
            delete fs_interface_;
            fs_interface_ = NULL;
        }
        if (log_) {
            delete log_;
            log_ = NULL;
        }
        
    }
    DataServerConfig * config_;
    Log * log_;
    FSDataSet * dataset_;
    FSDataInterface *  fs_interface_;

};
//测试环境是存储目录下面有blockid：3的完整数据文件，blockid：5 只有数据文件，其他的都没有对应的文件。
//

TEST_F(TestFSDataInterface, PrepareRead)
{
    Block b(4, 0, 1);
    EXPECT_EQ(RET_BLOCK_NOT_EXIST_IN_DS, (fs_interface_->PrepareRead(b))->ret_code());
    Block c(30, 0, 1);
    EXPECT_EQ(RET_BLOCK_OR_META_FILE_NOT_EXIST, (fs_interface_->PrepareRead(c))->ret_code());

    b.set_block_id(3);
    EXPECT_EQ(RET_SUCCESS, (fs_interface_->PrepareRead(b))->ret_code());
    b.set_block_version(2);
    EXPECT_EQ(RET_READ_VERSION_NOT_MATCH, (fs_interface_->PrepareRead(b))->ret_code());


}

TEST_F(TestFSDataInterface, PrepareWrite)
{
    //test success case and blockstatus exist 
    Block b(7, 0, 1);
    BlockStatusPtr status = fs_interface_->PrepareWrite(b, kWriteMode);
    EXPECT_EQ(RET_SUCCESS, status->ret_code());
    EXPECT_EQ(RET_SUCCESS, (fs_interface_->PrepareWrite(b, kWriteMode))->ret_code());
    EXPECT_EQ(RET_WRITE_MODE_INVALID, (fs_interface_->PrepareWrite(b, kReplicateMode))->ret_code());
    EXPECT_EQ(RET_OPERATION_ON_GOING, (fs_interface_->PrepareWrite(b, kSafeWriteMode))->ret_code());
    EXPECT_EQ(RET_WRITE_MODE_INVALID, (fs_interface_->PrepareWrite(b, 10))->ret_code());
    EXPECT_EQ(RET_OPERATION_ON_GOING, (fs_interface_->PrepareRead(b))->ret_code());

    dataset_->CloseFile(b, status->blockfile_fd(), status->metafile_fd() ,kWriteMode);

    b.set_block_id(8);
    status = fs_interface_->PrepareWrite(b, kSafeWriteMode);
    EXPECT_EQ(RET_SUCCESS, status->ret_code());
    EXPECT_EQ(RET_SUCCESS, (fs_interface_->PrepareWrite(b, kSafeWriteMode))->ret_code());
    EXPECT_EQ(RET_WRITE_MODE_INVALID, (fs_interface_->PrepareWrite(b, kReplicateMode))->ret_code());
    EXPECT_EQ(RET_OPERATION_ON_GOING, (fs_interface_->PrepareWrite(b, kWriteMode))->ret_code());
    EXPECT_EQ(RET_WRITE_MODE_INVALID, (fs_interface_->PrepareWrite(b, 10))->ret_code());
    //test read after write while write not complete  with safemode
    EXPECT_EQ(RET_SUCCESS, (fs_interface_->PrepareRead(b))->ret_code());

    dataset_->CloseFile(b, status->blockfile_fd(), status->metafile_fd() ,kWriteMode);

    //block  already exists 
    b.set_block_id(3);
    EXPECT_EQ(RET_BLOCK_FILE_EXISTS, (fs_interface_->PrepareWrite(b, kSafeWriteMode))->ret_code());
    EXPECT_EQ(RET_BLOCK_FILE_EXISTS, (fs_interface_->PrepareWrite(b, kWriteMode))->ret_code());

    b.set_block_version(0);
    EXPECT_EQ(RET_PIPELINE_VERSION_NOT_INVALID, (fs_interface_->PrepareWrite(b, kWriteMode))->ret_code());

    b.Set(3, 0, 2);
    status = fs_interface_->PrepareWrite(b, kWriteMode);
    EXPECT_EQ(RET_SUCCESS, status->ret_code());
    dataset_->CloseFile(b, status->blockfile_fd(), status->metafile_fd() ,kWriteMode);

	BLOCK_CACHE_ITER iter = fs_interface_->block_cache_.find(b.block_id());	
	if (fs_interface_->block_cache_.end() != iter) {
			fs_interface_->block_cache_.erase(iter);
    }

    b.Set(4, 0, 1);
    status = fs_interface_->PrepareWrite(b, kWriteMode);
    EXPECT_EQ(RET_SUCCESS, status->ret_code());
    // mode is not valid
    EXPECT_EQ(RET_WRITE_MODE_INVALID, (fs_interface_->PrepareWrite(b, 10))->ret_code());
    dataset_->CloseFile(b, status->blockfile_fd(), status->metafile_fd() ,kWriteMode);
}

TEST_F(TestFSDataInterface, PrepareReplicate)
{
    Block a(7, 0, 1);
    BlockStatusPtr  status = fs_interface_->PrepareReplicate(a);
    EXPECT_EQ(RET_SUCCESS, status->ret_code());
    EXPECT_EQ(RET_SUCCESS, (fs_interface_->PrepareReplicate(a))->ret_code());
    dataset_->CloseFile(a, status->blockfile_fd(), status->metafile_fd() ,kReplicateMode);

    Block b(6, 0, 1);
    status = fs_interface_->PrepareWrite(b, kWriteMode);
    EXPECT_EQ(RET_SUCCESS, status->ret_code());
    EXPECT_EQ(RET_OPERATION_ON_GOING, (fs_interface_->PrepareReplicate(b))->ret_code());
    dataset_->CloseFile(b, status->blockfile_fd(), status->metafile_fd() ,kWriteMode);


}

TEST_F(TestFSDataInterface, ReadAferWrite)
{
    Block b(6, 0, 1);
    BlockStatusPtr status = fs_interface_->PrepareWrite(b, kWriteMode);
    EXPECT_EQ(RET_SUCCESS, status->ret_code());
    EXPECT_EQ(RET_SUCCESS, (fs_interface_->PrepareWrite(b, kWriteMode))->ret_code());
    EXPECT_EQ(RET_OPERATION_ON_GOING, (fs_interface_->PrepareWrite(b, kSafeWriteMode))->ret_code());
    EXPECT_EQ(RET_WRITE_MODE_INVALID, (fs_interface_->PrepareWrite(b, 10))->ret_code());
    EXPECT_EQ(RET_OPERATION_ON_GOING, (fs_interface_->PrepareRead(b))->ret_code());

	BLOCK_CACHE_ITER iter = fs_interface_->block_cache_.find(b.block_id());	
	if (fs_interface_->block_cache_.end() != iter) {
			iter->second->mode_ = kWriteCompleteMode;
    }
    EXPECT_EQ(RET_SUCCESS, (fs_interface_->PrepareRead(b))->ret_code());

    dataset_->CloseFile(b, status->blockfile_fd(), status->metafile_fd() ,kWriteMode);

    b.set_block_id(3);
    EXPECT_EQ(RET_SUCCESS, (fs_interface_->PrepareRead(b))->ret_code());
    EXPECT_EQ(RET_OPERATION_ON_GOING, (fs_interface_->PrepareWrite(b, kWriteMode))->ret_code());

}

TEST_F(TestFSDataInterface, GetBlockStatus)
{
    Block b(6, 0, 1);
    EXPECT_TRUE((fs_interface_->GetBlockStatus(b))->ret_code() == RET_WRITE_ITEM_NOT_IN_MAP);

    BlockStatusPtr status = fs_interface_->PrepareWrite(b, kWriteMode);
    EXPECT_EQ(RET_SUCCESS, status->ret_code());
    EXPECT_TRUE((fs_interface_->GetBlockStatus(b))->ret_code() == RET_SUCCESS);

    dataset_->CloseFile(b, status->blockfile_fd(), status->metafile_fd() ,kWriteMode);

}


TEST_F(TestFSDataInterface, Complete)
{
    Block b(6, 0, 1);
    EXPECT_EQ(BLADE_ERROR, fs_interface_->Complete(b, kReadMode));

    BlockStatusPtr status = fs_interface_->PrepareWrite(b, kWriteMode);
    EXPECT_EQ(RET_SUCCESS, status->ret_code());
    EXPECT_EQ(BLADE_SUCCESS, fs_interface_->Complete(b, kWriteMode));
    dataset_->CloseFile(b, status->blockfile_fd(), status->metafile_fd() ,kWriteMode);

    b.set_block_id(7);
    status = fs_interface_->PrepareWrite(b, kSafeWriteMode);
    EXPECT_EQ(RET_SUCCESS, status->ret_code());
    EXPECT_EQ(BLADE_SUCCESS, fs_interface_->Complete(b, kSafeWriteMode));

    b.set_block_version(2);
    EXPECT_EQ(BLADE_ERROR, fs_interface_->Complete(b, kSafeWriteMode));

    b.set_block_version(1);
    dataset_->CloseFile(b, status->blockfile_fd(), status->metafile_fd() ,kWriteMode);

}
//TODO 
TEST_F(TestFSDataInterface, check)
{
    Block b(6, 0, 1);
    BlockStatusPtr status = fs_interface_->PrepareWrite(b, kWriteMode);
    EXPECT_EQ(RET_SUCCESS, status->ret_code());
	BLOCK_CACHE_ITER iter = fs_interface_->block_cache_.find(b.block_id());	
    EXPECT_TRUE (fs_interface_->block_cache_.end() != iter);
	if (fs_interface_->block_cache_.end() != iter) {
    }

    EXPECT_EQ(1,fs_interface_->block_cache_.size());

    b.set_block_id(7);
    status = fs_interface_->PrepareWrite(b, kWriteMode);
    EXPECT_EQ(RET_SUCCESS, status->ret_code());
	iter = fs_interface_->block_cache_.find(b.block_id());	
    EXPECT_TRUE (fs_interface_->block_cache_.end() != iter);
	if (fs_interface_->block_cache_.end() != iter) {
    }
    EXPECT_EQ(2,fs_interface_->block_cache_.size());

    b.set_block_id(8);
    EXPECT_EQ(RET_SUCCESS, (fs_interface_->PrepareWrite(b, kSafeWriteMode))->ret_code());
	iter = fs_interface_->block_cache_.find(b.block_id());	
    EXPECT_TRUE (fs_interface_->block_cache_.end() != iter);
	if (fs_interface_->block_cache_.end() != iter) {
    }
    EXPECT_EQ(3,fs_interface_->block_cache_.size());

    b.set_block_id(3);
    EXPECT_EQ(RET_SUCCESS, (fs_interface_->PrepareRead(b))->ret_code());
	iter = fs_interface_->block_cache_.find(b.block_id());	
    EXPECT_TRUE(fs_interface_->block_cache_.end() != iter);
    fs_interface_->block_expire_time_ = 5;
    sleep(1);

    EXPECT_EQ(4,fs_interface_->block_cache_.size());
    fs_interface_->CheckBlock();
    EXPECT_EQ(0,fs_interface_->block_cache_.size());


    b.set_block_id(8);
    EXPECT_EQ(RET_SUCCESS, (fs_interface_->PrepareRead(b))->ret_code());
    EXPECT_EQ(1,fs_interface_->block_cache_.size());

    dataset_->RemoveBlock(6);
    dataset_->RemoveBlock(7);
    dataset_->RemoveBlock(8);

}

}//end of namespace
}//end of namespace



