/*
*version : 1.0
*author  : funing
*date    : 2012-6-8
*/
#include "nameserver_impl.h"
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "gtest/gtest.h"
#include "blade_meta_manager.h"
#include "layout_manager.h"
#include "block_scanner_manager.h"
#include "block_replicate.h"
#include "block_redundant.h"


namespace bladestore
{

namespace nameserver
{
using namespace nameserver;

class T
{
public:
   int32_t  push(BladePacket *){return 1;}
};

class FateLayout: public :: BladeLayoutManager
{
public:
     
};


class FateNS: public :: NameServerImpl
{
public:
    FateNS(T &t1, T &t2, T &t3):heartbeat_manager_thread_(t1), read_packet_queue_thread_(t2), write_packet_queue_thread_(t3){}
    T & get_heartbeat_manager_thread(){return heartbeat_manager_thread_;}
    T & get_read_packet_queue_thread(){return read_packet_queue_thread_;}
    T & get_write_packet_queue_thread(){return write_packet_queue_thread_;} 
private: 
    T & heartbeat_manager_thread_;
    T & read_packet_queue_thread_;
    T & write_packet_queue_thread_; 
};

class FateMeta: public :: MetaManager
{
public:
    FateMeta(NameServerImpl * f):MetaManager(f), layout_manager_(){}
    BladeLayoutManager & get_layout_manager(){return layout_manager_;}
private: 
    BladeLayoutManager layout_manager_;
};

class BlockScannerManagerTest : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
        T * t1 = new T();
        T * t2 = new T();
        T * t3 = new T();
        FateNS * n = new FateNS(*t1, *t2, *t3);
        m_ = new FateMeta(n);
        redundant_launcher_ = new BlockRedundantLauncher(*m_);
        b_ = new BlockScannerManager(*m_);
        ASSERT_TRUE(b_ != NULL);
    }
    virtual void TearDown()
    {
    }
    void init();
    void LayoutInit();
    void InsertBlock(int rep, int num, BladeLayoutManager &);
    void SPInsertBlock(int rep, int num, BladeLayoutManager &);
    void ClearBlock(BladeLayoutManager &);

    BlockScannerManager::Scanner * replicate_scanner_;
    BlockScannerManager::Scanner * redundant_scanner_;
    
    FateMeta * m_;
    BlockScannerManager * b_;
    ReplicateLauncher * replicate_launcher_;
    BlockRedundantLauncher *  redundant_launcher_;
};

void BlockScannerManagerTest::init()
{
    int max_redundant_size = 10000;
    int max_replicate_size = 10000;

    std::set<int64_t> replicate_blocks;
    std::set<int64_t> redundant_blocks;
    replicate_blocks.clear();
    redundant_blocks.clear();

    ReplicateLauncher replicate_launcher(*m_);
    BlockRedundantLauncher redundant_launcher(*m_);

    //BladeLayoutManager *p = &(b_->meta_manager_.get_layout_manager());   
    //replicate_scanner_ = new BlockScannerManager::Scanner(true, max_replicate_size, replicate_launcher, replicate_blocks);
    redundant_scanner_ = new BlockScannerManager::Scanner(true, max_redundant_size, redundant_launcher, redundant_blocks);
    //BlockScannerManager::Scanner redundant_scanner(true, max_redundant_size, * redundant_launcher_, redundant_blocks);

    //b_->add_scanner(0, replicate_scanner_);
   // b_->add_scanner(0, *redundant_scanner_);

    LayoutInit();
}

void BlockScannerManagerTest::InsertBlock(int rep, int num, BladeLayoutManager & lay)
{
    int64_t rand;
    SERVER_MAP_ITER iter;
    for(int i = 1; i <= num; i++)
    {
        BladeBlockCollect * block = new BladeBlockCollect();
        block->block_id_ = i;
        block->replicas_number_ = 3;
        rand = i % lay.server_map_.size();//
        
        iter = lay.server_map_.begin();
        iter = lay.server_map_.find(rand);
        if(iter == lay.server_map_.end())
        {
            return ;
        }

        int temp = rep;
        for( ; temp > 0; --temp,++iter)
        {
            if(iter == lay.server_map_.end())
            {
                iter = lay.server_map_.begin();
            }
            iter->second->blocks_hold_.insert(i);
            block->dataserver_set_.insert(iter->second->dataserver_id_);
        }
        
        lay.blocks_map_.insert(BLOCKS_MAP::value_type(i, block));
        EXPECT_EQ(rep, block->dataserver_set_.size()) << "rep is:" << rep ;
        lay.block_to_check_.push(block->block_id_);
    }
    EXPECT_EQ(num, lay.block_to_check_.size());
    EXPECT_EQ(num, lay.blocks_map_.size());

}

//insert block from id: num to id: 2 * num - 1 
void BlockScannerManagerTest::SPInsertBlock(int rep, int num, BladeLayoutManager & lay)
{
    int64_t rand;
    SERVER_MAP_ITER iter;
    for(int i = num; i < 2 * num; i++)
    {
        BladeBlockCollect * block = new BladeBlockCollect();
        block->block_id_ = i;
        block->replicas_number_ = 3;
        rand = i % lay.server_map_.size();//
        
        iter = lay.server_map_.begin();
        iter = lay.server_map_.find(rand);
        if(iter == lay.server_map_.end())
        {
            return ;
        }

        int temp = rep;
        for( ; temp > 0; --temp,++iter)
        {
            if(iter == lay.server_map_.end())
            {
                iter = lay.server_map_.begin();
            }
            iter->second->blocks_hold_.insert(i);
            block->dataserver_set_.insert(iter->second->dataserver_id_);
        }
        
        lay.blocks_map_.insert(BLOCKS_MAP::value_type(i, block));
        EXPECT_EQ(rep, block->dataserver_set_.size()) << "rep is:" << rep ;
        lay.block_to_check_.push(block->block_id_);
    }
    LOGV(LL_DEBUG, "block check size is: %d", lay.block_to_check_.size());
    LOGV(LL_DEBUG, "block size is: %d", lay.blocks_map_.size());
}
void BlockScannerManagerTest::LayoutInit()
{
    BladeLayoutManager & layout = b_->meta_manager_.get_layout_manager();
    for(int i = 0; i <= 9; i++ )
    {
        DataServerInfo * ds_info = new DataServerInfo(i, i);
        ds_info->set_is_alive(true);
        set<DataServerInfo *> temp;                                                                                                              
        temp.insert(ds_info);
        layout.racks_map_.insert(RACKS_MAP::value_type(i, temp)); 
        layout.server_map_.insert(SERVER_MAP::value_type(i, ds_info));
    }
    EXPECT_EQ(10, layout.racks_map_.size());
    EXPECT_EQ(10, layout.server_map_.size());
    
}
void BlockScannerManagerTest::ClearBlock(BladeLayoutManager &layout)
{
    SERVER_MAP_ITER diter = layout.server_map_.begin();
    for( ; diter != layout.server_map_.end(); diter++)
    {
        diter->second->blocks_hold_.clear();
        EXPECT_EQ(0, diter->second->blocks_hold_.size());
    }

    BLOCKS_MAP_ITER biter = layout.blocks_map_.begin();
    for( ; biter != layout.blocks_map_.end(); biter++)
    {
        delete biter->second;
    }
    layout.blocks_map_.clear();
    EXPECT_EQ(0, layout.blocks_map_.size());

    layout.block_to_check_.clear();
    EXPECT_EQ(0, layout.block_to_check_.size());
}
TEST_F(BlockScannerManagerTest, ADDScannerTest)
{
    
}

TEST_F(BlockScannerManagerTest, RunTest)
{
    //init();

    int max_redundant_size = 10000;
    int max_replicate_size = 10000;

    std::set<int64_t> replicate_blocks;
    std::set<int64_t> redundant_blocks;
    replicate_blocks.clear();
    redundant_blocks.clear();

    ReplicateLauncher replicate_launcher(*m_);
    BlockRedundantLauncher redundant_launcher(*m_);

   // BladeLayoutManager *p = &(b_->meta_manager_.get_layout_manager());   
    replicate_scanner_ = new BlockScannerManager::Scanner(true, max_replicate_size, replicate_launcher, replicate_blocks);
    redundant_scanner_ = new BlockScannerManager::Scanner(true, max_redundant_size, redundant_launcher, redundant_blocks);
    //BlockScannerManager::Scanner redundant_scanner(true, max_redundant_size, * redundant_launcher_, redundant_blocks);

    b_->add_scanner(0, replicate_scanner_);
    b_->add_scanner(1, redundant_scanner_);

    LayoutInit();
    BladeLayoutManager & layout = b_->meta_manager_.get_layout_manager();
    {
        InsertBlock(2, 10, layout);
        EXPECT_EQ(BLADE_SUCCESS, b_->run());
        EXPECT_EQ(10, replicate_scanner_->result_.size());
        EXPECT_EQ(0, redundant_scanner_->result_.size());
        ClearBlock(layout);
        replicate_blocks.clear();
        redundant_blocks.clear();
    }
    {
        InsertBlock(3, 10, layout);
        EXPECT_EQ(BLADE_SUCCESS, b_->run());
        EXPECT_EQ(0, replicate_scanner_->result_.size());
        EXPECT_EQ(0, redundant_scanner_->result_.size());
        ClearBlock(layout);
        replicate_blocks.clear();
        redundant_blocks.clear();
    }
    {
        InsertBlock(4, 10, layout);
        EXPECT_EQ(BLADE_SUCCESS, b_->run());
        EXPECT_EQ(0, replicate_scanner_->result_.size());
        EXPECT_EQ(10, redundant_scanner_->result_.size());
        ClearBlock(layout);
        replicate_blocks.clear();
        redundant_blocks.clear();
    }
    {
        SPInsertBlock(2, 10, layout);
        SPInsertBlock(3, 20, layout);
        SPInsertBlock(4, 40, layout);
        EXPECT_EQ(BLADE_SUCCESS, b_->run());
        EXPECT_EQ(10, replicate_scanner_->result_.size());
        EXPECT_EQ(40, redundant_scanner_->result_.size());
        ClearBlock(layout);
        replicate_blocks.clear();
        redundant_blocks.clear();
    }

    {
        InsertBlock(4, 1, layout);
        EXPECT_EQ(BLADE_SUCCESS, b_->run());
        EXPECT_EQ(0, replicate_scanner_->result_.size());
        EXPECT_EQ(1, redundant_scanner_->result_.size());
        ClearBlock(layout);
        replicate_blocks.clear();
        redundant_blocks.clear();
    }

}

TEST_F(BlockScannerManagerTest, ProcessTest)
{
    //init();

    int max_redundant_size = 10000;
    int max_replicate_size = 10000;

    std::set<int64_t> replicate_blocks;
    std::set<int64_t> redundant_blocks;
    replicate_blocks.clear();
    redundant_blocks.clear();

    ReplicateLauncher replicate_launcher(*m_);
    BlockRedundantLauncher redundant_launcher(*m_);

    BladeLayoutManager *p = &(b_->meta_manager_.get_layout_manager());   
    replicate_scanner_ = new BlockScannerManager::Scanner(true, max_replicate_size, replicate_launcher, replicate_blocks);
    redundant_scanner_ = new BlockScannerManager::Scanner(true, max_redundant_size, redundant_launcher, redundant_blocks);
    //BlockScannerManager::Scanner redundant_scanner(true, max_redundant_size, * redundant_launcher_, redundant_blocks);

    b_->add_scanner(0, replicate_scanner_);
    b_->add_scanner(1, redundant_scanner_);

    LayoutInit();
    BladeLayoutManager & layout = b_->meta_manager_.get_layout_manager();
    {
        InsertBlock(2, 10, layout);
        for(int i = 1; i <= 10 ; i++)
        {
            EXPECT_EQ(SCANNER_NEXT, b_->process(i));
        }
        EXPECT_EQ(10, replicate_scanner_->result_.size());
        EXPECT_EQ(0, redundant_scanner_->result_.size());
        ClearBlock(layout);
        replicate_blocks.clear();
        redundant_blocks.clear();
    }
    {
        InsertBlock(3, 10, layout);
        for(int i = 1; i <= 10 ; i++)
        {
            EXPECT_EQ(SCANNER_CONTINUE, b_->process(i)) << " i is:" << i;
        }
        EXPECT_EQ(10, replicate_scanner_->result_.size());
        EXPECT_EQ(10, redundant_scanner_->result_.size());
        ClearBlock(layout);
        replicate_blocks.clear();
        redundant_blocks.clear();
    }
    {
        InsertBlock(4, 10, layout);
        for(int i = 1; i <= 10 ; i++)
        {
            EXPECT_EQ(SCANNER_CONTINUE, b_->process(i));
        }
        EXPECT_EQ(0, replicate_scanner_->result_.size());
        EXPECT_EQ(10, redundant_scanner_->result_.size());
        ClearBlock(layout);
        replicate_blocks.clear();
        redundant_blocks.clear();
    }
    
    {
        InsertBlock(2, 10, layout);
        for(int i = 1; i <= 10 ; i++)
        {
            EXPECT_EQ(SCANNER_NEXT, b_->process(i));
        }
        EXPECT_EQ(10, replicate_scanner_->result_.size());
        EXPECT_EQ(0, redundant_scanner_->result_.size());
        replicate_blocks.clear();
        redundant_blocks.clear();

        for(int i = 11; i <= 20 ; i++)
        {
            EXPECT_EQ(SCANNER_NEXT, b_->process(i));
        }
        EXPECT_EQ(0, replicate_scanner_->result_.size());
        EXPECT_EQ(0, redundant_scanner_->result_.size());
        ClearBlock(layout);
        replicate_blocks.clear();
        redundant_blocks.clear();
    }
    {
        InsertBlock(4, 1, layout);
        for(int i = 1; i <= 1 ; i++)
        {
            EXPECT_EQ(SCANNER_CONTINUE, b_->process(i));
        }
        EXPECT_EQ(0, replicate_scanner_->result_.size());
        EXPECT_EQ(1, redundant_scanner_->result_.size());
        ClearBlock(layout);
        replicate_blocks.clear();
        redundant_blocks.clear();
    }
    
}


}
}
