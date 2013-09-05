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
#include "block_redundant.h"
#include "monitor_manager.h"

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

class BlockRedundantTest : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
        T * t1 = new T();
        T * t2 = new T();
        T * t3 = new T();
        FateNS * n = new FateNS(*t1, *t2, *t3);
        m_ = new FateMeta(n);
        b_ = new BlockRedundantLauncher(*m_);
        monitor_ = new MonitorManager(b_->meta_manager_.get_layout_manager());
        ASSERT_TRUE(b_ != NULL);
    }
    virtual void TearDown()
    {
    }
    void init();
    void LayoutInit();
    void InsertDS(int rack, int sum, BladeLayoutManager &);
    void InsertBlock(int rep, int num, BladeLayoutManager &);
    void SPInsertBlock(int rep, int num, BladeLayoutManager &);
    void ClearBlock(BladeLayoutManager &);
    void ClearLayout(BladeLayoutManager &);

    MonitorManager * monitor_; 
    FateMeta * m_;
    BlockRedundantLauncher * b_;
};

void BlockRedundantTest::InsertDS(int rack, int sum, BladeLayoutManager & layout)
{
    set<DataServerInfo *> temp;  
    int size = layout.server_map_.size();
    for(int i = size; i < (sum + size); i++)
    {
        DataServerInfo * ds_info = new DataServerInfo(rack, i);
        ds_info->set_is_alive(true);
        temp.insert(ds_info);
        layout.server_map_.insert(SERVER_MAP::value_type(i, ds_info));
    }
    layout.racks_map_.insert(RACKS_MAP::value_type(rack, temp));
    EXPECT_EQ(sum, layout.racks_map_[rack].size()); 
    
    SERVER_MAP_ITER diter = layout.server_map_.begin();
    for(int i = size; i < (sum + size); i++)
    {
        diter = layout.server_map_.find(i);
        EXPECT_TRUE(diter != layout.server_map_.end());
    }
    
}

void BlockRedundantTest::InsertBlock(int rep, int num, BladeLayoutManager & lay)
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
void BlockRedundantTest::SPInsertBlock(int rep, int num, BladeLayoutManager & lay)
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
void BlockRedundantTest::LayoutInit()
{
    BladeLayoutManager & layout = b_->meta_manager_.get_layout_manager();
    //add dataserver
    for(int i = 1; i <= 3 ; i++)
    {
        InsertDS(i, 3, layout);
    }
    EXPECT_EQ(3, layout.racks_map_.size());
    EXPECT_EQ(3 * 3, layout.server_map_.size()); 
}
void BlockRedundantTest::ClearBlock(BladeLayoutManager &layout)
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

void BlockRedundantTest::ClearLayout(BladeLayoutManager & layout)
{
    layout.racks_map_.clear();
    layout.server_map_.clear();
    layout.blocks_map_.clear();
    
    EXPECT_EQ(0, layout.racks_map_.size());
    EXPECT_EQ(0, layout.server_map_.size());
    EXPECT_EQ(0, layout.blocks_map_.size());

}



TEST_F(BlockRedundantTest, InitTest)
{
    LayoutInit();
    BladeLayoutManager & layout = b_->meta_manager_.get_layout_manager();
    InsertBlock(2, 10, layout);
    //monitor_->monitor_ns();
    ClearBlock(layout);
}


TEST_F(BlockRedundantTest, CheckBlockTest)
{
    LayoutInit();
    BladeLayoutManager & layout = b_->meta_manager_.get_layout_manager();
    {
        InsertBlock(2, 10, layout);
        for(int i = 1; i <= 10 ; i++)
        {
            EXPECT_EQ(SCANNER_NEXT, b_->check_redundant_block(i)) << i;
        }
        ClearBlock(layout);
    }
    {
        InsertBlock(3, 10, layout);
        for(int i = 1; i <= 10 ; i++)
        {
            EXPECT_EQ(SCANNER_CONTINUE, b_->check_redundant_block(i)) << i;
        }
        ClearBlock(layout);
    }
    {
        InsertBlock(4, 10, layout);
        for(int i = 1; i <= 10 ; i++)
        {
            EXPECT_EQ(SCANNER_CONTINUE, b_->check_redundant_block(i)) << i;
        }
        ClearBlock(layout);
    }
    {
        SPInsertBlock(2, 10, layout);
        SPInsertBlock(3, 20, layout);
        SPInsertBlock(4, 40, layout);
    
        for(int i = 1; i <= 9 ; i++)
        {
            EXPECT_EQ(SCANNER_NEXT, b_->check_redundant_block(i));
        }
        for(int i = 10; i <= 19; i++)
        {
            EXPECT_EQ(SCANNER_NEXT, b_->check_redundant_block(i));
        }
        for(int i = 20; i <= 39; i++)
        {
            EXPECT_EQ(SCANNER_CONTINUE, b_->check_redundant_block(i));
        }
        for(int i = 40; i <= 79; i++)
        {
            EXPECT_EQ(SCANNER_CONTINUE, b_->check_redundant_block(i));
        }
        ClearBlock(layout);
    }
    
    {
        InsertBlock(6, 10, layout);
        for(int i = 1; i <= 10 ; i++)
        {
            EXPECT_EQ(SCANNER_CONTINUE, b_->check_redundant_block(i));
        }
        for(int i = 1; i <= 10 ; i++)
        {
            EXPECT_EQ(SCANNER_CONTINUE, b_->check_redundant_block(i));
        }
        ClearBlock(layout);
    }
    
    {
        InsertBlock(4, 1, layout);
        for(int i = 1; i <= 1; i++)
        {
            EXPECT_EQ(SCANNER_CONTINUE, b_->check_redundant_block(i));
        }
        ClearBlock(layout);
    }
    
    monitor_->monitor_ns();
}

TEST_F(BlockRedundantTest, CalcMaxTest)
{
    LayoutInit();
    BladeLayoutManager & layout = b_->meta_manager_.get_layout_manager();
    {
        InsertBlock(4, 10, layout);
        for(int i = 1; i <= 10; i++)
        {
            BLOCKS_MAP_ITER blocks_map_iter  = layout.blocks_map_.find(i);
            BladeBlockCollect * block_collect = blocks_map_iter->second;
            
            set<uint64_t> test_ds = block_collect->dataserver_set_;
            set<uint64_t> result_ds;
            
            EXPECT_EQ(-1, b_->calc_max_capacity_ds(test_ds, 1, result_ds));
            EXPECT_EQ(2, result_ds.size());
        }
        ClearBlock(layout);
    }
    {
        InsertBlock(9, 10, layout);
        for(int i = 1; i <= 10; i++)
        {
            BLOCKS_MAP_ITER blocks_map_iter  = layout.blocks_map_.find(i);
            BladeBlockCollect * block_collect = blocks_map_iter->second;
            
            set<uint64_t> test_ds = block_collect->dataserver_set_;
            set<uint64_t> result_ds;
            
            EXPECT_EQ(-5, b_->calc_max_capacity_ds(test_ds, 1, result_ds));
            EXPECT_EQ(6, result_ds.size());
        }
        ClearBlock(layout);
    }
    
    {
        ClearLayout(layout);
        InsertDS(1, 1, layout);
        InsertDS(2, 1, layout);
        InsertDS(3, 1, layout);
        InsertBlock(3, 10, layout);
        monitor_->monitor_ns();
        for(int i = 1; i <= 10; i++)
        {
            BLOCKS_MAP_ITER blocks_map_iter  = layout.blocks_map_.find(i);
            BladeBlockCollect * block_collect = blocks_map_iter->second;
            
            set<uint64_t> test_ds = block_collect->dataserver_set_;
            set<uint64_t> result_ds;
            
            EXPECT_EQ(0, b_->calc_max_capacity_ds(test_ds, 1, result_ds));
            EXPECT_EQ(1, result_ds.size());
        }

        for(int i = 1; i <= 10; i++)
        {
            BLOCKS_MAP_ITER blocks_map_iter  = layout.blocks_map_.find(i);
            BladeBlockCollect * block_collect = blocks_map_iter->second;
            
            set<uint64_t> test_ds = block_collect->dataserver_set_;
            set<uint64_t> result_ds;
            
            EXPECT_EQ(0, b_->calc_max_capacity_ds(test_ds, 2, result_ds));
            EXPECT_EQ(2, result_ds.size());
        }
        ClearBlock(layout);   
    }
}
}
}
