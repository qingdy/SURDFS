/*
*version : 1.0
*author  : funing
*date    : 2012-6-21
*/
#include "nameserver.h"
#include "bladestore_ops.h"
#include "amframe.h"
#include "blade_common_define.h"
#include "blade_common_data.h"
#include "gtest/gtest.h"
#include "layout_manager.h"
#include "monitor_manager.h"

#include "register_packet.h"
#include "heartbeat_packet.h"

namespace bladestore
{

namespace nameserver
{
using namespace pandora;
using namespace bladestore::nameserver;
using namespace bladestore::common;
class T
{
public:
   int32_t  push(BladePacket *){return 1;}
};


class LayoutManagerTest : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
        monitor_ = new MonitorManager(layout_);
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
    BladeLayoutManager  layout_;
};

void LayoutManagerTest::InsertDS(int rack, int sum, BladeLayoutManager & layout)
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

void LayoutManagerTest::InsertBlock(int rep, int num, BladeLayoutManager & lay)
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
void LayoutManagerTest::SPInsertBlock(int rep, int num, BladeLayoutManager & lay)
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
void LayoutManagerTest::LayoutInit()
{
    //add dataserver
    for(int i = 1; i <= 3 ; i++)
    {
        InsertDS(i, 3, layout_);
    }
    EXPECT_EQ(3, layout_.racks_map_.size());
    //EXPECT_EQ(3 * 3, layout_.server_map_.size()); 
}
void LayoutManagerTest::ClearBlock(BladeLayoutManager &layout)
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

void LayoutManagerTest::ClearLayout(BladeLayoutManager & layout)
{
    layout.racks_map_.clear();
    layout.server_map_.clear();
    layout.blocks_map_.clear();
    
    EXPECT_EQ(0, layout.racks_map_.size());
    EXPECT_EQ(0, layout.server_map_.size());
    EXPECT_EQ(0, layout.blocks_map_.size());

}



TEST_F(LayoutManagerTest, InitTest)
{
    for (int i = 0; i < 100 ; i++)
    {
        LayoutInit();
        monitor_->monitor_ns();
        ClearLayout(layout_);
        monitor_->monitor_ns();
    }
    //InsertBlock(2, 10, layout_);
    //monitor_->monitor_ns();
   // ClearBlock(layout_);
}


TEST_F(LayoutManagerTest, RegisterTest)
{
    {
        BladePacket * p = new RegisterPacket(1,1);
        EXPECT_EQ(BLADE_SUCCESS, layout_.register_ds(p, p));
        EXPECT_EQ(BLADE_SUCCESS, layout_.register_ds(p, p));
        EXPECT_EQ(1, layout_.server_map_.size());
        EXPECT_EQ(1, layout_.racks_map_.size());
        ClearLayout(layout_);
    } 

    {   
        for(int i = 1; i <= 10; i++)
        {
            for(int j = 1; j <= 5; j++)
            {
                BladePacket * p = new RegisterPacket(i, j + 10 * i);
                EXPECT_EQ(BLADE_SUCCESS, layout_.register_ds(p, p));
                EXPECT_EQ(BLADE_SUCCESS, layout_.register_ds(p, p));
            }
            EXPECT_EQ(i, layout_.racks_map_.size());
        }
        EXPECT_EQ(50, layout_.server_map_.size());
        
        for(int i = 1; i <= 10; i++)
        {
            for(int j = 1; j <= 5; j++)
            {
                BladePacket * p = new RegisterPacket(i,j + 10 * i);
                EXPECT_EQ(BLADE_SUCCESS, layout_.register_ds(p, p));
                EXPECT_EQ(BLADE_SUCCESS, layout_.register_ds(p, p));
            }
            EXPECT_EQ(10, layout_.racks_map_.size());
        }
        EXPECT_EQ(50, layout_.server_map_.size());

        //
        SERVER_MAP_ITER iter = layout_.server_map_.begin();
        for( ; iter != layout_.server_map_.end(); iter++)
        {
            iter->second->set_is_alive(false);
            EXPECT_FALSE(iter->second->is_alive());
        }
        
        for(int i = 1; i <= 10; i++)
        {
            for(int j = 1; j <= 5; j++)
            {
                BladePacket * p = new RegisterPacket(i, j + 10 * i);
                EXPECT_EQ(BLADE_SUCCESS, layout_.register_ds(p, p));
                EXPECT_EQ(BLADE_SUCCESS, layout_.register_ds(p, p));
            }
            EXPECT_EQ(10, layout_.racks_map_.size());
        }
        EXPECT_EQ(50, layout_.server_map_.size());
        for( ; iter != layout_.server_map_.end(); iter++)
        {
            EXPECT_TRUE(iter->second->is_alive());
        }
    }
    monitor_->monitor_ns();
}

TEST_F(LayoutManagerTest, UpdateTest)
{
    DataServerMetrics metrics = {1, 1, 1, 1};
    {
        HeartbeatPacket * p = new HeartbeatPacket(1, 1, metrics);
        HeartbeatReplyPacket * q = new HeartbeatReplyPacket();
        EXPECT_EQ(BLADE_ERROR, layout_.update_ds(p, q));
    }

    {
        BladePacket * r = new RegisterPacket(1,1);
        EXPECT_EQ(BLADE_SUCCESS, layout_.register_ds(r, r));
        HeartbeatPacket * p = new HeartbeatPacket(1, 1, metrics);
        HeartbeatReplyPacket * q = new HeartbeatReplyPacket();
        EXPECT_EQ(BLADE_SUCCESS, layout_.update_ds(p, q));
    }
}
        
TEST_F(LayoutManagerTest, RemoveBlockTest)
{
    {
        layout_.remove_block_collect(1);
        layout_.remove_block_collect(2);
        layout_.remove_block_collect(3);
    }

    {
        for(int i = 1; i <= 3 ; i++)
        {
            InsertDS(i, 3, layout_);
        }
        InsertBlock(3, 90, layout_);
        
        monitor_->monitor_ns();
        
        for(int i = 1; i <= 90; i++)
        {
            layout_.remove_block_collect(i);
        }

        monitor_->monitor_ns();
        SERVER_MAP_ITER iter = layout_.server_map_.begin();
        for( ; iter != layout_.server_map_.end(); iter++)
        {
            EXPECT_EQ(0, iter->second->blocks_hold_.size());
            EXPECT_EQ(30, iter->second->blocks_to_remove_.size());
        }

        EXPECT_EQ(0, layout_.blocks_map_.size());
    }
    
    {
        LayoutInit();
        SPInsertBlock(1, 90, layout_);
        SPInsertBlock(5, 180, layout_);
    
    monitor_->monitor_ns();
        for(int i = 90; i < 360; i++)
        {
            layout_.remove_block_collect(i);
        }

    monitor_->monitor_ns();
        SERVER_MAP_ITER iter = layout_.server_map_.begin();
        for( ; iter != layout_.server_map_.end(); iter++)
        {
            EXPECT_EQ(0, iter->second->blocks_hold_.size());
            //EXPECT_EQ(30, iter->second->blocks_to_remove_.size());
        }

        EXPECT_EQ(0, layout_.blocks_map_.size());
    }

}

TEST_F(LayoutManagerTest, CheckDSTest)
{
    LayoutInit();
    {
        std::set<uint64_t>  ds_dead_list;
        layout_.check_ds(0, ds_dead_list);
        EXPECT_EQ(9, ds_dead_list.size());
        ds_dead_list.clear();

        SERVER_MAP_ITER iter = layout_.server_map_.begin();
        for( ; iter != layout_.server_map_.end(); iter++)
        {
            iter->second->set_is_alive(false);
        }
        EXPECT_EQ(0, layout_.check_ds(0, ds_dead_list));

        iter = layout_.server_map_.begin();
        for( ; iter != layout_.server_map_.end(); iter++)
        {
            iter->second->last_update_time_ = TimeUtil::GetTime();
        }
        EXPECT_EQ(0, layout_.check_ds(1000, ds_dead_list));

    }
}

TEST_F(LayoutManagerTest, JoinDSTest)
{
    {   
        LayoutInit();
        bool is_new;
        DataServerInfo * p = new DataServerInfo(4, 10);
        p->set_is_alive(true);
        EXPECT_EQ(BLADE_SUCCESS, layout_.join_ds(p, is_new));
        EXPECT_TRUE(is_new);
        EXPECT_EQ(10, layout_.server_map_.size());
        EXPECT_EQ(4, layout_.racks_map_.size());
        ClearLayout(layout_);
    }

    {
        LayoutInit();
        bool is_new;
        SERVER_MAP_ITER iter = layout_.server_map_.begin();
        for( ; iter != layout_.server_map_.end(); iter++)
        {
            iter->second->set_is_alive(false);
        }

        DataServerInfo * p = new DataServerInfo(2, 1);
        p->set_is_alive(true);
        EXPECT_EQ(BLADE_SUCCESS, layout_.join_ds(p, is_new));
        EXPECT_TRUE(is_new);
        EXPECT_EQ(9, layout_.server_map_.size());
        EXPECT_EQ(3, layout_.racks_map_.size());

        RACKS_MAP_ITER riter = layout_.racks_map_.find(1);
        EXPECT_EQ(3, riter->second.size());
        riter = layout_.racks_map_.find(2);
        EXPECT_EQ(4, riter->second.size());

        monitor_->monitor_ns();
        ClearLayout(layout_);
    }
}


TEST_F(LayoutManagerTest, BuildBlockDSRelationTest)
{
    EXPECT_FALSE(layout_.build_block_ds_relation(1, 1));

    {
        LayoutInit();
        EXPECT_TRUE(layout_.build_block_ds_relation(1, 1));
        EXPECT_TRUE(layout_.build_block_ds_relation(1, 1));
        BLOCKS_MAP_ITER biter = layout_.blocks_map_.find(1);
        EXPECT_TRUE(biter != layout_.blocks_map_.end());
        EXPECT_EQ(1, biter->second->dataserver_set_.size());
        
        SERVER_MAP_ITER diter = layout_.server_map_.find(1);
        EXPECT_TRUE(diter != layout_.server_map_.end());
        EXPECT_EQ(1, diter->second->blocks_hold_.size());

        EXPECT_TRUE(layout_.build_block_ds_relation(1, 2));
        EXPECT_TRUE(layout_.build_block_ds_relation(2, 1));
        biter = layout_.blocks_map_.find(1);
        EXPECT_TRUE(biter != layout_.blocks_map_.end());
        EXPECT_EQ(2, biter->second->dataserver_set_.size());
        
        diter = layout_.server_map_.find(1);
        EXPECT_TRUE(diter != layout_.server_map_.end());
        EXPECT_EQ(2, diter->second->blocks_hold_.size());

        diter->second->set_is_alive(false);
        EXPECT_FALSE(layout_.build_block_ds_relation(1, 1));
        EXPECT_FALSE(layout_.build_block_ds_relation(2, 1));

    }
}

TEST_F(LayoutManagerTest, RemoveBlockDSRelationTest)
{
    EXPECT_FALSE(layout_.remove_block_ds_relation(1, 1));

    {
        LayoutInit();
        InsertBlock(3, 9, layout_);
        EXPECT_FALSE(layout_.remove_block_ds_relation(1, 10));

        EXPECT_TRUE(layout_.remove_block_ds_relation(1, 1));
        EXPECT_TRUE(layout_.remove_block_ds_relation(1, 2));
        EXPECT_TRUE(layout_.remove_block_ds_relation(1, 3));

        BLOCKS_MAP_ITER biter = layout_.blocks_map_.find(1);
        EXPECT_TRUE(biter != layout_.blocks_map_.end());
        EXPECT_EQ(0, biter->second->dataserver_set_.size());

        SERVER_MAP_ITER diter = layout_.server_map_.find(1);
        EXPECT_TRUE(diter != layout_.server_map_.end());
        EXPECT_EQ(2, diter->second->blocks_hold_.size());
        EXPECT_EQ(1, diter->second->blocks_to_remove_.size());

        diter = layout_.server_map_.find(2);
        EXPECT_TRUE(diter != layout_.server_map_.end());
        EXPECT_EQ(2, diter->second->blocks_hold_.size());
        EXPECT_EQ(1, diter->second->blocks_to_remove_.size());

        diter = layout_.server_map_.find(3);
        EXPECT_TRUE(diter != layout_.server_map_.end());
        EXPECT_EQ(2, diter->second->blocks_hold_.size());
        EXPECT_EQ(1, diter->second->blocks_to_remove_.size());

    }
}


TEST_F(LayoutManagerTest, ReleaseDSRelationTest)
{
    EXPECT_FALSE(layout_.release_ds_relation(1));

    {
        LayoutInit();
        InsertBlock(3, 9, layout_);
        SERVER_MAP_ITER diter = layout_.server_map_.find(1);
        EXPECT_TRUE(diter != layout_.server_map_.end());
        diter->second->blocks_hold_.insert(11);
        diter->second->blocks_hold_.insert(12);
        diter->second->blocks_hold_.insert(13);

        monitor_->monitor_ns();
        EXPECT_TRUE(layout_.release_ds_relation(1));
        EXPECT_EQ(0, diter->second->blocks_hold_.size());

        monitor_->monitor_ns();
        EXPECT_EQ(0, diter->second->blocks_to_remove_.size());
        
        //EXPECT_EQ(3, layout_.block_to_check_.size());
        
        BLOCKS_MAP_ITER biter = layout_.blocks_map_.find(1);
        EXPECT_TRUE(biter != layout_.blocks_map_.end());
        EXPECT_EQ(2, biter->second->dataserver_set_.size());
        biter = layout_.blocks_map_.find(8);
        EXPECT_TRUE(biter != layout_.blocks_map_.end());
        EXPECT_EQ(2, biter->second->dataserver_set_.size());
        biter = layout_.blocks_map_.find(9);
        EXPECT_TRUE(biter != layout_.blocks_map_.end());
        EXPECT_EQ(2, biter->second->dataserver_set_.size());
    }
}







}
}
