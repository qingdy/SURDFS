/*
 * funing
 */
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "gtest/gtest.h"

#include "bad_block_report_packet.h"
#include "block_received_packet.h"
#include "block_report_packet.h"
#include "heartbeat_packet.h"


namespace bladestore
{

namespace message
{
using namespace bladestore::message;
using namespace bladestore::common;
class MessageTest : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
    BadBlockReportPacket * bad_block_;
    BlockReceivedPacket * block_received_;
    BlockReportPacket * block_report_;
    HeartbeatReplyPacket * heart_reply_;
};

TEST_F(MessageTest, ConstructTest)
{
    //bad_block_report_packet
    {
        bad_block_ = new BadBlockReportPacket();
        ASSERT_TRUE(bad_block_ != NULL);
        EXPECT_EQ(0, bad_block_->ds_id_);
        EXPECT_EQ(0, bad_block_->bad_blocks_id_.size());
        delete bad_block_;
    
        set<int64_t> b_set; 
        for(int i = 0; i < 10; i++)
        { 
            int64_t b = i;
            b_set.insert(b);
        }
        bad_block_ = new BadBlockReportPacket(1, b_set);
        EXPECT_EQ(10, bad_block_->bad_blocks_id_.size());  
        b_set.clear();
        delete bad_block_; 
    }

    //block_received_packet
    {
        block_received_ = new BlockReceivedPacket();
        ASSERT_TRUE(block_received_ != NULL);
        EXPECT_EQ(0, block_received_->ds_id_);
        EXPECT_EQ(0, block_received_->received_blocks_info_.size());
        delete block_received_;
    
        set<BlockInfo* > b_set; 
        for(int i = 0; i < 10; i++)
        { 
            BlockInfo * b = new BlockInfo();
            b_set.insert(b);
        }
        block_received_ = new BlockReceivedPacket(1, b_set);
        EXPECT_EQ(10, block_received_->received_blocks_info_.size());  
        b_set.clear();
        delete block_received_; 
    }
    
    
    //block_report_packet
    {
        block_report_ = new BlockReportPacket();
        ASSERT_TRUE(block_report_ != NULL);
        EXPECT_EQ(0, block_report_->ds_id_);
        EXPECT_EQ(0, block_report_->report_blocks_info_.size());
        delete block_report_;
    
        set<BlockInfo* > b_set; 
        for(int i = 0; i < 10; i++)
        { 
            BlockInfo * b = new BlockInfo();
            b_set.insert(b);
        }
        block_report_ = new BlockReportPacket(1, b_set);
        EXPECT_EQ(10, block_report_->report_blocks_info_.size());  
        b_set.clear();
        delete block_report_; 
    }

    //heartbeat_reply_packet
    {
        heart_reply_ = new HeartbeatReplyPacket();
        ASSERT_TRUE(heart_reply_ != NULL);
        EXPECT_EQ(0, heart_reply_->ret_code_);
        EXPECT_EQ(0, heart_reply_->op_group_);
        EXPECT_EQ(0, heart_reply_->blocks_to_remove_.size());
        EXPECT_EQ(0, heart_reply_->blocks_to_replicate_.size());
        EXPECT_EQ(0, heart_reply_->blocks_to_get_length_.size());
        delete heart_reply_;

        heart_reply_ = new HeartbeatReplyPacket(BLADE_SUCCESS);
        heart_reply_->op_group_ = HeartbeatReplyPacket::OP_REGISTER_;
        ASSERT_TRUE(heart_reply_ != NULL);
        EXPECT_EQ(0, heart_reply_->ret_code_);
        EXPECT_EQ(1, heart_reply_->op_group_);
        EXPECT_EQ(0, heart_reply_->blocks_to_remove_.size());
        EXPECT_EQ(0, heart_reply_->blocks_to_replicate_.size());
        EXPECT_EQ(0, heart_reply_->blocks_to_get_length_.size());
                
    }
}

TEST_F(MessageTest, PackUnpackTest)
{
    //bad_block_report_packet
    {
        set<int64_t> b_set; 
        for(int i = 0; i < 10; i++)
        { 
            int64_t b = i;
            b_set.insert(b);
        }
        bad_block_ = new BadBlockReportPacket(1, b_set);
        bad_block_->pack();
        bad_block_->get_net_data()->set_read_data(bad_block_->content(), bad_block_->length());
        bad_block_->bad_blocks_id_.clear();
        bad_block_->unpack();
        EXPECT_EQ(10, bad_block_->bad_blocks_id_.size());  
        b_set.clear();
        delete bad_block_; 
    }

    //block_received_packet
    {
        set<BlockInfo* > b_set; 
        for(int i = 0; i < 10; i++)
        { 
            BlockInfo * b = new BlockInfo();
            b_set.insert(b);
        }
        block_received_ = new BlockReceivedPacket(1, b_set);
        block_received_->pack();
        block_received_->get_net_data()->set_read_data(block_received_->content(), block_received_->length());
        block_received_->received_blocks_info_.clear();
        block_received_->unpack();
        EXPECT_EQ(10, block_received_->received_blocks_info_.size());  
        b_set.clear();
        delete block_received_; 
    }

    //block_report_packet
    {
        set<BlockInfo* > b_set; 
        for(int i = 0; i < 10; i++)
        { 
            BlockInfo * b = new BlockInfo();
            b_set.insert(b);
        }   
        block_report_ = new BlockReportPacket(1, b_set);
        block_report_->pack();
        block_report_->get_net_data()->set_read_data(block_report_->content(), block_report_->length());
        block_report_->report_blocks_info_.clear();
        block_report_->unpack();
        EXPECT_EQ(10, block_report_->report_blocks_info_.size());  
        b_set.clear();
        delete block_report_; 
    }
    
    //heartbeat_reply_packet test1
    {
        heart_reply_ = new HeartbeatReplyPacket(); 
        int64_t blockid1 = 100;
        int64_t blockid2 = 200;                                                                                                                                                   int64_t blockid3 = 300;
        
        heart_reply_->blocks_to_remove_.insert(blockid1);
        heart_reply_->blocks_to_remove_.insert(blockid2);
        heart_reply_->blocks_to_remove_.insert(blockid3);

        uint64_t dsid1 = 111;
        uint64_t dsid2 = 222;
        uint64_t dsid3 = 333;
        set<uint64_t> tempDS ; 
        heart_reply_->blocks_to_replicate_ .insert(make_pair(blockid1, tempDS)); 
        tempDS.insert(dsid1);
        tempDS.insert(dsid2); 
        heart_reply_->blocks_to_replicate_ .insert(make_pair(blockid2, tempDS)); 
        tempDS.insert(dsid3);
        heart_reply_->blocks_to_replicate_ .insert(make_pair(blockid3, tempDS)); 
        heart_reply_->blocks_to_get_length_.insert(blockid1);

        heart_reply_->pack();
        heart_reply_->get_net_data()->set_read_data(heart_reply_->content(), heart_reply_->length());
        heart_reply_->clear_all();
        heart_reply_->unpack();
        EXPECT_EQ(3, heart_reply_->blocks_to_remove_.size());
        EXPECT_EQ(3, heart_reply_->blocks_to_replicate_.size());
        EXPECT_EQ(1, heart_reply_->blocks_to_get_length_.size());
        delete heart_reply_;
    }
    //heartbeat_reply_packet test2
    {
        heart_reply_ = new HeartbeatReplyPacket(); 
        set<int64_t> b;
        map<int64_t, set<uint64_t> > map;

        heart_reply_->blocks_to_remove_ = b;
        heart_reply_->blocks_to_replicate_ = map;
        heart_reply_->blocks_to_get_length_ = b;        


        heart_reply_->pack();
        heart_reply_->get_net_data()->set_read_data(heart_reply_->content(), heart_reply_->length());
        heart_reply_->clear_all();
        heart_reply_->unpack();
        EXPECT_EQ(0, heart_reply_->blocks_to_remove_.size());
        EXPECT_EQ(0, heart_reply_->blocks_to_replicate_.size());
        EXPECT_EQ(0, heart_reply_->blocks_to_get_length_.size());
        delete heart_reply_;
    }
    
}






/*
TEST_F(ClientTest, GetListing)
{
    ASSERT_TRUE(client != NULL);
    vector<string> file_names;
    file_names.push_back("/");
    EXPECT_EQ(file_names, client->get_listing("/"));
    client->mkdir("/mytest");
    client->mkdir("/mytest/test1");
    client->creat("/mytest/test2", 0, 3);
    file_names.push_back("mytest");
    EXPECT_EQ(file_names, client->get_listing("/"));
    file_names.clear();
    EXPECT_EQ(file_names, client->get_listing("/mytest/test1"));
    EXPECT_EQ(file_names, client->get_listing("/mytest/test2"));
    EXPECT_EQ(file_names, client->get_listing("/invalidpath"));
    file_names.push_back("test1");
    file_names.push_back("test2");
    EXPECT_EQ(file_names, client->get_listing("/mytest"));

}
*/

}
}
