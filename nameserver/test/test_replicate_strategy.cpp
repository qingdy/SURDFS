/*
 * version : 1.0
 * author  : yeweimin
 * date    : 2012-6-18
 */

#include <gtest/gtest.h>
#include "blade_dataserver_info.h"
#include "replicate_strategy.h"
#include "blade_net_util.h"
#include "register_packet.h"
#include "blade_packet_factory.h"

using namespace bladestore::nameserver;
using namespace bladestore::common;

namespace test
{
namespace nameserver
{
class ServerMatch                                                                                                                            
{
public:
    ServerMatch(uint64_t ds_id)
    {   
        ds_id_ = ds_id;
    }   

    bool operator() (DataServerInfo * dataserver_info)
    {   
        return dataserver_info->dataserver_id_ == ds_id_;
    }   

    uint64_t ds_id_;
};

class TestReplicateStrategy : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
        //layout_manager_();
        replica_strategy = new ReplicateStrategy(layout_manager_);
    }

    virtual void TearDown()
    {
        if(replica_strategy)
            delete replica_strategy;
        replica_strategy = NULL;
    }

    int32_t leave_ds(uint64_t ds_id)
    {
        bool code = layout_manager_.remove_dataserver_info(ds_id); 
        if(!code)
            return BLADE_ERROR;
        DataServerInfo * dataserver_info = NULL; 
        layout_manager_.server_mutex_.rlock()->lock(); 
        SERVER_MAP_ITER server_iter = layout_manager_.server_map_.find(ds_id);
        if(layout_manager_.server_map_.end() == server_iter) {
            layout_manager_.server_mutex_.rlock()->unlock();
            return BLADE_ERROR;
        }
        dataserver_info = server_iter->second;
        layout_manager_.server_mutex_.rlock()->unlock(); 

        layout_manager_.racks_mutex_.wlock()->lock();
        RACKS_MAP_ITER rack_iter =  layout_manager_.racks_map_.find(dataserver_info->rack_id_);
        if(layout_manager_.racks_map_.end() ==  rack_iter) {
            layout_manager_.racks_mutex_.wlock()->lock();
            return BLADE_ERROR;
        }
        std::set<DataServerInfo *>::iterator ds_iter = find_if((rack_iter->second).begin(), (rack_iter->second).end(), ServerMatch(ds_id));
        if(ds_iter != (rack_iter->second).end()) {
            (rack_iter->second).erase(ds_iter);
        }
        layout_manager_.racks_mutex_.wlock()->unlock(); 

        return BLADE_SUCCESS;
    }

    int32_t register_ds(int32_t rack_id, uint64_t ds_id)
    {
        DataServerInfo *ds_info = new DataServerInfo(rack_id, ds_id);
        ds_info->reset();
        ds_info->set_is_alive(true);
        ds_info->last_update_time_ = TimeUtil::GetTime();
        ds_info->dataserver_metrics_.total_space_ = 64000000000;
        ds_info->dataserver_metrics_.used_space_ = 0;
        ds_info->dataserver_metrics_.num_connection_ = 20;
        ds_info->dataserver_metrics_.cpu_load_ = 2;

        layout_manager_.server_mutex_.wlock()->lock();
        layout_manager_.get_server_map().insert(SERVER_MAP::value_type(ds_id, ds_info));
        layout_manager_.server_mutex_.wlock()->unlock();
    
        layout_manager_.racks_mutex_.wlock()->lock();
        RACKS_MAP_ITER rack_iter = layout_manager_.get_racks_map().find(rack_id);
        if(rack_iter == layout_manager_.get_racks_map().end())
        {
            set<DataServerInfo *> temp;
            temp.insert(ds_info);
            layout_manager_.get_racks_map().insert(RACKS_MAP::value_type(rack_id, temp));
        }
        else 
        {
            (rack_iter->second).insert(ds_info);
        }
        layout_manager_.racks_mutex_.wlock()->unlock();
    }
    ReplicateStrategy *replica_strategy;
    BladeLayoutManager layout_manager_;
};

TEST_F(TestReplicateStrategy, testGetPipeline)
{
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.1"));
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.2"));
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.3"));
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.4"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.5"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.6"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.7"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.8"));
    register_ds(3, BladeNetUtil::string_to_addr("10.10.72.9"));
    register_ds(3, BladeNetUtil::string_to_addr("10.10.72.10"));
    register_ds(3, BladeNetUtil::string_to_addr("10.10.72.11"));
    register_ds(3, BladeNetUtil::string_to_addr("10.10.72.12"));

    vector<uint64_t> result1;
    result1.push_back(BladeNetUtil::string_to_addr("10.10.72.1")); //rack 1
    result1.push_back(BladeNetUtil::string_to_addr("10.10.72.10")); //rack 3
    result1.push_back(BladeNetUtil::string_to_addr("10.10.72.3")); //rack 1
    replica_strategy->get_pipeline("10.10.72.3", result1);
    uint64_t expect_ds = BladeNetUtil::string_to_addr("10.10.72.3");
    EXPECT_EQ(expect_ds, result1[0]);
    layout_manager_.server_mutex_.rlock()->lock();
    int32_t rack_id_1 = layout_manager_.get_dataserver_info(result1[0])->rack_id_;
    int32_t rack_id_2 = layout_manager_.get_dataserver_info(result1[1])->rack_id_;
    layout_manager_.server_mutex_.rlock()->unlock();
    EXPECT_TRUE(rack_id_1 == rack_id_2);

    vector<uint64_t> result2;
    result2.push_back(BladeNetUtil::string_to_addr("10.10.72.5")); //rack 2
    result2.push_back(BladeNetUtil::string_to_addr("10.10.72.11")); //rack 3
    result2.push_back(BladeNetUtil::string_to_addr("10.10.72.6")); //rack 2
    replica_strategy->get_pipeline("10.10.72.11", result2);
    uint64_t expect_ds1 = BladeNetUtil::string_to_addr("10.10.72.11");
    EXPECT_EQ(expect_ds1, result2[0]);

    replica_strategy->get_pipeline("11.11.1.1", result2);
    layout_manager_.server_mutex_.rlock()->lock();
    int32_t rack_id_3 = layout_manager_.get_dataserver_info(result2[1])->rack_id_;
    int32_t rack_id_4 = layout_manager_.get_dataserver_info(result2[2])->rack_id_;
    layout_manager_.server_mutex_.rlock()->unlock();
    //EXPECT_TRUE(rack_id_3 == rack_id_4);
    printf("\nip : %s\n", BladeNetUtil::addr_to_string(layout_manager_.get_dataserver_info(result2[0])->dataserver_id_).c_str());

//    replica_strategy->get_pipeline("12.23.22.1", result1);
//    layout_manager_.server_mutex_.rlock()->lock();
//    int32_t rack_id_5 = layout_manager_.get_dataserver_info(result1[0])->rack_id_;
//    int32_t rack_id_6 = layout_manager_.get_dataserver_info(result1[1])->rack_id_;
//    layout_manager_.server_mutex_.rlock()->unlock();
//    //EXPECT_TRUE(rack_id_5 == rack_id_6);
//    printf("\nip : %s\n", BladeNetUtil::addr_to_string(layout_manager_.get_dataserver_info(result1[0])->dataserver_id_).c_str());
//
//    replica_strategy->get_pipeline("13.23.22.1", result1);
//    layout_manager_.server_mutex_.rlock()->lock();
//    int32_t rack_id_7 = layout_manager_.get_dataserver_info(result1[0])->rack_id_;
//    int32_t rack_id_8 = layout_manager_.get_dataserver_info(result1[1])->rack_id_;
//    layout_manager_.server_mutex_.rlock()->unlock();
//    EXPECT_TRUE(rack_id_7 == rack_id_8);
    vector<uint64_t> result3;
    result3.push_back(BladeNetUtil::string_to_addr("10.10.72.5")); //rack 2
    result3.push_back(BladeNetUtil::string_to_addr("10.10.72.11")); //rack 3
    result3.push_back(BladeNetUtil::string_to_addr("10.10.72.6")); //rack 2
    replica_strategy->get_pipeline("11.11.1.1", result3);
    printf("\nip : %s\n", BladeNetUtil::addr_to_string(layout_manager_.get_dataserver_info(result3[0])->dataserver_id_).c_str());

    result3.clear();
    result3.push_back(BladeNetUtil::string_to_addr("10.10.72.5")); //rack 2
    result3.push_back(BladeNetUtil::string_to_addr("10.10.72.11")); //rack 3
    result3.push_back(BladeNetUtil::string_to_addr("10.10.72.6")); //rack 2
    replica_strategy->get_pipeline("11.11.1.1", result3);
    printf("\nip : %s\n", BladeNetUtil::addr_to_string(layout_manager_.get_dataserver_info(result3[0])->dataserver_id_).c_str());

    result3.clear();
    result3.push_back(BladeNetUtil::string_to_addr("10.10.72.5")); //rack 2
    result3.push_back(BladeNetUtil::string_to_addr("10.10.72.11")); //rack 3
    result3.push_back(BladeNetUtil::string_to_addr("10.10.72.6")); //rack 2
    replica_strategy->get_pipeline("11.11.1.1", result3);
    printf("\nip : %s\n", BladeNetUtil::addr_to_string(layout_manager_.get_dataserver_info(result3[0])->dataserver_id_).c_str());

    result3.clear();
    result3.push_back(BladeNetUtil::string_to_addr("10.10.72.5")); //rack 2
    result3.push_back(BladeNetUtil::string_to_addr("10.10.72.11")); //rack 3
    result3.push_back(BladeNetUtil::string_to_addr("10.10.72.6")); //rack 2
    replica_strategy->get_pipeline("11.11.1.1", result3);
    printf("\nip : %s\n", BladeNetUtil::addr_to_string(layout_manager_.get_dataserver_info(result3[0])->dataserver_id_).c_str());
    printf("\nip : %s\n", BladeNetUtil::addr_to_string(layout_manager_.get_dataserver_info(result3[1])->dataserver_id_).c_str());
}

TEST_F(TestReplicateStrategy, testChooseTarget)
{
    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    RegisterPacket * packet = new RegisterPacket(1, BladeNetUtil::string_to_addr("10.10.72.1:8000"));
    packet->pack();
    BladePacket *blade_packet = packet_factory_->create_packet(OP_REGISTER);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    BladePacket *reply_packet = NULL;
    int32_t return_code = layout_manager_.register_ds(blade_packet, reply_packet);
    ASSERT_EQ(BLADE_SUCCESS, return_code);
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
    DataServerMetrics ds_metrics = {64000000000, 0, 20, 2};
    layout_manager_.get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.1:8000"))->update_heartbeat(ds_metrics);
    
    packet = new RegisterPacket(1, BladeNetUtil::string_to_addr("10.10.72.1:8001"));
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_REGISTER);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    return_code = layout_manager_.register_ds(blade_packet, reply_packet);
    ASSERT_EQ(BLADE_SUCCESS, return_code);
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
    ds_metrics.used_space_ = 100000;
    layout_manager_.get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.1:8001"))->update_heartbeat(ds_metrics);

    vector<uint64_t> result;
    replica_strategy->choose_target(3, "10.10.72.1", result);
    EXPECT_EQ(2, result.size());

    packet = new RegisterPacket(2, BladeNetUtil::string_to_addr("10.10.72.2:9000"));
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_REGISTER);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    return_code = layout_manager_.register_ds(blade_packet, reply_packet);
    ASSERT_EQ(BLADE_SUCCESS, return_code);
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
    layout_manager_.get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.2:9000"))->update_heartbeat(ds_metrics);

    result.clear();
    replica_strategy->choose_target(3, "10.10.72.2", result);
    EXPECT_EQ(3, result.size());

    packet = new RegisterPacket(1, BladeNetUtil::string_to_addr("10.10.72.1:9009"));
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_REGISTER);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    return_code = layout_manager_.register_ds(blade_packet, reply_packet);
    ASSERT_EQ(BLADE_SUCCESS, return_code);
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
    ds_metrics.used_space_ = 100011;
    layout_manager_.get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.1:9009"))->update_heartbeat(ds_metrics);

    result.clear();
    replica_strategy->choose_target(3, "10.10.72.2", result);
    EXPECT_EQ(3, result.size());
    int32_t rack_id1 = layout_manager_.get_dataserver_info(result[1])->rack_id_;
    int32_t rack_id2 = layout_manager_.get_dataserver_info(result[2])->rack_id_;
    EXPECT_TRUE(rack_id1 == rack_id2);
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.1:9009"), result[1]);
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.1:9009"), result[2]);

    return_code = leave_ds(BladeNetUtil::string_to_addr("10.10.72.2:9000"));
    ASSERT_EQ(BLADE_SUCCESS, return_code);
    ASSERT_EQ(3, layout_manager_.get_alive_ds_size());
    result.clear();
    replica_strategy->choose_target(3, "10.10.72.2", result);
    EXPECT_EQ(3, result.size());
    EXPECT_EQ(BladeNetUtil::string_to_addr("10.10.72.1:8000"), result[0]);

    return_code = leave_ds(BladeNetUtil::string_to_addr("10.10.72.1:9009"));
    ASSERT_EQ(BLADE_SUCCESS, return_code);
    ASSERT_EQ(2, layout_manager_.get_alive_ds_size());
    result.clear();
    replica_strategy->choose_target(3, "10.10.72.2", result);
    EXPECT_EQ(2, result.size());

    return_code = leave_ds(BladeNetUtil::string_to_addr("10.10.72.1:8001"));
    ASSERT_EQ(BLADE_SUCCESS, return_code);
    packet = new RegisterPacket(3, BladeNetUtil::string_to_addr("10.10.72.3:9009"));
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_REGISTER);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    return_code = layout_manager_.register_ds(blade_packet, reply_packet);
    ASSERT_EQ(BLADE_SUCCESS, return_code);
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
    ds_metrics.used_space_ = 100011;
    layout_manager_.get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.3:9009"))->update_heartbeat(ds_metrics);

    result.clear();
    replica_strategy->choose_target(3, "10.10.72.2", result);
    EXPECT_EQ(2, result.size());

    packet = new RegisterPacket(4, BladeNetUtil::string_to_addr("10.10.72.4:9009"));
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_REGISTER);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    return_code = layout_manager_.register_ds(blade_packet, reply_packet);
    ASSERT_EQ(BLADE_SUCCESS, return_code);
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
    ds_metrics.used_space_ = 100011;
    layout_manager_.get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.4:9009"))->update_heartbeat(ds_metrics);
    result.clear();
    replica_strategy->choose_target(3, "10.10.72.2", result);
    EXPECT_EQ(3, result.size());

    result.clear();
    replica_strategy->choose_target(3, "10.10.72.4", result);
    EXPECT_EQ(3, result.size());
    EXPECT_EQ(BladeNetUtil::string_to_addr("10.10.72.4:9009"), result[0]);

    ds_metrics.used_space_ = 60000000000;
    layout_manager_.get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.4:9009"))->update_heartbeat(ds_metrics);
    result.clear();
    replica_strategy->choose_target(3, "10.10.72.4", result);
    ASSERT_EQ(2, result.size());
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.4:9009"), result[0]);
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.4:9009"), result[1]);

    packet = new RegisterPacket(5, BladeNetUtil::string_to_addr("10.10.72.5:9009"));
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_REGISTER);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    return_code = layout_manager_.register_ds(blade_packet, reply_packet);
    ASSERT_EQ(BLADE_SUCCESS, return_code);
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
    ds_metrics.used_space_ = 100011;
    layout_manager_.get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.5:9009"))->update_heartbeat(ds_metrics);
    result.clear();
    replica_strategy->choose_target(3, "10.10.72.4", result);
    ASSERT_EQ(3, result.size());
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.4:9009"), result[0]);
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.4:9009"), result[1]);
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.4:9009"), result[2]);
}



TEST_F(TestReplicateStrategy, testChooseTarget2)
{
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.1"));
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.2"));
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.3"));
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.4"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.5"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.6"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.7"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.8"));
    register_ds(3, BladeNetUtil::string_to_addr("10.10.72.9"));
    register_ds(3, BladeNetUtil::string_to_addr("10.10.72.10"));
    register_ds(3, BladeNetUtil::string_to_addr("10.10.72.11"));
    register_ds(3, BladeNetUtil::string_to_addr("10.10.72.12"));

    vector<uint64_t> result, choosen_ds;
    choosen_ds.push_back(BladeNetUtil::string_to_addr("10.10.72.1"));
    replica_strategy->choose_target(2, BladeNetUtil::string_to_addr("10.10.72.1"), choosen_ds, result);
    EXPECT_EQ(2, result.size());
    int32_t rack_id_1 = layout_manager_.get_dataserver_info(result[0])->rack_id_;
    EXPECT_EQ(1, rack_id_1);
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.1"), result[0]);
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.1"), result[1]);

    result.clear();
    choosen_ds.clear();
    choosen_ds.push_back(BladeNetUtil::string_to_addr("10.10.72.2"));
    replica_strategy->choose_target(4, BladeNetUtil::string_to_addr("10.10.72.2"), choosen_ds, result);
    EXPECT_EQ(4, result.size());
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.2"), result[0]);
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.2"), result[1]);
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.2"), result[2]);
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.2"), result[3]);

    result.clear();
    choosen_ds.clear();
    choosen_ds.push_back(BladeNetUtil::string_to_addr("10.10.72.1"));
    choosen_ds.push_back(BladeNetUtil::string_to_addr("10.10.72.2"));
    replica_strategy->choose_target(1, BladeNetUtil::string_to_addr("10.10.72.2"), choosen_ds, result);
    EXPECT_EQ(1, result.size());
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.1"), result[0]);
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.2"), result[0]);
    rack_id_1 = layout_manager_.get_dataserver_info(result[0])->rack_id_;
    EXPECT_NE(1, rack_id_1);

    result.clear();
    choosen_ds.clear();
    choosen_ds.push_back(BladeNetUtil::string_to_addr("10.10.72.2"));
    choosen_ds.push_back(BladeNetUtil::string_to_addr("10.10.72.12"));
    replica_strategy->choose_target(1, BladeNetUtil::string_to_addr("10.10.72.12"), choosen_ds, result);
    EXPECT_EQ(1, result.size());
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.2"), result[0]);
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.12"), result[0]);
    rack_id_1 = layout_manager_.get_dataserver_info(result[0])->rack_id_;
    EXPECT_EQ(3, rack_id_1);
}

TEST_F(TestReplicateStrategy, testChooseTarget3)
{
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.1"));
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.2"));
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.3"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.4"));

    layout_manager_.get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.3"))->dataserver_metrics_.used_space_ = 999999;
    vector<uint64_t> result;
    replica_strategy->choose_target(3, "10.10.72.10", result);
    EXPECT_EQ(3, result.size());
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.3"), result[0]);
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.3"), result[1]);
    EXPECT_NE(BladeNetUtil::string_to_addr("10.10.72.3"), result[2]);
}

TEST_F(TestReplicateStrategy, testChooseTarget4)
{
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.1"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.2"));
    register_ds(3, BladeNetUtil::string_to_addr("10.10.72.3"));
    layout_manager_.get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.3"))->dataserver_metrics_.used_space_ = 63999999990;
    vector<uint64_t> result;
    for(int i = 0; i <500; i++) {
        replica_strategy->choose_target(3, "10.10.72.10", result);
        EXPECT_EQ(2, result.size());
        result.clear();
    }
}

TEST_F(TestReplicateStrategy, testChooseTarget5)
{
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.1"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.2"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.3"));
    layout_manager_.get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.3"))->dataserver_metrics_.used_space_ = 63999999990;
    layout_manager_.get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.2"))->dataserver_metrics_.used_space_ = 63999999990;
    vector<uint64_t> result;
    for(int i = 0; i < 50; i++) {
        replica_strategy->choose_target(3, "10.10.72.1", result);
        EXPECT_EQ(1, result.size());
        result.clear();
    }
}

TEST_F(TestReplicateStrategy, testChooseTarget6)
{
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.1"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.2"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.3"));
    layout_manager_.get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.3"))->dataserver_metrics_.used_space_ = 63999999990;
    vector<uint64_t> result; 
    replica_strategy->choose_target(3, "10.10.72.1", result);
    EXPECT_EQ(2, result.size());
}

}
}
