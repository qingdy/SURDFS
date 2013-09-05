/*
 * version : 1.0
 * author  : carloswjx
 * date    : 2012-6-26
 */

#include <gtest/gtest.h>
#include <fcntl.h> 
#include "singleton.h"
#include "time_util.h"
#include "utility.h"
#include "dataserver_net_handler.h"
#include "dataserver_net_server.h"
#include "blade_common_data.h"
#include "dataserver_impl.h"
#include "dataserver_conf.h"
#include "register_packet.h"
#include "heartbeat_packet.h"
#include "block_report_packet.h"
#include "bad_block_report_packet.h"
#include "block_received_packet.h"
#include "dataserver_response.h"
#include "dataserver_heartbeat.h"

namespace bladestore
{
namespace dataserver
{

using namespace pandora;
using namespace bladestore::common;
using namespace bladestore::message;
namespace pandora {
     class Log;
}

class FakeDataServerImpl;

class FakeDSHeartbeatManager : public DSHeartbeatManager
{
public:
    FakeDSHeartbeatManager(FakeDataServerImpl * ds_impl);
    ~FakeDSHeartbeatManager();
    //bool Start();    
    //bool Init();    
    //void Join();    
	//void Run(CThread * thread, void * arg);
    bool    DSRegister(SyncResponse *&,bool,bool,int32_t);
    bool    DSBlockReport(set<BlockInfo *> &,bool,bool,int);
    bool    DSReceivedBlockReport(BlockInfo b,bool);
    int32_t GetCpuLoad(int, int); 
    //int32_t  rack_id() const {return rack_id_;}
    //uint64_t ds_id() const {return ds_id_;}
    //float    cpu_total() const {return cpu_total_;}
    //float    cpu_user() const {return cpu_user_;}
    //int32_t  cpu_usage() const {return cpu_usage_;}
    //int64_t  last_report() const {return last_report_;}
    //void     set_last_report(int64_t last_report) {last_report_ = last_report;}
    FakeDataServerImpl * ds_impl_;
};


class FakeDataServerImpl
{
public:
    FakeDataServerImpl(): ds_h_test_(NULL)
    {
        ds_h_test_ = new FakeDSHeartbeatManager(this);
    }
    ~FakeDataServerImpl()
    {
        if (ds_h_test_) {
            delete ds_h_test_;
            ds_h_test_ = NULL;
        }
    }
    FakeDSHeartbeatManager * ds_h_test_;
};

FakeDSHeartbeatManager::FakeDSHeartbeatManager(FakeDataServerImpl * ds_impl): ds_impl_(ds_impl)
{
}

FakeDSHeartbeatManager::~FakeDSHeartbeatManager()
{
}

bool FakeDSHeartbeatManager::DSRegister(SyncResponse *&sync_register, bool cannotsend, bool connect, int32_t ret)
{
    //ds_impl_->log()->Write(MSG_DEBUG, "Now in register");
    RegisterPacket * p = new RegisterPacket(rack_id_, ds_id_);
    if (!p) {
        //ds_impl_->log()->Write(MSG_ERROR, "new RegisterPacket error.");
        return false;
    }
    //SyncResponse *sync_register = new SyncResponse(p->operation());
    if (!sync_register) {
        //ds_impl_->log()->Write(MSG_ERROR, "new SyncResponse error.");
        delete p;
        return false;
    }
    int32_t ret_pack = p->pack();
    if (BLADE_SUCCESS != ret_pack) {
        delete p;
        //ds_impl_->log()->Write(MSG_ERROR, "registerpacket packet pack error");
        return false;
    }
    
    //uint64_t ns_id = DataServerConfig::Instance()->ns_id();
    if (cannotsend) {
        //ds_impl_->log()->Write(MSG_ERROR, "get_peer_id(endpoint.GetFd()) != ns_id");
        //sleep(DS_RECONNECT_NS_TIME);
        if (!connect) {
        delete p;
        return false;
        }
    }

    if (ret == 0) {
        //ds_impl_->log()->Write(MSG_DEBUG, "Register Send packet OK");   
        //sync_register->cond().Lock();
        //while (sync_register->wait() == true) {
            //sync_register->cond().Wait(DS_REGISTER_WAIT);
        //}
        //sync_register->cond().Unlock();
        //sync_register->set_wait(true);

        if (sync_register->response_ok()) {
            //ds_impl_->log()->Write(MSG_INFO, "register response OK.");
            delete sync_register;
            return true;
        } else {
            delete sync_register;
            //ds_impl_->log()->Write(MSG_ERROR, 
            //"registerpacket packet already send, error in response");
            return false;
        }
    } else if (ret < 0) {
        delete sync_register;
        //ds_impl_->log()->Write(MSG_ERROR, "Register Send packet error");   
        return false;
    }
}

bool FakeDSHeartbeatManager::DSBlockReport(set<BlockInfo *> &report_block,bool cannotsend,  bool connect, int error)
{
    //ds_impl_->log()->Write(MSG_DEBUG, "report blocks num: %d", report_block.size());   
    BlockReportPacket * p = new BlockReportPacket(ds_id_, report_block);
    if (!p) {
        //ds_impl_->log()->Write(MSG_ERROR, "new BlockReportPacket error.");
        return false;
    }
    int32_t ret_pack = p->pack();
    if (BLADE_SUCCESS != ret_pack) {
        delete p;
        //ds_impl_->log()->Write(MSG_ERROR, "BlockReportpacket packet pack error");
        return false;
    }
    
    //uint64_t ns_id = DataServerConfig::Instance()->ns_id();
    if (cannotsend) {
        //ds_impl_->log()->Write(MSG_ERROR, "get_peer_id(endpoint.GetFd()) != ns_id");
        //sleep(DS_RECONNECT_NS_TIME);
        if (!connect) {
            delete p;
            return false;
        }
    }

    //int32_t error = Singleton<AmFrame>::Instance().SendPacket(
    //                ds_impl_->net_server()->stream_handler()->end_point(), p, true);
    if (error < 0 ) {
        //ds_impl_->log()->Write(MSG_ERROR, "BlockReport Send packet error: %d", error);
        return false;
    } else {
        return true;
    }
}

bool FakeDSHeartbeatManager::DSReceivedBlockReport(BlockInfo received, bool error)
{
    BlockReceivedPacket * p = new BlockReceivedPacket(ds_id_, received);
    if (!p) {
        //ds_impl_->log()->Write(MSG_ERROR, "new BlockReceivedPacket error.");
        return false;
    }
    int32_t ret_pack = p->pack();
    if (BLADE_SUCCESS != ret_pack) {
        delete p;
        //ds_impl_->log()->Write(MSG_ERROR, "BlockReceivedpacket pack error");
        return false;
    }
    
    //int32_t error = Singleton<AmFrame>::Instance().SendPacket(
    //                            ds_impl_->net_server()->end_point(), p);  

    if (!error) {
        //ds_impl_->log()->Write(MSG_INFO, "Block Received Send packet ok");
        return true;
    } else {
        //ds_impl_->log()->Write(MSG_ERROR, "Block Received Send packet error");
        return false;
    }
}

int32_t FakeDSHeartbeatManager::GetCpuLoad(int fake1, int fake2)  
{ 
    FILE  * cpu_file;
    size_t  cpu_file_buf_len;
    float   usage = cpu_usage_;  
    float   user = 0, nice = 0, sys = 0, idle = 0, iowait = 0;  
    
    cpu_file = fopen("/proc/stat","r");
    if (fake1 == 0) cpu_file = NULL;
    if (!cpu_file) {
        //ds_impl_->log()->Write(MSG_ERROR, "Open /proc/stat error");
        return -1;
    }
    memset(cpu_file_buffer_, 0, sizeof(cpu_file_buffer_));
    cpu_file_buf_len = fread(cpu_file_buffer_, 1, sizeof(cpu_file_buffer_), cpu_file);
    if (fake2 == 0) cpu_file_buf_len = 0;
    fclose(cpu_file);
    if (cpu_file_buf_len == 0) {
        //ds_impl_->log()->Write(MSG_ERROR, "Read /proc/stat error");
        return -1;
    }
    cpu_file_buffer_[cpu_file_buf_len] = '\0';
    sscanf(cpu_file_buffer_, "cpu %f %f %f %f %f", &user, &nice, &sys, &idle, &iowait);  
    if (idle <= 0)
        idle = 0; 
    if (user - cpu_user_ == 0 || user + nice + sys + idle - cpu_total_ == 0) {
        return cpu_usage_; 
    }
    usage = 100 * (user - cpu_user_) / (user + nice + sys + idle - cpu_total_);  
    if (usage > 100)  
        usage = 100; 
    usage = usage > 0 ? usage : -usage;
    
    cpu_total_ = user + nice + sys + idle;
    cpu_user_ = user;    
    cpu_usage_ = (int32_t)usage;
    //ds_impl_->log()->Write(MSG_DEBUG, "cpu usage = %d.", cpu_usage_);
    
    return cpu_usage_;
} 

class TestHeartBeat : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
        dataserver = new  FakeDataServerImpl();
    }

    virtual void TearDown()
    {
        if (dataserver) {
            delete dataserver;
            dataserver = NULL;
        }
    }
    FakeDataServerImpl *dataserver;
};

TEST_F(TestHeartBeat, testgetcpuload)
{
    EXPECT_GE(dataserver->ds_h_test_->GetCpuLoad(1,1),0.0);
    EXPECT_LE(dataserver->ds_h_test_->GetCpuLoad(1,1),100.0);
    EXPECT_EQ(dataserver->ds_h_test_->GetCpuLoad(0,1),-1);
    EXPECT_EQ(dataserver->ds_h_test_->GetCpuLoad(1,0),-1);
    EXPECT_EQ(dataserver->ds_h_test_->GetCpuLoad(0,0),-1);
}

TEST_F(TestHeartBeat, testreceivedblockreport)
{
    BlockInfo block1 = BlockInfo(5,5);
    EXPECT_TRUE(dataserver->ds_h_test_->DSReceivedBlockReport(block1, false));
    BlockInfo block2 = BlockInfo(-1,5);
    EXPECT_TRUE(dataserver->ds_h_test_->DSReceivedBlockReport(block2, false));
    BlockInfo block3 = BlockInfo(5,-1);
    EXPECT_TRUE(dataserver->ds_h_test_->DSReceivedBlockReport(block3, false));
    BlockInfo block4 = BlockInfo(0,5);
    EXPECT_TRUE(dataserver->ds_h_test_->DSReceivedBlockReport(block4, false));
    BlockInfo block5 = BlockInfo(5,0);
    EXPECT_TRUE(dataserver->ds_h_test_->DSReceivedBlockReport(block5, false));
    BlockInfo block6 = BlockInfo(0,0);
    EXPECT_TRUE(dataserver->ds_h_test_->DSReceivedBlockReport(block6, false));
    BlockInfo block7 = BlockInfo(-1,-1);
    EXPECT_TRUE(dataserver->ds_h_test_->DSReceivedBlockReport(block7, false));
    
    BlockInfo block8 = BlockInfo(5,5);
    EXPECT_FALSE(dataserver->ds_h_test_->DSReceivedBlockReport(block8, true));
    BlockInfo block9 = BlockInfo(-1,5);
    EXPECT_FALSE(dataserver->ds_h_test_->DSReceivedBlockReport(block9, true));
    BlockInfo block10 = BlockInfo(5,-1);
    EXPECT_FALSE(dataserver->ds_h_test_->DSReceivedBlockReport(block10, true));
    BlockInfo block11 = BlockInfo(0,5);
    EXPECT_FALSE(dataserver->ds_h_test_->DSReceivedBlockReport(block11, true));
    BlockInfo block12 = BlockInfo(5,0);
    EXPECT_FALSE(dataserver->ds_h_test_->DSReceivedBlockReport(block12, true));
    BlockInfo block13 = BlockInfo(0,0);
    EXPECT_FALSE(dataserver->ds_h_test_->DSReceivedBlockReport(block13, true));
    BlockInfo block14 = BlockInfo(-1,-1);
    EXPECT_FALSE(dataserver->ds_h_test_->DSReceivedBlockReport(block14, true));

}

TEST_F(TestHeartBeat, testblockreport)
{
    BlockInfo * block;

    set<BlockInfo *> blockset1;
    EXPECT_TRUE(dataserver->ds_h_test_->DSBlockReport(blockset1, false, true, 0));

    set<BlockInfo *> blockset2;
    block = new BlockInfo(3,3);
    blockset2.insert(block);
    EXPECT_TRUE(dataserver->ds_h_test_->DSBlockReport(blockset2, false, true, 0));

    set<BlockInfo *> blockset3;
    block = new BlockInfo(3,3);
    blockset3.insert(block);
    block = new BlockInfo(3,3);
    blockset3.insert(block);
    block = new BlockInfo(3,3);
    blockset3.insert(block);
    EXPECT_TRUE(dataserver->ds_h_test_->DSBlockReport(blockset3, false, true, 0));

    set<BlockInfo *> blockset4;
    EXPECT_FALSE(dataserver->ds_h_test_->DSBlockReport(blockset4, false, true, -1));

    set<BlockInfo *> blockset5;
    block = new BlockInfo(3,3);
    blockset5.insert(block);
    EXPECT_FALSE(dataserver->ds_h_test_->DSBlockReport(blockset5, false, true, -1));

    set<BlockInfo *> blockset6;
    block = new BlockInfo(3,3);
    blockset6.insert(block);
    block = new BlockInfo(3,3);
    blockset6.insert(block);
    block = new BlockInfo(3,3);
    blockset6.insert(block);
    EXPECT_FALSE(dataserver->ds_h_test_->DSBlockReport(blockset6, false, true, -1));

    set<BlockInfo *> blockset7;
    EXPECT_TRUE(dataserver->ds_h_test_->DSBlockReport(blockset7, true, true, 0));

    set<BlockInfo *> blockset8;
    block = new BlockInfo(3,3);
    blockset8.insert(block);
    EXPECT_TRUE(dataserver->ds_h_test_->DSBlockReport(blockset8, true, true, 0));

    set<BlockInfo *> blockset9;
    block = new BlockInfo(3,3);
    blockset9.insert(block);
    block = new BlockInfo(3,3);
    blockset9.insert(block);
    block = new BlockInfo(3,3);
    blockset9.insert(block);
    EXPECT_TRUE(dataserver->ds_h_test_->DSBlockReport(blockset9, true, true, 0));

    set<BlockInfo *> blockset10;
    EXPECT_FALSE(dataserver->ds_h_test_->DSBlockReport(blockset10, true, false, 0));

    set<BlockInfo *> blockset11;
    block = new BlockInfo(3,3);
    blockset11.insert(block);
    EXPECT_FALSE(dataserver->ds_h_test_->DSBlockReport(blockset11, true, false, 0));

    set<BlockInfo *> blockset12;
    block = new BlockInfo(3,3);
    blockset12.insert(block);
    block = new BlockInfo(3,3);
    blockset12.insert(block);
    block = new BlockInfo(3,3);
    blockset12.insert(block);
    EXPECT_FALSE(dataserver->ds_h_test_->DSBlockReport(blockset12, true, false, 0));
}

TEST_F(TestHeartBeat, testregister)
{
    SyncResponse *sync_register = new SyncResponse(OP_REGISTER);
    sync_register->set_response_ok(true);
    EXPECT_TRUE(dataserver->ds_h_test_->DSRegister(sync_register, false, true, 0));

    sync_register = new SyncResponse(OP_REGISTER);
    sync_register->set_response_ok(true);
    EXPECT_FALSE(dataserver->ds_h_test_->DSRegister(sync_register, false, true, -1));
    
    sync_register = new SyncResponse(OP_REGISTER);
    sync_register->set_response_ok(true);
    EXPECT_TRUE(dataserver->ds_h_test_->DSRegister(sync_register, true, true, 0));
    
    sync_register = new SyncResponse(OP_REGISTER);
    sync_register->set_response_ok(true);
    EXPECT_FALSE(dataserver->ds_h_test_->DSRegister(sync_register, true, true, -1));
    
    sync_register = new SyncResponse(OP_REGISTER);
    sync_register->set_response_ok(true);
    EXPECT_FALSE(dataserver->ds_h_test_->DSRegister(sync_register, true, false, 0));
    
    sync_register = new SyncResponse(OP_REGISTER);
    sync_register->set_response_ok(false);
    EXPECT_FALSE(dataserver->ds_h_test_->DSRegister(sync_register, false, true, 0));
    
    sync_register = new SyncResponse(OP_REGISTER);
    sync_register->set_response_ok(false);
    EXPECT_FALSE(dataserver->ds_h_test_->DSRegister(sync_register, false, true, 0));
}

}//namespace dataserver
}//namespace baldestore
