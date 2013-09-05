/*
*version : 1.0
*author  : funing
*date    : 2012-6-8
*/
#include "nameserver_impl.h"
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "gtest/gtest.h"
#include "blade_nameserver_handler.h"
#include "heartbeat_manager.h"
#include "blade_meta_manager.h"


namespace bladestore
{

namespace nameserver
{
using namespace nameserver;

class HeartbeatManagerTest : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
    HeartbeatManager * h;
};

class T
{
public:
   int32_t  push(BladePacket *){return 1;}
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
    FateMeta(NameServerImpl * f):MetaManager(f){}
    int32_t update_ds(BladePacket *, BladePacket *){return BLADE_SUCCESS; }
private: 
        
};



TEST_F(HeartbeatManagerTest, ExcuteTest)
{
    T * t1 = new T();
    T * t2 = new T();
    T * t3 = new T();
    FateNS * n = new FateNS(*t1, *t2, *t3);
    FateMeta * m = new FateMeta(n);
    h = new HeartbeatManager(*m);
    MetaManager & meta = h->meta_manager_;
    ASSERT_TRUE(h != NULL);
    //n->get_heartbeat_manager_thread().start();
    {
        BladePacket *p = new HeartbeatPacket();
        //EXPECT_EQ(BLADE_SUCCESS, h->push(p));
        delete p;
    } 
    
}




}
}
