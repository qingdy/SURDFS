/*
*version : 1.0
*author  : funing
*date    : 2012-6-8
*/
#include "nameserver.h"
#include "nameserver_impl.h"
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "gtest/gtest.h"
#include "blade_nameserver_handler.h"
#include "block_report_packet.h"
#include "get_listing_packet.h"

namespace bladestore
{

namespace nameserver
{
using namespace nameserver;

class BladeNsStreamHandlerTest : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
    BladeNsStreamHandler * d;
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



TEST_F(BladeNsStreamHandlerTest, OnReceivedTest)
{
    T * t1 = new T();
    T * t2 = new T();
    T * t3 = new T();
    FateNS * n = new FateNS(*t1, *t2, *t3);
    d = new BladeNsStreamHandler(*n);
    ASSERT_TRUE(d != NULL);
    {
        BladePacket *p = new HeartbeatPacket();
        p->pack();
        d->OnReceived(*p, NULL);
        delete p;
    }
    {
        BladePacket *p = new GetListingPacket(" ");
        p->pack();
        d->OnReceived(*p, NULL);
        delete p;
    }
    {
        BladePacket *p = new BlockReportPacket();
        p->pack();
        d->OnReceived(*p, NULL);
        delete p;
    } 
    
}




}
}
