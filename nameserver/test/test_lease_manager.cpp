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
#include "blade_meta_manager.h"
#include "lease_manager.h"

namespace bladestore
{

namespace nameserver
{
using namespace nameserver;
class T
{
public:
    T(){num = 0;}
    int32_t  push(BladePacket *){ return ++num;}
    int32_t num;
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
class LeaseManagerTest : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
        T * t1 = new T();
        T * t2 = new T();
        T * t3 = new T();
        NameServerImpl * n = new FateNS(*t1, *t2, *t3);
        l = new LeaseManager(n);
        ASSERT_TRUE(l != NULL);

        for(int i = 1; i < 100; i++)
        {
            EXPECT_EQ(BLADE_SUCCESS, l->register_lease(i, i));
        }

        //l->start();
    }
    virtual void TearDown()
    {
        delete l;
    }
    LeaseManager * l;
};
    

TEST_F(LeaseManagerTest, RegisterTest)
{
    //abnormal
    {
        for(int i = 1; i <10; i++)
        {
            EXPECT_EQ(BLADE_ERROR, l->register_lease(i, i));
        }
    }
}

TEST_F(LeaseManagerTest, HasValidTest)
{
    //normal
    {
        for(int i = 1; i <10; i++)
        {
            EXPECT_EQ(true, l->has_valid_lease(i));
        }
    }
    //abnormal 
    {
        for(int i = 100; i < 200; i++)
        {
            EXPECT_EQ(false, l->has_valid_lease(i));
        }
    }
    
}

TEST_F(LeaseManagerTest, RenewTest)
{
    //normal
    {
        for(int i = 1; i <100; i++)
        {
            EXPECT_EQ(BLADE_SUCCESS, l->renew_lease(i));
        }
    }
    //abnormal 
    {
        for(int i = 100; i < 200; i++)
        {
            EXPECT_EQ(BLADE_ERROR, l->renew_lease(i));
        }
    }
    
}

TEST_F(LeaseManagerTest, RelinquishTest)
{
    //normal
    {
        for(int i = 1; i <50; i++)
        {
            EXPECT_EQ(BLADE_SUCCESS, l->relinquish_lease(i));
        }
    }
    //abnormal
    {
        for(int i = 1; i < 10; i++)
        {
            EXPECT_EQ(BLADE_ERROR, l->relinquish_lease(i));
        }
        for(int i = 1000; i < 1010; i++)
        {
            EXPECT_EQ(BLADE_ERROR, l->relinquish_lease(i));
        }
    }
    //re-register
    {
        for(int i = 1; i < 10; i++)
        {
            EXPECT_EQ(BLADE_SUCCESS, l->register_lease(i, i));
        }
    }
}


//TEST_F(LeaseManagerTest, ThreadTest)
//{
//    l->start();
//    FateNS * p = static_cast<FateNS *> (l->nameserver_);
//    usleep(100 * 1000000);
//    EXPECT_EQ(0, p->get_write_packet_queue_thread().num);
//    usleep(210 * 1000000);
//    EXPECT_EQ(99, p->get_write_packet_queue_thread().num);
//    {
//        for(int i = 1; i <100; i++)
//        {
//            EXPECT_EQ(BLADE_SUCCESS, l->relinquish_lease(i));
//        }
//    }
//
//    {
//        for(int i = 1; i < 100; i++)
//        {
//            EXPECT_EQ(BLADE_SUCCESS, l->register_lease(i, i));
//        }
//    }
//    EXPECT_EQ(99, p->get_write_packet_queue_thread().num);
//
//    usleep(100 * 1000000);
//    {
//        for(int i = 1; i <100; i++)
//        {
//            EXPECT_EQ(BLADE_SUCCESS, l->renew_lease(i));
//        }
//    }
//    usleep(210 * 1000000);
//    EXPECT_EQ(99, p->get_write_packet_queue_thread().num);
//    usleep(100 * 1000000);
//    EXPECT_EQ(198, p->get_write_packet_queue_thread().num);
//
//}

}
}
