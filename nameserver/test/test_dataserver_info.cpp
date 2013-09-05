/*
*version : 1.0
*author  : funing
*date    : 2012-6-8
*/

#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "gtest/gtest.h"
#include "blade_dataserver_info.h"

namespace bladestore
{

namespace nameserver
{

class DataServerInfoTest : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
    DataServerInfo * d;
};


TEST_F(DataServerInfoTest, StartTest)
{
    d = new DataServerInfo();
    ASSERT_TRUE(d != NULL);
}

TEST_F(DataServerInfoTest, CopyTest)
{
    //
    {
    }
    //
}

TEST_F(DataServerInfoTest, InsertTest)
{
    //
    {
        set<DataServerInfo *> ds_set;
        set<DataServerInfo *>::iterator iter;
        set<DataServerInfo *>::iterator iter2;
        DataServerInfo * b_set;
        for(int i = 0 ; i < 20; i++)
        {
            b_set = new DataServerInfo();
            b_set->dataserver_id_ = 1;
            ds_set.insert(b_set);
        }
        EXPECT_EQ(1, ds_set.size());
    
    }
    {
        set<DataServerInfo *> ds_set;
        set<DataServerInfo *>::iterator iter;
        set<DataServerInfo *>::iterator iter2;
        DataServerInfo * b_set;
        for(int i = 0 ; i < 20; i++)
        {
            b_set = new DataServerInfo();
            b_set->dataserver_id_ = i;
            ds_set.insert(b_set);
        }
        iter = ds_set.begin();
        iter2 = iter;
        iter2++;
        for(int i = 0 ; i < 19; i++ )
        {
            EXPECT_LT((*iter)->dataserver_id_, (*iter2)->dataserver_id_);
            iter++;
            iter2++;
        }
    
    }
    {
        set<DataServerInfo *> ds_set;
        set<DataServerInfo *>::iterator iter;
        set<DataServerInfo *>::iterator iter2;
        DataServerInfo * b_set;
        for(int i = 0 ; i < 20; i++)
        {
            b_set = new DataServerInfo();
            b_set->dataserver_id_ = 20-i;
            ds_set.insert(b_set);
        }
        iter = ds_set.begin();
        iter2 = iter;
        iter2++;
        for(int i = 0 ; i < 19; i++ )
        {
            EXPECT_GT((*iter)->dataserver_id_, (*iter2)->dataserver_id_);
            iter++;
            iter2++;
        }
    
    }
}

TEST_F(DataServerInfoTest, InsertTest2)
{
    //
    {
        set<DataServerInfo> ds_set;
        set<DataServerInfo>::iterator iter;
        set<DataServerInfo>::iterator iter2;
        for(int i = 0 ; i < 20; i++)
        {
            DataServerInfo b_set;
            b_set.dataserver_id_ = 1;
            ds_set.insert(b_set);
        }
        EXPECT_EQ(1, ds_set.size());
    
    }
    {
        set<DataServerInfo > ds_set;
        set<DataServerInfo >::iterator iter;
        set<DataServerInfo >::iterator iter2;
        for(int i = 0 ; i < 20; i++)
        {
            DataServerInfo b_set;
            b_set.dataserver_id_ = i;
            ds_set.insert(b_set);
        }
        iter = ds_set.begin();
        iter2 = iter;
        iter2++;
        //(*iter).block_id_;
        for(int i = 0 ; i < 19; i++ )
        {
           // EXPECT_LT((iter).block_id_, (iter2).block_id_);
            //EXPECT_LT(iter, iter2);
            iter++;
            iter2++;
        }
    
    }
}

}
}
