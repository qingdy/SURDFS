/*
 *version : 1.0
 *author  : funing
 *date    : 2012-6-8
 *
 */

#include <set>
#include <gtest/gtest.h>

#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "blade_block_collect.h"

namespace bladestore
{

namespace nameserver
{
using namespace std;
class BlockCollectTest : public ::testing::Test
{
protected:
    virtual void SetUp()
    {
    }
    virtual void TearDown()
    {
    }
    BladeBlockCollect * b;
};


TEST_F(BlockCollectTest, StartTest)
{
    b = new BladeBlockCollect();
    ASSERT_TRUE(b != NULL);
    EXPECT_EQ(0, b->dataserver_set_.size());
    EXPECT_EQ(-1, b->block_id_);
    EXPECT_EQ(-1, b->version_);
    EXPECT_EQ(-1, b->replicas_number_);
}

TEST_F(BlockCollectTest, CopyTest)
{
    //
    {
    }
    //
}

TEST_F(BlockCollectTest, InsertTest)
{
    //
    {
        set<BladeBlockCollect *> block_set;
        set<BladeBlockCollect *>::iterator iter;
        set<BladeBlockCollect *>::iterator iter2;
        BladeBlockCollect * b_set;
        for(int i = 0 ; i < 20; i++)
        {
            b_set = new BladeBlockCollect();
            b_set->block_id_ = 1;
            block_set.insert(b_set);
        }
        //EXPECT_EQ(1, block_set.size());
    
    }
    {
        set<BladeBlockCollect *> block_set;
        set<BladeBlockCollect *>::iterator iter;
        set<BladeBlockCollect *>::iterator iter2;
        BladeBlockCollect * b_set;
        for(int i = 0 ; i < 20; i++)
        {
            b_set = new BladeBlockCollect();
            b_set->block_id_ = i;
            block_set.insert(b_set);
        }
        iter = block_set.begin();
        iter2 = iter;
        iter2++;
        for(int i = 0 ; i < 19; i++ )
        {
          //  EXPECT_LT((*iter)->block_id_, (*iter2)->block_id_);
            iter++;
            iter2++;
        }
    
    }
    {
        set<BladeBlockCollect *> block_set;
        set<BladeBlockCollect *>::iterator iter;
        set<BladeBlockCollect *>::iterator iter2;
        BladeBlockCollect * b_set;
        for(int i = 0 ; i < 20; i++)
        {
            b_set = new BladeBlockCollect();
            b_set->block_id_ = 20-i;
            block_set.insert(b_set);
        }
        iter = block_set.begin();
        iter2 = iter;
        iter2++;
        for(int i = 0 ; i < 19; i++ )
        {
            //EXPECT_LT((*iter)->block_id_, (*iter2)->block_id_);
            iter++;
            iter2++;
        }
    
    }
}
//TEST_F(BlockCollectTest, InsertTest2)
//{
//    //
//    {
//        set<BladeBlockCollect> block_set;
//        set<BladeBlockCollect>::iterator iter;
//        set<BladeBlockCollect>::iterator iter2;
//        BladeBlockCollect b_set;
//        for(int i = 0 ; i < 20; i++)
//        {
//            BladeBlockCollect b_set;
//            b_set.block_id_ = 1;
//            //block_set.insert(b_set);
//        }
//        //?EXPECT_EQ(1, block_set.size());
//    
//    }
//    {
//        set<BladeBlockCollect > block_set;
//        set<BladeBlockCollect >::iterator iter;
//        set<BladeBlockCollect >::iterator iter2;
//        BladeBlockCollect b_set;
//        for(int i = 0 ; i < 20; i++)
//        {
//            BladeBlockCollect b_set;
//            b_set.block_id_ = i;
//            //?block_set.insert(b_set);
//        }
//        iter = block_set.begin();
//        iter2 = iter;
//        iter2++;
//        //(*iter).block_id_;
//        for(int i = 0 ; i < 19; i++ )
//        {
//           // EXPECT_LT((iter).block_id_, (iter2).block_id_);
//            //EXPECT_LT(iter, iter2);
//            iter++;
//            iter2++;
//        }
//    
//    }
//}

}
}
