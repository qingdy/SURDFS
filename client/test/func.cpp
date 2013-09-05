#include <vector>
#include <time.h>
#include "client.h"
#include "located_blocks.h"
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "int_to_string.h"
#include "gtest/gtest.h"
#include "log.h"
#include "time_util.h"

using std::vector;
using pandora::TimeUtil;
using namespace bladestore::common;
using namespace bladestore::client;

//class TestNS : public ::testing::Test
//{
//protected:
//    virtual void SetUp()
//    {
//        client_ = new Client();
//        int ret = client_->Init();
//        if (BLADE_ERROR == ret)
//            abort();
//    }
//    virtual void TearDown()
//    {
//        delete client_;
//        client_ = NULL;
//    }
//    Client *client_;
//};
//
// TEST_F(TestNS, AddBlockTest)
// {
//     string dname[] = {"/L1", "/L1/L2", "/L1/L2/L3", "/L1/L2/L3/L4", "/L1/L2/L3/L4/L5"};
//     for (int i = 0; i < 5; ++i) {
//         client_impl_->MakeDirectory(dname[i]);
//     }
//     LocatedBlock lb;
//     for (int j = 0; j< 5; j++) {
//         for (int32_t k = 0; k < 100; ++k) {
//             string ks = dname[j];
//             ks.append("/file").append(Int32ToString(k));
//             LOGV(LL_INFO, "filepath: %s.", ks.c_str());
//             int64_t fid = client_impl_->Creat(ks, 0, 3);
//             ASSERT_GT(fid, 0);
//             for (int32_t i = 0; i < 200; ++i) {
//                 lb = client_impl_->AddBlock(fid);
//                 ASSERT_GT(lb.block_id(), 0);
//                 LOGV(LL_INFO, "Block ID: %ld.", lb.block_id());
//                 client_impl_->Complete(fid, lb.block_id(), 64u<<20);
//             }
//             lb = client_impl_->AddBlock(fid);
//             client_impl_->Complete(fid, lb.block_id(), 67798);
//         }
//     }
// }

//TEST_F(TestNS, Mkdir1Test)
//{
//    for (int32_t i = 0; i < 10000; ++i) {
//        client_->MakeDirectory("/" + Int32ToString(i));
//    }
//    vector<string> filenames = client_->GetListing("/");
//
//    for (vector<string>::iterator it = filenames.begin(); it != filenames.end(); ++ it) {
//        LOGV(LL_INFO, "filenames: %s.", (*it).c_str());
//    }
//
////    for (int32_t i = 0; i < 10000; ++i) {
////        client_->DeleteFile("/" + Int32ToString(i));
////    }
//}

//TEST_F(TestNS, Mkdir2Test)
//{
//    string dir = "";
//    int64_t maxtime1;
//    time_t mkdirstart = time(NULL);
//    for (int32_t i = 0; i < 10000; ++i) {
//        dir.append("/").append(Int32ToString(i));
//        int64_t t1 = TimeUtil::GetTime();
//        client_->MakeDirectory(dir);
//        maxtime1 = TimeUtil::GetTime() - t1;
//    }
//    time_t mkdirend = time(NULL);
//
//    client_->SetWorkingDir(dir);
//    vector<string> filelist1 = client_->GetListing(dir);
//    vector<string> filelist2 = client_->GetListing("..");
//
//    int64_t maxtime2;
//    time_t deletestart = time(NULL);
//    for (; dir != ""; dir = dir.substr(0, dir.rfind("/"))) {
//        int64_t t2 = TimeUtil::GetTime();
//        client_->Delete(dir);
//        maxtime2 = TimeUtil::GetTime() - t2;
//    }
//    time_t deleteend = time(NULL);
//
//    printf("mkdir start: %ld.\n", mkdirstart);
//    printf("mkdir end: %ld.\n", mkdirend);
//    printf("mkdir maxtime: %ldms.\n", maxtime1);
//
//    printf("delete start: %ld.\n", deletestart);
//    printf("delete end: %ld.\n", deleteend);
//    printf("delete maxtime: %ldms.\n", maxtime2);
//    //printf the content of filelists
//    for (vector<string>::iterator it = filelist1.begin(); it != filelist1.end(); ++it) {
//        printf("filelist1: %s\n", (*it).c_str());
//    }
//
//    for (vector<string>::iterator it = filelist2.begin(); it != filelist2.end(); ++it) {
//        printf("filelist2: %s\n", (*it).c_str());
//    }
//}

//TEST_F(TestNS, MkdirLongNameTest)
int main(int argc, char ** argv)
{
    Client client;
    int ret = client.Init();
    if (BLADE_ERROR == ret)
        abort();
    string dir("/aaaaaaaaa");
    ret = BLADE_SUCCESS;
    int64_t i;
//    for (i = 10; ret==BLADE_SUCCESS; i += 10) {
//        ret = client.MakeDirectory(dir);
//        LOGV(LL_INFO, "the result length is: %ld.", i);
//        if (ret == BLADE_SUCCESS)
//            dir.append("aaaaaaaaaa");
//        else
//            break;
//    }
//
//    LOGV(LL_INFO, "the result length is: %ld.", i);


//TEST_F(TestNS, MkdirDepthTest)
    dir = "/"+string(argv[0]+2)+"b";
    ret = BLADE_SUCCESS;
    for (i = 1; ret==BLADE_SUCCESS; ++i) {
        ret = client.MakeDirectory(dir);
        LOGV(LL_INFO, "the result depth is: %ld.", i);
        if (ret == BLADE_SUCCESS)
            dir.append("/b");
        else
            break;
    }

    LOGV(LL_INFO, "the result depth is: %ld.", i);

}

