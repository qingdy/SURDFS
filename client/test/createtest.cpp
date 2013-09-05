#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include "client.h"
#include "blade_common_define.h"
#include "int_to_string.h"
#include "time_util.h"
#include "cthread.h"
#include "log.h"

#define random(x) (rand()%x)

using namespace bladestore::client;
using namespace bladestore::common;
using namespace pandora;

int main(int argc, char **argv)
{
    if (argc != 2) {
        LOGV(LL_ERROR,"params error.");
        return -1;
    }
    Client client;
    int initret = client.Init();
    if (BLADE_ERROR == initret)
        abort();

    string dname[] = {"0", "0/1", "0/1/2", "0/1/2/3", "0/1/2/3/4"};

    string src(string(argv[0]) + argv[1]);
    //client.MakeDirectory(src);

    for (int i = 0; i < 5; ++i) {
        client.MakeDirectory(src + dname[i]);
    }

    int64_t time_start = TimeUtil::GetTime();
    LOGV(LL_INFO, "StartTime: %ld.", time_start);

    for (int32_t k = 0; k < 486; ++k) {
        client.Create(src + dname[random(5)] + "/" + Int32ToString(k) + "_" + (argv[0] + 2), 1, 3);
        //client.Create(Int32ToString(k) + "_" + (argv[0] + 2), 1, 3);
    }

    int64_t time_end = TimeUtil::GetTime();
    LOGV(LL_INFO, "EndTime: %ld.", time_end);

    LOGV(LL_INFO, "UsedTime: %ld.", time_end - time_start);
    LOGV(LL_INFO, "IOPS: %lf.", 1000000 * (double)5.000000/(time_end - time_start));
}
//int thread_num = 10;
//string clientindex;
//class MyRunable:public Runnable
//{
//public:
//    void Run(CThread * thread, void * arg)
//    {
//        LOGV(LL_INFO,"start thread[%ld]", *(int64_t*)arg);
//        Client *client = new Client();
//        int initret = client->Init();
//        if (BLADE_ERROR == initret)
//            abort();
//        for (int32_t k = 0; k < 1000; ++k) {
//            client->Create("/" + Int32ToString(*((int64_t*)arg)) + "_" + Int32ToString(k) + "_" + clientindex, 1, 3);
//        }
//
////        for (int32_t k = 0; k < 1000; ++k) {
////            client.Delete("/" + Int32ToString(k) + (argv[0] + 2));
////        }
//
//    }
//};
//
//int main(int argc, char **argv)
//{
//
//    clientindex = string(argv[0] + 2);
//    MyRunable mr;
//    CThread *thread[thread_num];
//    int64_t id[thread_num];
//
//    for (int64_t t = 0; t < thread_num; ++t)
//    {
//        id[t] = t;
//    }
//
//    int64_t i;
//    for(i = 0;i < thread_num;i++)
//    {
//        thread[i] = new CThread();
//        thread[i]->Start(&mr, &id[i]);
//    }
//
//    for(i = 0;i < thread_num;i++)
//    {
//        thread[i]->Join();
//    }
//}
