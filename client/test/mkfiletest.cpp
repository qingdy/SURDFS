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

string src;
int thread_num;
int filenum;
class MyRunable:public Runnable
{
public:
    void Run(CThread * thread, void * arg)
    {
        LOGV(LL_INFO,"start thread[%ld]", *(int64_t*)arg);
        Client *client = new Client();
        int initret = client->Init();
        if (BLADE_ERROR == initret)
            abort();

        for (int32_t k = 0; k < filenum ; ++k) {
            string name(src);
            name.append("_").append(Int32ToString(*((int64_t*)arg))).append("_").append(Int32ToString(k));
            client->Create(name, 0, 3);
        }
    }
};

int main(int argc, char **argv)
{
    if (argc != 4) {
        printf("%s clientindex thread_num filenumperthread\n", argv[0]);
        return -1;
    }

    ::pandora::Log::logger_.SetLogPrefix("../log", "client_log");
    ::pandora::Log::logger_.set_max_file_size(100000000);
    ::pandora::Log::logger_.set_max_file_count(20);

    thread_num = atoi(argv[2]);
    filenum = atoi(argv[3]);
    src = string(argv[1]);
    MyRunable mr;
    CThread *thread[thread_num];
    int64_t id[thread_num];


    for (int64_t t = 0; t < thread_num; ++t)
    {
        id[t] = t;
    }

    int64_t starttime = TimeUtil::GetTime();

    int64_t i;
    for(i = 0;i < thread_num;i++)
    {
        thread[i] = new CThread();
        thread[i]->Start(&mr, &id[i]);
    }

    for(i = 0;i < thread_num;i++)
    {
        thread[i]->Join();
    }

    int64_t endtime = TimeUtil::GetTime();

    int64_t expendtime = endtime - starttime;

    LOGV(LL_INFO, "thread number: %d, file number: %d.", thread_num, filenum);
    LOGV(LL_INFO, "ExpendTime: %ld.", expendtime);
    LOGV(LL_INFO, "IOPS: %lf.", (double)1000000 * thread_num * filenum / expendtime);
    printf("IOPS: %lf.\n", (double)1000000 * thread_num * filenum / expendtime);
}
