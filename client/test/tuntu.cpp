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

    string dname[] = {"0", "0/1", "0/1/2", "0/1/2/3", "0/1/2/3/4"};
//    int64_t time_end = TimeUtil::GetTime();
//    LOGV(LL_INFO, "EndTime: %ld.", time_end);
//
//    LOGV(LL_INFO, "UsedTime: %ld.", time_end - time_start);
//    LOGV(LL_INFO, "IOPS: %lf.", 1000000 * (double)5.000000/(time_end - time_start));
//}
string src;
int thread_num;
int filenum;
int filelen;
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


	string filename = src + Int32ToString(*((int64_t*)arg));
        int64_t fid = client->Create(filename, 0, 3);
            
        ssize_t readlocallen;
        int64_t writelen;
        int writelentotal=0;
        int localfid = open("file", O_RDONLY);
        int32_t buffersize = 10485760;
        char *buf = (char *)malloc(buffersize);
        do {
            readlocallen = read(localfid, (void *)buf, buffersize);
            if (readlocallen > 0) {
                writelen = client->Write(fid, buf, readlocallen);
                if (readlocallen != writelen)
                {   
                    printf("############not all write:read:%ld,write:%ld######################\n",readlocallen,writelen);
                    assert(0); 
                }   
                writelentotal += writelen;
            }   
        } while (readlocallen > 0); 

        client->Close((int64_t)fid);
        close(localfid);
        free(buf);

//        for (int32_t k = 0; k < 1000; ++k) {
//            client.Delete("/" + Int32ToString(k) + (argv[0] + 2));
//        }

    }
};

int main(int argc, char **argv)
{
    if (argc != 5) {
        printf("%s clientindex thread_num filenumperthread filelength\n", argv[0]);
        return -1;
    }

    ::pandora::Log::logger_.SetLogPrefix("../log", "client_log");
    ::pandora::Log::logger_.set_max_file_size(100000000);
    ::pandora::Log::logger_.set_max_file_count(20);

    thread_num = atoi(argv[2]);
    filenum = atoi(argv[3]);
    filelen = atoi(argv[4]);
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
    printf("tuntu: %lf MB/s.\n", (double)1000000 * thread_num * filenum * filelen / expendtime);
}
