#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
#include "client.h"
#include "blade_common_define.h"
#include "int_to_string.h"
#include "time_util.h"
#include "blade_file_info.h"
#include "cthread.h"
#include "log.h"

using namespace bladestore::client;
using namespace bladestore::common;
using namespace pandora;

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

        int64_t fid;
        ssize_t readlen;
        int32_t buffersize = 10485760;
        char *buf = (char *)malloc(buffersize);
        string filename = src + Int32ToString(*((int64_t*)arg));

        fid = client->Open(filename, O_RDONLY);
        if (fid == -4) { 
            printf("BLADE_FILE_NOT_EXIST\n");
        } else {
            do {
                readlen = client->Read(fid, (void *)buf, buffersize);
            } while (readlen > 0);
            free(buf);  
        }
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

    printf("read tuntun: %lf MB/s.\n", (double)1000000 * thread_num * filenum * filelen / expendtime);
}
