#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include "client.h"
#include "blade_common_define.h"
#include "time_util.h"

using namespace bladestore::client;
using bladestore::common::BLADE_ERROR;
using namespace pandora;

int main(int argc, char **argv)
{
    if(argc != 2)
    {
        printf("./exefile index\n");  
        return 0;
    }
    string index(argv[1]);
    ::pandora::Log::logger_.SetLogPrefix("../log", (index + "_" + "client_log").c_str());
    ::pandora::Log::logger_.set_max_file_size(100000000);
    ::pandora::Log::logger_.set_max_file_count(20);

    Client client;
    int initret = client.Init();
    if (BLADE_ERROR == initret)
        abort();

    int64_t fid;
    ssize_t readlocallen;
    int64_t writelen;
    int writelentotal=0;
    int32_t buffersize = 10485760;   //1M : 1 << 20 
    char *buf = (char *)malloc(buffersize);
    string src = (argv[0] ); 
    src.append(argv[1]);

//    int localfid = open("/opt/BladeStore/100Mfile", O_RDONLY);//本地文件
//    int localfid = open("/data4/BladeStore/client/test/data/64Mfile", O_RDONLY);//本地文件
    int localfid = open("file", O_RDONLY);
	if (localfid < 0)
		return -1;
//    printf("localfid:%d\n", localfid);

    fid = client.Create(src, 0, 3);
//    printf("bladefid:%ld\n", fid);
      if (fid < 0) {  
        printf("creat file error\n");
        return -1;
    }  
	else 
	{     
        int64_t begin = TimeUtil::GetTime();
        do {
//            printf("buffersize: %d\n", buffersize);
            readlocallen = read(localfid, (void *)buf, buffersize);
//            printf("readlocallen:%ld\n", readlocallen);
            //把缓存内容写入BS
             if (readlocallen > 0) {
                writelen = client.Write(fid, buf, readlocallen);
//                printf("writelen:%ld\n", writelen);
                if (readlocallen != writelen)
                 {
                    printf("############not all write:read:%ld,write:%ld######################\n",readlocallen,writelen);
                    return -1;
                   }
                if (writelen != readlocallen)
                   {
                    printf("@@@@@@@@@@@@@@@@@@error in write@@@@@@@@@@@@@@@@@@@@@@@@@\n");
                    return -1;
                }
                writelentotal += writelen;
              } 
          } while (readlocallen > 0);

        int64_t end = TimeUtil::GetTime();
//	printf("before close\n");
        client.Close((int64_t)fid);
//	printf("after close\n");
        close(localfid);
        printf("writelentotal:%d\n",writelentotal);
        free(buf);

//		int64_t end = TimeUtil::GetTime();
		double used = (end - begin)/1000000.000000;
		printf("start time is : %ld\n",begin);
        printf("finish time is : %ld\n",end);
		printf("used total time is : %lf s\n",used);
		printf("tun tu lv is : %lf MB/s\n",100/used);
    }      
}
