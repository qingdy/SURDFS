#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include "client.h"
#include "blade_common_define.h"
#include "time_util.h"
#include "int_to_string.h"

using namespace bladestore::client;
using namespace bladestore::common;
using namespace pandora;

int main(int argc, char **argv)
{
    if(argc != 2)
    {
        printf("./exefile index\n");
        return 0;
    }
    string index(argv[1]);
    ::pandora::Log::logger_.SetLogPrefix("../log", (index + "_" + "read_client_log").c_str());
    ::pandora::Log::logger_.set_max_file_size(100000000);
    ::pandora::Log::logger_.set_max_file_count(20);

    Client client;
    int initret = client.Init();
    if (BLADE_ERROR == initret)
        abort();

    int64_t fid;
    ssize_t readlen;
//    int writelocallen;
    int32_t buffersize = 5242880;
    char *buf = (char *)malloc(buffersize);
    string src(argv[0]);
    src.append(argv[1]);

//    int localfid = open("/data4/BladeStore/client/test/readfile", O_RDWR | O_APPEND | O_CREAT | O_TRUNC);//本地文件
//    printf("localfid:%d\n", localfid);

    fid = client.Open(src, O_RDONLY);
//    printf("bladefid:%d\n", fid);
    if (fid == -4) { 
        printf("BLADE_FILE_NOT_EXIST\n");
        return -1;
    } else {

	int64_t begin = TimeUtil::GetTime();
         do {
//            printf("buffersize: %d\n", buffersize);
            readlen = client.Read(fid, (void *)buf, buffersize);
//            printf("readlen:%d\n", readlen);
            //把缓存内容写入本地文件
//              if (readlen > 0) {
//                writelocallen = write(localfid, buf, readlen);
//               printf("writelocallen:%d\n", writelocallen);
//            }
        } while (readlen > 0);
//        close(localfid);
        free(buf);

		int64_t end = TimeUtil::GetTime();
		double used = (end - begin)/1000000.000000;
		printf("start time is : %ld\n",begin);
		printf("finish time is : %ld\n",end);
	    printf("used total time is : %lf s\n",used);
		printf("tun tu lv is : %lf MB/s\n",100/used);
//		string path("/opt/BladeStore/tuntu/");
//		string tmp("_");
//		path = path + argv[0] + tmp + Int64ToString(begin) + tmp + Int64ToString(end);
//		int logfid = open(path.c_str(), O_RDWR | O_APPEND | O_CREAT | O_TRUNC);
//		close(logfid);
    }
    client.Close(fid);
	return 0;
}
