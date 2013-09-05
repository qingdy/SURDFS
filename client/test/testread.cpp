#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include "client.h"
#include "blade_common_define.h"
#include "time_util.h"

using namespace bladestore::client;
using namespace bladestore::common;
using namespace pandora;

int main(int argc, char **argv)
{
    //测试bladestore的读流程
    if (argc != 3) {
        printf("%s filepath buffersize\n", argv[0]);
        return -1;
    }

    Client client;
    int initret = client.Init();
    if (BLADE_ERROR == initret)
        abort();

    int64_t fid;
    ssize_t readlen;
    int writelocallen;
    int32_t buffersize = atoi(argv[2]);
    char *buf = (char *)malloc(buffersize);
    string src(argv[1]);

    int localfid = open("/data4/BladeStore/client/test/readfile", O_RDWR | O_APPEND | O_CREAT | O_TRUNC);//本地文件
    printf("localfid:%d\n", localfid);

    fid = client.Open(argv[1], O_RDONLY);
    printf("bladefid:%d\n", fid);
    if (fid == -4) {
        printf("BLADE_FILE_NOT_EXIST\n");
        return -1;
    } else {

	int64_t begin = TimeUtil::GetTime();
        do {
            printf("buffersize: %d\n", buffersize);
            readlen = client.Read(fid, (void *)buf, buffersize);
            printf("readlen:%d\n", readlen);
            //把缓存内容写入本地文件
            if (readlen > 0) {
                writelocallen = write(localfid, buf, readlen);
                printf("writelocallen:%d\n", writelocallen);
            }
        } while (readlen > 0);
        close(localfid);
        free(buf);

		int64_t end = TimeUtil::GetTime();
		double used = (end - begin)/1000000.000000;
		printf("start time is : %ld\n",begin);
		printf("finish time is : %ld\n",end);
	    printf("used total time is : %lf s\n",used);
		printf("tun tu lv is : %lf MB/s\n",100/used);
    }
    client.Close(fid);
}
