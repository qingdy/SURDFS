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
    Client client;
    int initret = client.Init();
    if (BLADE_ERROR == initret)
        abort();

    int64_t fid;
    ssize_t readlen;
    int writelocallen;
    int32_t buffersize = 10485760;
    char *buf = (char *)malloc(buffersize);
    string src(argv[1]);

    int localfid = open("readfile", O_RDWR | O_APPEND | O_CREAT | O_TRUNC);//本地文件

    fid = client.Open(src, O_RDONLY);
//    printf("bladefid:%d\n", fid);
    if (fid == -4) { 
        printf("BLADE_FILE_NOT_EXIST\n");
        return -1;
    } else {
        printf("start read\n");

        do {
//            printf("buffersize: %d\n", buffersize);
            readlen = client.Read(fid, (void *)buf, buffersize);
//            printf("readlen:%d\n", readlen);
            //把缓存内容写入本地文件
              if (readlen > 0) {
                writelocallen = write(localfid, buf, readlen);
//               printf("writelocallen:%d\n", writelocallen);
            }
        } while (readlen > 0);
        close(localfid);
        free(buf);
    }
    printf("finish read\n");
    client.Close(fid);
	return 0;
}
