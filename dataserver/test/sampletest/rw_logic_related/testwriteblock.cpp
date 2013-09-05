#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include "client.h"
#include "blade_common_define.h"

using namespace bladestore::client;
using bladestore::common::BLADE_ERROR;

int main(int argc, char **argv)
{
    //测试bladestore的写流程
    if (argc != 2) {
        printf("%s filepath \n", argv[0]);
        return -1;
    }

    Client client;
    int initret = client.Init();
    if (BLADE_ERROR == initret)
        abort();

    int64_t fid;
    ssize_t readlocallen;
    int64_t writelen;
    int writelentotal=0;
    int32_t buffersize = 10485760;
    char *buf = (char *)malloc(buffersize);
    string src(argv[1]);

    int localfid = open("writefile", O_RDONLY);//本地文件
    //int localfid = open("/root/BladeStore/trunk/client/test/part1", O_RDONLY);//本地文件
    printf("localfid:%d\n", localfid);

    fid = client.Create(argv[1], 0, 3);
    printf("bladefid:%ld\n", fid);
    if (fid < 0) {
        printf("creat file error\n");
        return -1;
    } else {
        do {
            //printf("buffersize: %d\n", buffersize);
            readlocallen = read(localfid, (void *)buf, buffersize);
            printf("readlocallen:%ld\n", readlocallen);
            //把缓存内容写入BS
            if (readlocallen > 0) {
                writelen = client.Write(fid, buf, readlocallen);
                printf("writelen:%ld\n", writelen);
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

        client.Close((int64_t)fid);
        close(localfid);
        printf("writelentotal:%d\n",writelentotal);
        free(buf);
    }
}
