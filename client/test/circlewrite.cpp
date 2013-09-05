#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include "client.h"
#include "blade_common_define.h"
#include "int_to_string.h"
#include "time_util.h"
#include "log.h"

using namespace bladestore::client;
using bladestore::common::BLADE_ERROR;
using pandora::TimeUtil;

int main(int argc, char **argv)
{
    Client client;
    int initret = client.Init();
    if (BLADE_ERROR == initret)
        abort();

    int64_t fid;
    ssize_t readlocallen;
    int64_t writelen;
    int writelentotal=0;
    int32_t buffersize = 1048576; // 1M 
    char current_time[20];
    char *buf = (char *)malloc(buffersize);

    int i = 0;
    while (i < 300) {
        int localfid = open("/data4/BladeStore/client/test/readfile", O_RDONLY);//本地文件
        string src = Int32ToString(i)+ (argv[0] + 2) + TimeUtil::TimeToStr(time(NULL), current_time);
        LOGV(LL_INFO, "filename: %s.", src.c_str());
        fid = client.Create(src, 0, 3);
        if (fid < 0) {
            LOGV(LL_ERROR, "Created file id: %ld error.", fid);
            return -1;
        }
        do {
            readlocallen = read(localfid, (void *)buf, buffersize);
            LOGV(LL_INFO, "readlocallen:%ld.", readlocallen);
            if (readlocallen > 0) {
                writelen = client.Write(fid, buf, readlocallen);
                LOGV(LL_INFO, "writelen:%ld.", writelen);
                if (readlocallen != writelen)
                {
                    LOGV(LL_ERROR, "not all write:read:%ld,write:%ld.",readlocallen,writelen);
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
        LOGV(LL_INFO, "writelentotal:%d.",writelentotal);

        ++i;
    }
    free(buf);
}
