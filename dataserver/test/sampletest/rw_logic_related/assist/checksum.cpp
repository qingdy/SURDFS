#include "blade_crc.h"
#include "fcntl.h"
#include "log.h"
using namespace bladestore::common;
using namespace pandora;

int main(int argc, char **argv)
{
    //declare some local variables
    int32_t fd_block_data;
    int32_t fd_checksum;
    //get packet info
    int64_t block_length;
    int64_t checksum_offset;
    int16_t ret_code;
    int64_t end_offset;

    //read the data
    fd_block_data = open("/home/block/blk_3", O_RDWR);
    fd_block_data = open("/home/block/blk_3_2.meta", O_RDWR);
    LOGV(LL_INFO, "fd_block,%d", fd_block_data);
    char *data_temp = static_cast<char *>(malloc(64:wq*1024*1024+5));  
    char *checksum = static_cast<char *>(malloc(16*1024+5));
    if(data_temp == NULL ||checksum == NULL)
    LOGV(LL_INFO, "malloc error");
    ssize_t flag = read(fd_block_data, data_temp, 32*1024);
    if (flag == -1){
        LOGV(LL_INFO, "readerror:%d",flag);
        return 0;
    }
    LOGV(LL_INFO, "readok:%d",flag);
    sleep(5);
    if (Func::crc_generate(data_temp, checksum, 28*1024))
        LOGV(LL_INFO, "checksum success:%s \n",checksum);

    sleep(5);
    
    Func::crc_update(checksum, 4, data_temp, 28*1024, 4*1024);
   // generate the reply packet
    LOGV(LL_INFO, "send ok\n");
//     write checksum file

    bool yyy = Func::crc_check(data_temp, checksum , 32*1024);
    if  (yyy)
        LOGV(LL_INFO, "checksum match hah .\n");
    else
        LOGV(LL_INFO, "checksum not match.\n");
    LOGV(LL_INFO, "checksum success:%s \n",checksum);

    if (Func::crc_generate(data_temp, checksum, 32*1024))
        LOGV(LL_INFO, "checksum success:%s \n",checksum);
    free(data_temp);
    free(checksum);
    return 1;
}
    //
