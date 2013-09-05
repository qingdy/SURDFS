#include "dataserver.h"
#include "dataserver_net_server.h"
#include "block.h"
#include "mempool.h"
#include "blade_crc.h"
#include "fsdataset.h"
#include "blade_common_define.h"
using namespace bladestore::dataserver;
using namespace bladestore::common;
using namespace bladestore::message;
using namespace pandora;

int main(int argc, char **argv)
{
    //declare some local variables
    FSDataSet dataset = FSDataSet("/home/dataserver");
    int32_t fd_block_data;
    int32_t fd_checksum;
    //get packet info
    int64_t block_length;
    int64_t checksum_offset;
    int16_t ret_code;
    int16_t chunks_num_to_read;
    int64_t end_offset;
    Block  block_local = Block(4,0,0);

    if(( dataset.meta_file_exists(block_local) == 0) ||(dataset.block_file_exists(block_local)) == 0){
        LOGV(LL_ERROR, "file not exist.\n");
        ret_code = RET_READ_BLOCK_FILE_INVALID;    
        return 0;
    }
    block_length = dataset.get_block_length(block_local);

    LOGV(LL_ERROR, "file  exist%d\n",block_length);
    //read the data
    fd_checksum = dataset.get_meta_file_description(block_local,0);
    fd_block_data = dataset.get_block_file_description(block_local,0);

    if((fd_block_data == -1)||(fd_checksum ==-1)){
        LOGV(LL_ERROR, "request length invalidata");
        return 0;
    }

    LOGV(LL_ERROR, "fd:%d ;fd2: %d;",fd_block_data,fd_checksum);
    sleep(5);
    char *data_temp = static_cast<char *>(malloc(block_length+5));  
    unsigned char *checksum = static_cast<unsigned char *>(malloc(block_length/BLADE_CHUNK_SIZE*4+5));
    if(data_temp == NULL ||checksum == NULL)
        LOGV(LL_INFO, "malloc");
    ssize_t flag = read(fd_block_data,data_temp,block_length);
    if (flag == -1){
        LOGV(LL_INFO, "readerror:%d",flag);
        return 0;
    }
    LOGV(LL_INFO, "readerror:%d",flag);
    sleep(5);
    if (Func::crc_generate(data_temp, checksum, block_length))
        LOGV(LL_INFO, "checksum success:%s block_length:%d\n",checksum,block_length);

    sleep(5);
    LOGV(LL_ERROR, "len: %d;data_temp:%s;\n", strlen(data_temp), data_temp);
    LOGV(LL_ERROR, " checksum:%s\n",checksum);
   // generate the reply packet
    LOGV(LL_INFO, "send ok\n");
//     write checksum file
    ssize_t flag1 =  write(fd_checksum,checksum,block_length/BLADE_CHUNK_SIZE*4);
    if (flag1 == -1)
        LOGV(LL_INFO, "write error`\n");

    unsigned char *chec_temp = static_cast< unsigned char *>(malloc(block_length/BLADE_CHUNK_SIZE*4+5));
    lseek(fd_checksum,0,SEEK_SET);
    lseek(fd_block_data,0,SEEK_SET);
    flag = read(fd_checksum,chec_temp,(block_length/BLADE_CHUNK_SIZE*4));
    LOGV(LL_INFO, "data_temp: %s \n",data_temp);
    LOGV(LL_INFO, "chec_temp:%s\n",chec_temp);
    bool yyy = Func::crc_check(data_temp,chec_temp,block_length);
    if  (yyy)
        LOGV(LL_INFO, "checksum match.\n");
    free(data_temp);
    free(checksum);
    free(chec_temp);
    return 1;
}
    //
