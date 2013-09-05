#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include "log.h"
#include "mempool.h"
#include "client_net_handler.h"
#include "blade_net_util.h"
#include "write_pipeline_packet.h"
#include "write_packet_packet.h"
#include "blade_crc.h"

using namespace bladestore::client;
using namespace pandora;

int main(int argc, char **argv)
{
    //测试bladestore的读流程
    if (argc != 11) {
        printf("usage:%s file_id  block_id  block_version  block_offset data_length  mode flag ds1  ds2  ds3\n", argv[0]);
        return -1;
    }

    bool ret = MemPoolInit();
    assert(ret == true);

    int64_t file_id = atol(argv[1]);
    int64_t block_id = atol(argv[2]);
    int32_t block_version = atoi(argv[3]);
    int64_t block_offset = atol(argv[4]);
    int64_t data_length = atol(argv[5]);
    int8_t mode = atoi(argv[6]);
    int8_t flag = atoi(argv[7]);

    vector<uint64_t> ds_ids; 
    uint64_t ds_id = BladeNetUtil::string_to_addr(argv[8]);
    ds_ids.push_back(ds_id);
    ds_id = BladeNetUtil::string_to_addr(argv[9]);
    ds_ids.push_back(ds_id);
    ds_id = BladeNetUtil::string_to_addr(argv[10]);
    ds_ids.push_back(ds_id);

    ClientStreamSocketHandler * ds_conn_handler_ = new ClientStreamSocketHandler(BladeCommonData::amframe_);
    assert(ds_conn_handler_);
    int32_t error = BladeCommonData::amframe_.AsyncConnect(argv[8], ds_conn_handler_);
    if (error < 0) {
        LOGV(LL_ERROR, "AsyncConnect error for connection to dataserver.");
        return CLIENT_CAN_NOT_CONNECT_DATASERVER;
    } else {
        LOGV(LL_INFO, "Connect to Dataserver returned normally.");
    }

    WritePipelinePacket *p = new WritePipelinePacket(file_id, block_id, block_version, mode, ds_ids);
    assert(p);
    if (BLADE_SUCCESS != p->pack()) {
        LOGV(LL_ERROR, "pack WritePipelinePacket error.");
        delete p;
        return -1;
    }

    error = BladeCommonData::amframe_.SendPacket(ds_conn_handler_->end_point(), p);
    if (error == 0) {
        LOGV(LL_DEBUG, "packet already sent.");
    } else {
        delete p;
        LOGV(LL_ERROR, "Send packet error, error: %d", error);
    }
        
    sleep(1);
    char *data = static_cast<char *>(MemPoolAlloc(data_length));
    memset(data, 'a',data_length);
    int64_t checksum_len = ((block_offset + data_length + BLADE_CHUNK_SIZE - 1)/
                                   BLADE_CHUNK_SIZE) * BLADE_CHECKSUM_SIZE;
    char *checksum = static_cast<char *>(MemPoolAlloc(checksum_len));
    
    if (!Func::crc_generate(data, checksum, data_length, 0)) {
        LOGV(LL_ERROR, "crc generate error,block id:%ld ", block_id);
        if (NULL != data) {
        MemPoolFree(data);
        data = NULL;
        }
        if (NULL != checksum){
            MemPoolFree(checksum);
            checksum  = NULL;
        }
        return -1;
    }

    //if checksum_len_flag=1 我们就应该将其的checksum_len设置为不正确的
    switch (flag)
    {
        case 0:
            LOGV(LL_INFO, "normal case.");
            break;
        case 1:
            LOGV(LL_INFO, "data_length not match checksum_len case.");
            checksum_len -= 4;
            break;
        case 2:
            LOGV(LL_INFO, "data_ not match checksum case.");
            *checksum = 'b';
            break;
        case 3:
            LOGV(LL_INFO, "block_offset is not in [0,64M].");
            block_offset = 67108865;//64M +1
            checksum_len += 4;
            break;
        default:
            LOGV(LL_ERROR, "pack WritePipelinePacket error.");
            return -1;
    }

    WritePacketPacket *p_write = new WritePacketPacket(block_id, block_version, block_offset,
                                1, 3, data_length, checksum_len, data, checksum);
    assert(p_write);
    if (BLADE_SUCCESS != p_write->pack()) {
        LOGV(LL_ERROR, "pack WritePipelinePacket error.");
        delete p_write;
        return -1;
    }
    error = BladeCommonData::amframe_.SendPacket(ds_conn_handler_->end_point(), p_write);
    if (error == 0) {
        LOGV(LL_DEBUG, "write packet already sent.");
    } else {
        delete p_write;
        LOGV(LL_ERROR, "Send write packet error, error: %d", error);
        return -1;
    }
    sleep(1);
    BladeCommonData::amframe_.CloseEndPoint(ds_conn_handler_->end_point());
    MemPoolDestory();
    return error;

}

