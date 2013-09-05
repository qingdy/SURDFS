#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include "log.h"
#include "client_net_handler.h"
#include "blade_net_util.h"
#include "write_pipeline_packet.h"

using namespace bladestore::client;
using namespace pandora;

int main(int argc, char **argv)
{
    //测试bladestore的读流程
    if (argc != 8) {
        printf("usage:%s file_id  block_id  block_version  mode  ds1  ds2  ds3\n", argv[0]);
        return -1;
    }

    int64_t file_id = atol(argv[1]);
    int64_t block_id = atol(argv[2]);
    int32_t block_version = atoi(argv[3]);
    int8_t mode = atol(argv[4]);

    vector<uint64_t> ds_ids; 
    uint64_t ds_id = BladeNetUtil::string_to_addr(argv[5]);
    ds_ids.push_back(ds_id);
    ds_id = BladeNetUtil::string_to_addr(argv[6]);
    ds_ids.push_back(ds_id);
    ds_id = BladeNetUtil::string_to_addr(argv[7]);
    ds_ids.push_back(ds_id);

    ClientStreamSocketHandler * ds_conn_handler_ = new ClientStreamSocketHandler(BladeCommonData::amframe_);
    assert(ds_conn_handler_);
    int32_t error = BladeCommonData::amframe_.AsyncConnect(argv[6], ds_conn_handler_);
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
    BladeCommonData::amframe_.CloseEndPoint(ds_conn_handler_->end_point());
    return error;

}

