#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include "log.h"
#include "client_net_handler.h"
#include "read_block_packet.h"

using namespace bladestore::client;
using namespace pandora;

int main(int argc, char **argv)
{
    //测试bladestore的读流程
    if (argc != 6) {
        printf("usage:%s block_id  block_version  offset  length  ds1\n", argv[0]);
        return -1;
    }

    int64_t block_id = atol(argv[1]);
    int32_t block_version = atoi(argv[2]);
    int64_t offset = atol(argv[3]);
    int64_t length = atol(argv[4]);

    ClientStreamSocketHandler * ds_conn_handler_ = new ClientStreamSocketHandler(BladeCommonData::amframe_);
    assert(ds_conn_handler_);
    int32_t error = BladeCommonData::amframe_.AsyncConnect(argv[5], ds_conn_handler_);
    if (error < 0) {
        LOGV(LL_ERROR, "AsyncConnect error for connection to dataserver.");
        return CLIENT_CAN_NOT_CONNECT_DATASERVER;
    } else {
        LOGV(LL_INFO, "Connect to Dataserver returned normally.");
    }

    ReadBlockPacket *p = new ReadBlockPacket(block_id, block_version, (int64_t)offset, length, 1);
    assert(p);
    if (BLADE_SUCCESS != p->pack()) {
        LOGV(LL_ERROR, "pack ReadBlockPacket error.");
        delete p;
        return 0;
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

