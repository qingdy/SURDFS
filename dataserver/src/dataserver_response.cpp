
#include "blade_packet.h"
#include "dataserver_response.h"

namespace bladestore
{
namespace dataserver
{
/*----------------------------------WriteResponse-----------------------------------------*/
WriteResponse::WriteResponse() : DataServerResponse()
{
    channel_id_ = 0;
    AtomicSet(&response_count_,0);
}

WriteResponse::WriteResponse(int16_t operation, 
                             AmFrame::StreamEndPoint end_point,
                             uint32_t channel_id, uint64_t peer_id)
    :DataServerResponse(operation), end_point_(end_point), channel_id_(channel_id), peer_id_(peer_id)
{
    AtomicSet(&response_count_,0);
    AtomicSet(&error_,0);
}

WriteResponse::~WriteResponse()
{
}

int32_t WriteResponse::AddResponseCount()
{
    return AtomicAdd(&response_count_, 1);
}

/*----------------------------------ClientTask-----------------------------------------*/
ClientTask::ClientTask()
{
    packet_ = NULL;
    write_response_ = NULL;
}

ClientTask::ClientTask(BladePacket * packet, WriteResponse * write_response)
    : packet_(packet), write_response_(write_response)
{
}

ClientTask::~ClientTask()
{
    if (packet_)
        delete packet_;
    // can't delete client_data here because client_data will be used by replypacket
}

/*----------------------------------SyncResponse-----------------------------------------*/
SyncResponse::SyncResponse(int16_t operation) : DataServerResponse(operation)
{
    response_ok_ = false;
    AtomicSet(&response_count_, 0);
    AtomicSet(&error_, 0);
    AtomicSet(&base_count_, 0);
    wait_ = true;
}

SyncResponse::SyncResponse(int16_t operation, int32_t base_count) : DataServerResponse(operation)
{
    AtomicSet(&base_count_, base_count);
    response_ok_ = false;
    AtomicSet(&response_count_, 0);
    AtomicSet(&error_, 0);
    wait_ = true;
}

SyncResponse::~SyncResponse()
{
}

int32_t SyncResponse::AddResponseCount()
{
    return AtomicAdd(&response_count_, 1);
}

/*-------------------------------------NSTask--------------------------------------*/
NSTask::NSTask()
{
    packet_ = NULL;
    sync_response_ = NULL;
}

NSTask::NSTask(BladePacket * packet, SyncResponse * sync_response)
    : packet_(packet), sync_response_(sync_response)
{
}

NSTask::~NSTask()
{
    if (NULL != packet_)
        delete packet_;
}

}//end of namespace
}
