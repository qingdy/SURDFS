#include "log.h"
#include "client.h"
#include "amframe.h"
#include "client_net_handler.h"
#include "client_response.h"

namespace bladestore
{
namespace client
{

ClientResponse::ClientResponse(): operation_(0)
{
}

ClientResponse::ClientResponse(int16_t operation): operation_(operation)
{
}

ClientResponse::~ClientResponse()
{
}

ResponseSync::ResponseSync(): ClientResponse(), response_packet_(NULL), request_packet_(NULL), response_error_(false), is_sync_packet_(false), wait_(true)
{
    AtomicSet(&response_count_, 0);
    AtomicSet(&unit_total_count_, 0);
}

ResponseSync::ResponseSync(int16_t operation): ClientResponse(operation), response_packet_(NULL), request_packet_(NULL), response_error_(false), is_sync_packet_(false), wait_(true)
{
    AtomicSet(&response_count_, 0);
    AtomicSet(&unit_total_count_, 0);
}

ResponseSync::ResponseSync(int16_t operation, int32_t count): ClientResponse(operation), response_packet_(NULL), request_packet_(NULL), response_error_(false), is_sync_packet_(false), wait_(true)
{
    AtomicSet(&response_count_, 0);
    AtomicSet(&unit_total_count_, count);
}

ResponseSync::~ResponseSync()
{
    if (NULL != response_packet_) {
        delete response_packet_;
        response_packet_ = NULL;
    }
    if (NULL != request_packet_) {
        delete request_packet_;
        request_packet_ = NULL;
    }
    for(map<int64_t, BladePacket*>::iterator iter = request_packets_.begin(); iter != request_packets_.end(); iter++) {
        if (iter->second) {
            delete iter->second;
            iter->second = NULL;
        }
    } 
    request_packets_.clear();
    for(map<int64_t, BladePacket*>::iterator iter = response_packets_.begin(); iter != response_packets_.end(); iter++) {
        if (iter->second) {
            delete iter->second;
            iter->second = NULL;
        }
    } 
    response_packets_.clear();
}

int32_t ResponseSync::AddResponseCount()
{
    return AtomicAdd(&response_count_, 1);
}

bool ResponseSync::AddRequestPacket(int64_t seq, BladePacket* packet)
{
    rw_request_lock_.wlock()->lock();
    pair<map<int64_t, BladePacket*>::iterator, bool> ret = request_packets_.insert(make_pair(seq, packet));
//    LOGV(LL_DEBUG, "AddRequestPacket:seq:%ld", seq);
    rw_request_lock_.wlock()->unlock();
    return ret.second;
}

bool ResponseSync::AddResponsePacket(int64_t seq, BladePacket* packet)
{
    rw_response_lock_.wlock()->lock();
    pair<map<int64_t, BladePacket*>::iterator, bool> ret = response_packets_.insert(make_pair(seq, packet));
//    LOGV(LL_DEBUG, "AddResponsePacket:seq:%ld", seq);
    rw_response_lock_.wlock()->unlock();
    return ret.second;
}

void ResponseSync::EraseRequestPacket(map<int64_t, BladePacket*>::iterator it)
{
    rw_request_lock_.wlock()->lock();
    delete it->second;
    it->second = NULL;
//    LOGV(LL_DEBUG, "EraseRequestPacket:seq:%ld", it->first);
    request_packets_.erase(it);
    rw_request_lock_.wlock()->unlock();
}

bool ResponseSync::EraseRequestPacket(const int64_t seq)
{
    rw_request_lock_.wlock()->lock();
    map<int64_t, BladePacket*>::iterator it = request_packets_.find(seq);
    if (it != request_packets_.end()) {
        delete it->second;
        it->second = NULL;
 //       LOGV(LL_DEBUG, "EraseRequestPacket:seq:%ld", seq);
        request_packets_.erase(it);
        rw_request_lock_.wlock()->unlock();
        return true;
    } else {
        rw_request_lock_.rlock()->unlock();
        return false;
    }
}

void ResponseSync::EraseResponsePacket(map<int64_t, BladePacket*>::iterator it)
{
    rw_response_lock_.wlock()->lock();
    delete it->second;
    it->second = NULL;
    response_packets_.erase(it);
    rw_response_lock_.wlock()->unlock();
}

}// end of namespace client
}// end of namespace bladestore
