#ifndef BLADESTORE_CLIENT_CLIENT_RESPONSE_H
#define BLADESTORE_CLIENT_CLIENT_RESPONSE_H

#include <map>
#include <stdlib.h>
#include <stdint.h>
#include "thread_cond.h"
#include "blade_common_define.h"
#include "blade_packet.h"
#include "atomic.h"
#include "blade_rwlock.h"

namespace bladestore
{
namespace client
{

using namespace bladestore::message;
using namespace pandora;
using namespace std;

class ClientResponse
{
public:
    ClientResponse();
    ClientResponse(int16_t);
    ~ClientResponse();

    int16_t operation() { return operation_;}
    void set_operation(int16_t operation) { operation_ = operation;}
protected:
    int16_t operation_;
};

class ResponseSync : public ClientResponse
{
public:
    ResponseSync();
    ResponseSync(int16_t);
    ResponseSync(int16_t, int32_t);
    ~ResponseSync();

    map<int64_t, BladePacket*>& request_packets() { return request_packets_;}
    map<int64_t, BladePacket*>& response_packets() { return response_packets_;}
    BladePacket* response_packet() { return response_packet_;}
    BladePacket* request_packet() { return request_packet_;}
    int32_t      response_count() { return AtomicGet(&response_count_);}
    int32_t      unit_total_count() { return AtomicGet(&unit_total_count_);}
    bool response_error() { return response_error_;}
    bool is_sync_packet() { return is_sync_packet_;}
    bool wait() { return wait_;}
    CThreadCond& cond() { return cond_;}

    void set_response_packet(BladePacket* packet) { response_packet_ = packet;}
    void set_request_packet(BladePacket* packet) { request_packet_= packet;}
    void set_response_count(const int32_t response_count) 
         { AtomicSet(&response_count_, response_count);}
    void set_unit_total_count(const int32_t unit_total_count)
         { AtomicSet(&unit_total_count_, unit_total_count);}
    void set_response_error(bool response_error) { response_error_ = response_error;}
    void set_is_sync_packet(bool is_empty) { is_sync_packet_ = is_empty;}
    void set_wait(bool wait) { wait_ = wait;}

    int32_t AddResponseCount();
    bool    AddRequestPacket(int64_t seq, BladePacket* packet);
    bool    AddResponsePacket(int64_t seq, BladePacket* packet);
    void    EraseRequestPacket(map<int64_t, BladePacket*>::iterator);
    bool    EraseRequestPacket(const int64_t seq);
    void    EraseResponsePacket(map<int64_t, BladePacket*>::iterator);
private:
    map<int64_t, BladePacket*> request_packets_;//(seq, packet)
    map<int64_t, BladePacket*> response_packets_;//(seq, packet)
    BladePacket* response_packet_;
    BladePacket* request_packet_;
    atomic_t     response_count_;  //response number
    atomic_t     unit_total_count_;//expected response number
    bool response_error_;
    bool is_sync_packet_;
    bool wait_;
    CRWLock rw_request_lock_;
    CRWLock rw_response_lock_;
    CThreadCond cond_; // sync sender check with Amframe I/O threads

    DISALLOW_COPY_AND_ASSIGN(ResponseSync);
};

}//end of namespace client
}//end of namespace bladestore
#endif
