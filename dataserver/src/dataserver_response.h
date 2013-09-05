/*
 * Sohu R&D  2012
 *
 * File Description:
 *      The client data class used by write related operation
 *
 * Author   : @YYY
 * Version  : 1.0
 * Date     : 2012-06-01
 */

#ifndef BLADESTORE_DATASERVER_RESPONSE_H
#define BLADESTORE_DATASERVER_RESPONSE_H 1

#include "atomic.h"
#include "amframe.h"
#include "socket_context.h"
#include "thread_cond.h"

namespace bladestore
{
namespace dataserver
{
using pandora::AmFrame;
using pandora::atomic_t;
using pandora::CThreadCond;
using bladestore::message::BladePacket;

class DataServerResponse
{
public:
    DataServerResponse() { operation_ = 0;}
    DataServerResponse(int16_t operation) { operation_ = operation;}
    virtual ~DataServerResponse() {}
    
    void    set_operation(int16_t operation) { operation_ = operation;}
    int16_t operation() { return operation_;}

protected:
    int16_t operation_;
};

/*------------------------for client-------------------*/
//for writepipeline and  writepacket
class WriteResponse : public DataServerResponse
{
public:
    WriteResponse();
    WriteResponse(int16_t operation, AmFrame::StreamEndPoint end_point,
                  uint32_t channel_id, uint64_t peer_id);
    virtual ~WriteResponse();

    AmFrame::StreamEndPoint end_point() { return end_point_;}
    uint32_t channel_id() { return channel_id_;}

    int32_t error() { return AtomicGet(&error_);}
    uint64_t peer_id() {return peer_id_;}
    int32_t AddResponseCount();
    int32_t GetResponseCount() { return AtomicGet(&response_count_);}
    void    set_error(int32_t error) { AtomicSet(&error_, error);}//如果出错的话讲error的值设为1
    void set_peer_id(uint64_t peer_id)
    {
        peer_id_ = peer_id;
    }

private:
    AmFrame::StreamEndPoint end_point_;//end point to pre ds
    uint32_t channel_id_;//channel_id to pre node(ds or client)
    atomic_t response_count_;//only when response_count = 2 ,ds will send writepacketreply to pre node(dataserver or client)
    atomic_t    error_;//error=0:packets processed and received success; error=1:packet time out or error occured while handling packet
    uint64_t peer_id_;//发起连接方的id，防止连接断了之后继续发包。

    DISALLOW_COPY_AND_ASSIGN(WriteResponse);
};

class ClientTask
{
public:
    ClientTask();
    ClientTask(BladePacket * packet, WriteResponse * write_response);
    virtual ~ClientTask();

    BladePacket    * packet() { return packet_;}
    WriteResponse  * write_response() { return write_response_;}

private:
    BladePacket    * packet_;
    WriteResponse  * write_response_;

    DISALLOW_COPY_AND_ASSIGN(ClientTask);
};

/*------------------------for ns-------------------*/
//for replicate use
class SyncResponse : public DataServerResponse
{
public:
    SyncResponse(int16_t operation);
    SyncResponse(int16_t operation, int32_t base_count);
    virtual ~SyncResponse();

    int32_t      AddResponseCount();
    bool         response_ok() { return response_ok_;}
    int32_t      response_count() { return AtomicGet(&response_count_);}
    int32_t      error() { return AtomicGet(&error_);}
    int32_t      base_count() {return AtomicGet(&base_count_);}
    bool         wait() { return wait_;}
    CThreadCond& cond() { return cond_;} 
    void         set_response_ok(bool response_ok) { response_ok_ = response_ok;}
    void         set_error(int32_t error) { AtomicSet(&error_, error);}
    void         set_base_count(int32_t base_count) { AtomicSet(&base_count_, base_count);}
    void         set_wait(bool wait) { wait_ = wait;}

private:
    bool        response_ok_;
    atomic_t    response_count_;//num of packets time out or reply success
    atomic_t    error_;//error=0:packets received success error=1:packet time out or error occured while handling packet
    atomic_t    base_count_;//num of packets need reply
    bool        wait_;
    CThreadCond cond_;

    DISALLOW_COPY_AND_ASSIGN(SyncResponse);
};

class NSTask
{
public:
    NSTask();
    NSTask(BladePacket * packet, SyncResponse * sync_response);
    virtual ~NSTask();

    BladePacket    * packet() { return packet_;}
    SyncResponse   * sync_response() { return sync_response_;}

private:
    BladePacket    * packet_;
    SyncResponse   * sync_response_;

    DISALLOW_COPY_AND_ASSIGN(NSTask);
};

}//end of namespace message
}//end of namespace dataserver

#endif
