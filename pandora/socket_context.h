/*
 *  Copyright (c) 2012, Sohu R&D
 *  All rights reserved.
 * 
 *  File   name: socket_context.h 
 *  Description: This file defines the class SocketContext
 * 
 *  Version: 1.0
 *  Author : jianyuli@sohu-inc.com
 *  Date   : 2012-1-12
 */

#ifndef PANDORA_AMFRAME_SOCKET_CONTEXT_H
#define PANDORA_AMFRAME_SOCKET_CONTEXT_H

#include <string>
#include <deque>
#include <vector>
#include "socket.h"
#include "stream_socket.h"
#include "listener_socket.h"
#include "packet.h"
#include "packetqueue.h"
#include "socket_handler.h"
#include "channelpool.h"
#include "event_poller.h"
#include "amframe.h"

namespace pandora
{

enum {
    kAMFRAME_CONNECTING = 1,
    kAMFRAME_CONNECTED,
    kAMFRAME_CLOSED,
    kAMFRAME_UNCONNECTED
};

class SocketContext
{
    friend class Transport;
public:
    SocketContext(
        AmFrame* amframe,
        Socket *socket,
        SocketId sock_id,
        const AmFrame::EndPointOptions& options,
        bool is_server
    );
    virtual ~SocketContext();

    void set_event_poller(EventPoller* event_poller)
    {
        event_poller_ = event_poller;
    }

    const AmFrame::EndPointOptions& endpoint_options()
    {
        return endpoint_options_;
    }

    virtual bool HandleReadEvent() = 0;

    virtual bool HandleWriteEvent() = 0;

    /*
     * Check Timeout
     * @param now: current time (unit: us)
     */
    virtual void CheckTimeout(int64_t now) = 0;

    virtual void Close()
    {
    }

    virtual int32_t SendPacket(Packet* packet, 
                               bool need_ack,
                               void* client_data,
                               int32_t timeout) = 0;
    
    virtual SocketHandler* event_handler() const
    {
        return event_handler_;
    }

    SocketId sock_id() const
    {
        return sock_id_;
    }

    int GetFd() const
    {
        return sock_id_.sock_fd_;
    }

    Socket* socket()
    {
        return socket_;
    }

    void EnableWrite(bool writeOn)
    {
        if (event_poller_) {
            event_poller_->SetEvent(GetFd(), true, writeOn);
        }
    }

    int AddRef() 
    {
        return AtomicAdd(&refcount_, 1);
    }

    void SubRef()
    {
        AtomicDec(&refcount_);
    }

    int GetRef()
    {
        return AtomicGet(&refcount_);
    }

    bool IsConnectedState()
    {
        return (state_ == kAMFRAME_CONNECTED || state_ == kAMFRAME_CONNECTING);
    }

    int state() 
    {
        return state_;
    }

    void set_state(int32_t state)
    {
        state_ = state;
    }

    bool in_used()
    {
        return in_used_;
    }

    void set_in_used(bool in_used)
    {
        in_used_ = in_used;
    }

    int64_t last_use_time()
    {
        return last_use_time_;
    }

    void set_is_server(bool is_server)
    {
        is_server_ = is_server;
    }

protected:
    void set_event_handler(SocketHandler* handler)
    {
        event_handler_ = handler;
    }

protected:
    AmFrame* amframe_;
    EventPoller* event_poller_;
    SocketHandler* event_handler_;
    Socket *socket_;
    SocketId sock_id_;
    AmFrame::EndPointOptions endpoint_options_;

    int32_t last_error_;
    int32_t state_;
    atomic_t refcount_;
    bool is_server_;
    bool in_used_;
    int64_t last_use_time_;

private:
    SocketContext *prev_;
    SocketContext *next_;

    SocketContext(const SocketContext&);
    void operator=(const SocketContext&);
};

} // namespace pandora

#endif // PANDORA_AMFRAME_SOCKET_CONTEXT_H
