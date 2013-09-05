/*
 *  Copyright (c) 2012, Sohu R&D
 *  All rights reserved.
 * 
 *  File   name: listen_socket_context.h 
 *  Description: This file defines the class ListenSocketContext
 * 
 *  Version: 1.0
 *  Author : jianyuli@sohu-inc.com
 *  Date   : 2012-1-12
 */

#ifndef PANDORA_AMFRAME_LISTEN_SOCKET_CONTEXT_H
#define PANDORA_AMFRAME_LISTEN_SOCKET_CONTEXT_H

#include "socket_context.h"

namespace pandora
{

class AmFrame;

class ListenSocketContext : public SocketContext
{
public:
    ListenSocketContext(
        AmFrame* amframe,
        Socket* socket,
        SocketId sock_id,
        ListenSocketHandler* handler,
        const AmFrame::EndPointOptions& options
    ) : SocketContext(amframe, socket, sock_id, options, true)
    {
        set_event_handler(handler);
    }

    virtual bool HandleReadEvent(); // override

    virtual bool HandleWriteEvent()
    {
        return true;
    }

    virtual void CheckTimeout(int64_t now)
    {
    }

    virtual int32_t SendPacket(Packet* packet, 
                               bool need_ack,
                               void* client_data,
                               int32_t timeout)
    {
        return 0;
    }

    virtual ListenSocketHandler* event_handler() const
    {
        return static_cast<ListenSocketHandler*>(
            SocketContext::event_handler());
    }
};

} // namespace pandora

#endif // PANDORA_AMFRAME_LISTEN_SOCKET_CONTEXT_H
