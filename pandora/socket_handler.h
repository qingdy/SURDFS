/*
 *  Copyright (c) 2012, Sohu R&D
 *  All rights reserved.
 * 
 *  File   name: socket_handler.h 
 *  Description: This file defines the class SocketHandler and 
 *               its derived classes
 * 
 *  Version: 1.0
 *  Author : jianyuli@sohu-inc.com
 *  Date   : 2012-1-12
 */

#ifndef PANDORA_AMFRAME_SOCKET_HANDLER_H
#define PANDORA_AMFRAME_SOCKET_HANDLER_H

#include <string>
#include <vector>
#include "packet.h"
#include "amframe.h"

namespace pandora
{

class SocketHandler
{
protected:
    SocketHandler(AmFrame& amframe): amframe_(amframe)
    {
    }
    AmFrame::EndPoint end_point_;

public:
    virtual ~SocketHandler() {}

    void SetEndPointId(SocketId id)
    {
        end_point_.set_sock_id(id);
    }
    virtual AmFrame::EndPoint& end_point()
    {
        return end_point_;
    }
    AmFrame& amframe() const
    {
        return amframe_;
    }

    virtual void OnClose(int error_code) = 0;

private:
    AmFrame& amframe_;
};

class StreamSocketHandler : public SocketHandler
{
public:
    StreamSocketHandler(AmFrame& amframe):
        SocketHandler(amframe)
    {
    }

    virtual void OnConnected() = 0;

    virtual int DetectPacketSize(const void* data, size_t length) = 0;

    AmFrame::StreamEndPoint& end_point() // override
    {
        return static_cast<AmFrame::StreamEndPoint&>(end_point_);
    }

    virtual void OnReceived(const Packet& packet, void *client_data) = 0;

    virtual void OnTimeout(void *client_data) = 0;

    virtual bool OnSent(Packet* packet)
    {
        return true;
    }

};

class ListenSocketHandler : public SocketHandler
{
public:
    ListenSocketHandler(AmFrame& amframe):
        SocketHandler(amframe)
    {
    }

    virtual StreamSocketHandler* OnAccepted(SocketId id) = 0;

    AmFrame::ListenEndPoint& end_point() // override
    {
        return static_cast<AmFrame::ListenEndPoint&>(end_point_);
    }
};

class LineStreamSocketHandler : public StreamSocketHandler
{
public:
    LineStreamSocketHandler(AmFrame& amframe): StreamSocketHandler(amframe)
    {
    }
    int DetectPacketSize(const void* header, size_t size)
    {
        const char* p = (const char*) memchr(header, '\n', size);
        if (p)
            return p - (const char*)header + 1;
        else
            return 0;
    }
};

} // namespace pandora

#endif // PANDORA_AMFRAME_SOCKET_HANDLER_H
