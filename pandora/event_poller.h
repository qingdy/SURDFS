/*
 *  Copyright (c) 2012, Sohu R&D
 *  All rights reserved.
 * 
 *  File   name: event_poller.h 
 *  Description: This file defines the class EventPoller
 * 
 *  Version: 1.0
 *  Author : jianyuli@sohu-inc.com
 *  Date   : 2012-1-12
 */

#ifndef PANDORA_AMFRAME_EVENT_POLLER_H
#define PANDORA_AMFRAME_EVENT_POLLER_H

namespace pandora
{

#define MAX_SOCKET_EVENTS (256)

enum IoEventMask
{
    kIoEventConnected = 1,
    kIoEventReadable = 2,
    kIoEventWriteable = 4,
    kIoEventAcceptable = 8,
    kIoEventClosed = 16
};

struct IoEvent
{
    int fd;
    unsigned int mask;
    int error_code;
};

// abstract event poller interface
class EventPoller
{
public:
    virtual ~EventPoller() {}

    virtual bool AddEvent(int fd, bool enable_read, bool enable_write) = 0;

    virtual bool SetEvent(int fd, bool enable_read, bool enable_write) = 0;

    virtual bool ClearEventRequest(int fd) = 0;

    virtual int PollEvents(int timeout, IoEvent *ioevents, int count) = 0;

};

} // namespace pandora

#endif // PANDORA_AMFRAME_EVENT_POLLER_H
