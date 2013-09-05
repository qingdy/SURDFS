/*
 *  Copyright (c) 2012, Sohu R&D
 *  All rights reserved.
 * 
 *  File   name: epoll_event_poller.h 
 *  Description: This file defines the class EpollEventPoller
 * 
 *  Version: 1.0
 *  Author : jianyuli@sohu-inc.com
 *  Date   : 2012-1-12
 */

#ifndef PANDORA_AMFRAME_EPOLL_EVENT_POLLER_H
#define PANDORA_AMFRAME_EPOLL_EVENT_POLLER_H

#include <sys/epoll.h>
#include <fcntl.h>
#include "event_poller.h"

namespace pandora
{

// EventPoller using linux epoll
class EpollEventPoller : public EventPoller
{
public:
    EpollEventPoller();
    virtual ~EpollEventPoller();

    virtual bool AddEvent(int fd, bool enable_read, bool enable_write);
    virtual bool SetEvent(int fd, bool enable_read, bool enable_write);
    virtual bool ClearEventRequest(int fd);
    virtual int PollEvents(int timeout, IoEvent *ioevents, int count);
private:
    int epoll_fd_;
};

} // namespace pandora

#endif // PANDORA_AMFRAME_EPOLL_EVENT_POLLER_H
