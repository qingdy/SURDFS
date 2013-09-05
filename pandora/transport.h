/*
 *  Copyright (c) 2012, Sohu R&D
 *  All rights reserved.
 * 
 *  File   name: transport.h 
 *  Description: This file defines the class Transport
 * 
 *  Version: 1.0
 *  Author : jianyuli@sohu-inc.com
 *  Date   : 2012-1-12
 */

#ifndef PANDORA_AMFRAME_TRANSPORT_H
#define PANDORA_AMFRAME_TRANSPORT_H

#include <list>
#include "cthread.h"
#include "socket_context.h"
#include "epoll_event_poller.h"

namespace pandora
{

typedef EpollEventPoller EventPollerType;

class Transport: public Runnable
{
public:
    Transport(size_t max_fd_value = 0x10000);
    ~Transport();

    bool start();

    bool stop();

    bool wait();

    void Run(CThread *thread, void *arg);

    bool* GetStop();

    void AddSocketContext(SocketContext *context, bool read_on, bool write_on);

    void RemoveSocketContext(SocketContext *context);

    SocketContext* GetSocketContext(int32_t fd);

    void ClearSocketContext(int32_t fd);

private:
    void EventLoop(EventPollerType *event_poller);

    void TimeoutLoop();
    
    void Destory();

private:
    std::vector<SocketContext*> socket_contexts_;
    EventPollerType event_poller_;

    CThread read_write_thread_;
    CThread timeout_thread_;
    bool stop_;

    // The socket contexts list to be deleted
    SocketContext *context_del_list_head_, *context_del_list_tail_;
    // The in use socket contexts list
    SocketContext *context_list_head_, *context_list_tail_;
    // Whether in use socket list has been changed
    bool context_list_changed_;
    int32_t context_list_count_;
    atomic64_t context_list_recycle_;  // for debug
    CThreadMutex contexts_mutex_;
};

} // namespace pandora

#endif // PANDORA_AMFRAME_TRANSPORT_H
