/*
 *  Copyright (c) 2012, Sohu R&D
 *  All rights reserved.
 * 
 *  File   name: packetqueue.h 
 *  Description: This file defines the class PacketQueue
 * 
 *  Version: 1.0
 *  Author : jianyuli@sohu-inc.com
 *  Date   : 2012-1-12
 */

#ifndef PANDORA_AMFRAME_PACKET_QUEUE_H
#define PANDORA_AMFRAME_PACKET_QUEUE_H

#include "packet.h"

namespace pandora
{

class PacketQueue {
public:
    PacketQueue();

    ~PacketQueue();

    Packet *Pop();

    void Clear();

    void Push(Packet *packet);

    int size()
    {
        return size_;
    }

    bool Empty()
    {
        return (size_ == 0);
    }

    /*
     * move the packets in this queue to other queue
     */
    void MoveTo(PacketQueue *destQueue);

    /*
     * get the timeout packets
     */
    Packet *GetTimeoutList(int64_t now);

    Packet *GetPacketList()
    {
        return head_;
    }

    Packet *head()
    {
        return head_;
    }

    Packet* tail()
    {
        return tail_;
    }
protected:
    Packet *head_;
    Packet *tail_;
    int size_;
};

}

#endif

