/*
 *  Copyright (c) 2012, Sohu R&D
 *  All rights reserved.
 * 
 *  File   name: packet.h 
 *  Description: This file defines the class Packet
 * 
 *  Version: 1.0
 *  Author : jianyuli@sohu-inc.com
 *  Date   : 2012-1-12
 */

#ifndef PANDORA_AMFRAME_PACKET_H
#define PANDORA_AMFRAME_PACKET_H

#include <stdlib.h>
#include <string.h>
#include "channel.h"
#include "time_util.h"

namespace pandora
{

class Packet
{
    friend class PacketQueue;

public:
    Packet():
        channel_id_(0),
        expire_time_(0),
        channel_(NULL),
        own_buffer_(false),
        content_(NULL),
        length_(0)
    {
    }

    virtual ~Packet()
    {
        DeleteContent();
    }

    void set_channel_id(uint32_t chid)
    {
        channel_id_ = chid;
    }

    uint32_t channel_id() const
    {
        return channel_id_;
    }

    int64_t expire_time() const
    {
        return expire_time_;
    }

    void set_expire_time(int milliseconds)
    {
        if (milliseconds == 0) {
            milliseconds = 1000 * 86400;
        }

        expire_time_ = TimeUtil::GetTime() +
            static_cast<int64_t>(milliseconds) * static_cast<int64_t>(1000);
    }

    void set_channel(Channel *channel)
    {
        if (channel) {
            channel_ = channel;
            channel_id_ = channel->id();
        }
    }

    Channel *channel() const
    {
        return channel_;
    }

    // @brief packet has its own buffer
    void  SetContent(const void* content, size_t length)
    {
        DeleteContent();
        content_ = malloc(length);
        ::memcpy(content_, content, length);
        length_ = length;
        own_buffer_ = true;
    }

    // @brief packet doesn't have its own buffer
    void  SetContentPtr(void* content, size_t length, bool own_buffer = false)
    {
        DeleteContent();
        content_ = content;
        length_ = length;
        own_buffer_ = own_buffer;
    }

    void  DeleteContent()
    {
        if (content_ != NULL)
        {
            if (own_buffer_)
                free(content_);
            content_ = NULL;
        }
        own_buffer_ = false;
        length_ = 0;
    }

    uint8_t* content()
    {
        if (content_ != NULL)
        {
            return (uint8_t*)(content_);
        }
        return NULL;
    }

    const uint8_t* content() const
    {
        if (content_ != NULL)
        {
            return (const uint8_t*)(content_);
        }
        return NULL;
    }

    size_t length() const
    {
        return length_;
    }

    Packet* next() const {
        return next_;
    }

    void Free() {
        delete this;
    }

private:
    Packet(const Packet&);
    Packet& operator=(const Packet&);
private:
    uint32_t channel_id_;
    int64_t expire_time_;
    Channel *channel_;
    bool own_buffer_;
protected:
    void* content_;
    size_t length_;
public:
    Packet* next_;
};

} // namespace pandora

#endif // PANDORA_AMFRAME_PACKET_H
