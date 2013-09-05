/*
 *  Copyright (c) 2012, Sohu R&D
 *  All rights reserved.
 * 
 *  File   name: channel.h 
 *  Description: This file defines the class Channel
 * 
 *  Version: 1.0
 *  Author : jianyuli@sohu-inc.com
 *  Date   : 2012-1-12
 */

#ifndef PANDORA_AMFRAME_CHANNEL_H
#define PANDORA_AMFRAME_CHANNEL_H

namespace pandora
{

class Channel
{
    friend class ChannelPool;

public:
    Channel()
    {
        prev_ = NULL;
        next_ = NULL;
        expire_time_ = 0;
    }

    void set_id(uint32_t id)
    {
        id_ = id;
    }

    uint32_t id()
    {
        return id_;
    }

    void set_args(void *args)
    {
        args_ = args;
    }

    void *args()
    {
        return args_;
    }

    void set_expire_time(int64_t expireTime)
    {
        expire_time_ = expireTime;
    }

    int64_t expire_time()
    {
        return expire_time_;
    }

    Channel *next()
    {
        return next_;
    }

private:
    uint32_t id_;
    void *args_;
    int64_t expire_time_;

private:
    Channel *prev_;
    Channel *next_;
};

} //namespance pandora  

#endif //PANDORA_AMFRAME_CHANNEL_H
