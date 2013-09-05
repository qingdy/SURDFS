/*
 *  Copyright (c) 2012, Sohu R&D
 *  All rights reserved.
 * 
 *  File   name: channelpool.h 
 *  Description: This file defines the class ChannelPool
 * 
 *  Version: 1.0
 *  Author : jianyuli@sohu-inc.com
 *  Date   : 2012-1-12
 */

#ifndef PANDORA_AMFRAME_CHANNEL_POOL_H
#define PANDORA_AMFRAME_CHANNEL_POOL_H

#include <list>
#include <ext/hash_map>
#include "atomic.h"
#include "channel.h"
#include "thread_mutex.h"

namespace pandora
{
#define CHANNEL_CLUSTER_SIZE 25
#define AMFRAME_MAX_TIME (1ll<<62)

class ChannelPool
{

public:
    ChannelPool();

    ~ChannelPool();
    
    Channel *AllocChannel();

    bool FreeChannel(Channel *channel);
    bool AppendChannel(Channel *channel);

    Channel* OfferChannel(uint32_t id);

    Channel* GetTimeoutList(int64_t now);

    bool AppendFreeList(Channel *add_list);

    int GetUseListCount() {
        return use_map_.size();
    }

    void SetExpireTime(Channel *channel, int64_t now); 

private:
    __gnu_cxx::hash_map<uint32_t, Channel*> use_map_;
    std::list<Channel*> cluster_list_;
    CThreadMutex mutex_;

    Channel *free_list_head_;
    Channel *free_list_tail_;
    Channel *use_list_head_;
    Channel *use_list_tail_;
    int max_use_count_;

    static atomic_t channel_id_;
    static atomic_t total_count_;
};

} // namespace pandora

#endif /*PANDORA_AMFRAME_CHANNEL_POOL_H*/
