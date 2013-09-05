/*
* Copyright (c) 2012£¬Sohu R&D
* All rights reserved.
* 
* File   name : thread_cond.h
* Description : linux thread condition wrap
* 
* Version : 1.0
* Author  : binghan@sohu-inc.com
* Date    : 2012-01-18
*/

#ifndef PANDORA_SYSTEM_THREAD_THREAD_COND_H
#define PANDORA_SYSTEM_THREAD_THREAD_COND_H

#include <sys/time.h>
#include <stdint.h>
#include "thread_mutex.h"

namespace pandora 
{
/*
 * linux thread condition wrap
 **/
class CThreadCond : public CThreadMutex 
{
public:
    CThreadCond();
    ~CThreadCond();
    /**
     * wait up for the signal
     * @param  milliseconds : overtime time(unit: ms), 0 means wait forever
     */
    bool Wait(int milliseconds = 0);
    /**
     * wake up one
     */
    void Signal();
    /**
     * wake up all
     */
    void Broadcast();
private:
    pthread_cond_t cond_;
};

}

#endif /*PANDORA_SYSTEM_THREAD_THREAD_COND_H*/
