/*
* Copyright (c) 2012£¬Sohu R&D
* All rights reserved.
* 
* File   name : thread_mutex.h
* Description : brief wrap to linux thread lock
* 
* Version : 1.0
* Author  : binghan@sohu-inc.com
* Date    : 2012-01-18
*/

#ifndef PANDORA_SYSTEM_THREAD_THREAD_MUTEX_H
#define PANDORA_SYSTEM_THREAD_THREAD_MUTEX_H

#include <pthread.h>
#include <assert.h>

namespace pandora 
{

class CThreadMutex 
{
public:
    CThreadMutex();
    ~CThreadMutex();

    void Lock();
    int Trylock();
    void Unlock();

protected:
    pthread_mutex_t mutex_;
};

class CThreadGuard
{
public:
    CThreadGuard(CThreadMutex * thread_mutex);
    ~CThreadGuard();

private:
    CThreadMutex * thread_mutex_;
};

}   //end of namespace

#endif /*PANDORA_SYSTEM_THREAD_THREAD_MUTEX_H*/
