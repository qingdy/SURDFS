/*
* Copyright (c) 2012��Sohu R&D
* All rights reserved.
* 
* File   name : mutex.h
* Description : 
* 
* Version : 1.0
* Author  : binghan@sohu-inc.com
* Date    : 2012-02-06
*/

#ifndef PANDORA_MUTEX_H
#define PANDORA_MUTEX_H 

#include <pthread.h>
#include "lock.h"
#include "log.h"

namespace pandora
{

class Mutex
{
public:

    typedef LockT<Mutex> MU_Lock;
    typedef TryLockT<Mutex> MU_TryLock;

    Mutex();
    ~Mutex();

    void Lock() const;
    bool TryLock() const;
    void Unlock() const;
    bool WillUnlock() const;

private:
    Mutex(const Mutex&);
    Mutex& operator=(const Mutex&);

    struct LockState
    {
        pthread_mutex_t* mutex;
    };

    void Unlock(LockState&) const;
    void Lock(LockState&) const;
    mutable pthread_mutex_t mutex_;

    friend class Cond;
};

}//end namespace

#endif
