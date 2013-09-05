/*
* Copyright (c) 2012£¬Sohu R&D
* All rights reserved.
* 
* File   name : thread.h
* Description : wrap to linux thread
* 
* Version : 1.0
* Author  : binghan@sohu-inc.com
* Date    : 2012-02-01
*/

#ifndef PANDORA_CTHREAD_H
#define PANDORA_CTHREAD_H

#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <pthread.h>
#include "time_wrap.h"

namespace pandora
{
class Runnable;

class CThread 
{
public:
    CThread();
    int Start(Runnable * r, void * a) ;
    // Wait for the thread exit
    int Join();
    // Stop execute current thread obj, and execute another thread obj
    static void Yield();
    // Suspend the thread, and time is decided by param timeout
    static void Ssleep(const pandora::Time& timeout);
    // Get Runnable obj
    Runnable * GetRunnable() const;
    // Get the call back arguments
    void * GetArgs() const;
    // Get thread id
    pthread_t GetThreadId() const;
    // The call back function of the thread
    static void * Hook(void * arg);

private:
    pthread_t tid_;      
    Runnable * runnable_;
    void * args_;
};

class Runnable 
{
public:
    virtual ~Runnable(){}
    virtual void Run(CThread * thread, void * arg) = 0;
};

};

#endif /*PANDORA_THREAD_H*/
