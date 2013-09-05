/**
 * Copyright (c) 2012£¬Sohu R&D
 * All rights reserved.
 *
 * File name: sem.h
 * Description: semaphore interface
 * Date: 2012-2-3
 */

#ifndef __SEMAPHORE_H__
#define __SEMAPHORE_H__

#include <stdint.h>
#include <semaphore.h>

namespace pandora
{

class Semaphore
{
public:
    Semaphore(uint32_t value);
    ~Semaphore();

    bool Wait();
    bool TryWait();
    bool TimedWait(int32_t timeout);
    bool Post();
    int32_t GetValue() const;
    bool need_retry() { return need_retry_; }

private:
    sem_t sem_;
    bool need_retry_;     //Only for checking needs to TryWait again
};

}
#endif
