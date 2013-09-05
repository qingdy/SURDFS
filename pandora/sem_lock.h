/**
 * Copyright (c) 2012£¬Sohu R&D
 * All rights reserved.
 *
 * File name: sem_lock.h
 * Description: semaphore interface
 * Date: 2012-2-10
 */

#ifndef __SEM_LOCK_H__
#define __SEM_LOCK_H__

#include <stdint.h>

namespace pandora {

class SemaphoreLock
{
public:
    SemaphoreLock();
    int32_t InitSem(int32_t key);
    int32_t GetSemId() const
    {
        if (!initialize_)
            return -1;

        return sem_id_;
    }

    int32_t GetSemKey() const
    {
        if (!initialize_)
            return -1;

        return key_id_;
    }

    bool Acquire();
    bool Release();
    bool IsLock() const;

    explicit SemaphoreLock(int32_t sem_id)
    {
        initialize_ = true;
        sem_id_     = sem_id;
    }

private:
    bool initialize_;
    int32_t  sem_id_;
    int32_t  key_id_;
};

}
#endif
