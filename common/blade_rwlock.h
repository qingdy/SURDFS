/*
 *version  :  1.0
 *author   :  chen yunyun
 *date     :  2012-4-18
 *
 */
#ifndef BLADESTORE_COMMON_RW_LOCK_H
#define BLADESTORE_COMMON_RW_LOCK_H

#include <pthread.h>

#include "blade_lock_guard.h"

namespace bladestore
{
namespace common
{

enum ELockMode
{
    NO_PRIORITY,
    WRITE_PRIORITY,
    READ_PRIORITY
};

class CRLock
{
public:
    CRLock(pthread_rwlock_t* lock) : _rlock(lock) {}
    ~CRLock() {}

    int lock() const;
    int tryLock() const;
    int unlock() const;
    
private:
    mutable pthread_rwlock_t* _rlock;
};

class CWLock
{
public:
    CWLock(pthread_rwlock_t* lock) : _wlock(lock) {}
    ~CWLock() {}
    
    int lock() const;
    int tryLock() const;
    int unlock() const;
    
private:
    mutable pthread_rwlock_t* _wlock;
};    

class CRWLock 
{
public:
    CRWLock(ELockMode lockMode = NO_PRIORITY);
    ~CRWLock();

    CRLock* rlock() const {return _rlock;}
    CWLock* wlock() const {return _wlock;} 

private:
    CRLock* _rlock;
    CWLock* _wlock;
    pthread_rwlock_t _rwlock;
};

class CRWSimpleLock
{
public:
    CRWSimpleLock(ELockMode lockMode = NO_PRIORITY);
    ~CRWSimpleLock();
    
    int rdlock();
    int wrlock();
    int tryrdlock();
    int trywrlock();
    int unlock();
    
private:    
    pthread_rwlock_t _rwlock;
};

class CRLockGuard
{
public:
    CRLockGuard(const CRWLock& rwlock, bool block = true) : _guard((*rwlock.rlock()), block) {}
    ~CRLockGuard(){}

    bool acquired()
    {
        return _guard.Acquired();
    }

private:
    CLockGuard<CRLock> _guard;
};

class CWLockGuard
{
public:
    CWLockGuard(const CRWLock& rwlock, bool block = true) : _guard((*rwlock.wlock()), block) {}
    ~CWLockGuard(){}

    bool acquired()
    {
        return _guard.Acquired();
    }

private:
    CLockGuard<CWLock> _guard;
};

}//end of namespace common
}//end of namespace bladestore 
#endif
