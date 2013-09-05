/*
 *version  : 1.0
 *authro   : chen yunyun
 *date     : 2012-4-18
 *
 */
#ifndef BLADESTORE_COMMON_LOCK_GUARD_H
#define BLADESTORE_COMMON_LOCK_GUARD_H
namespace bladestore
{
namespace common
{
template <class T>
class CLockGuard
{
public:
    CLockGuard(const T& lock, bool block = true) : _lock(lock)
    {
        _acquired = !(block ? _lock.lock() : _lock.tryLock());
    }

    ~CLockGuard()
    {
        _lock.unlock();
    }

    bool Acquired() const
    {
        return _acquired;
    }
    
private:
    const T& _lock;
    mutable bool _acquired;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
