#ifndef PANDORA_SINGLETON_H
#define PANDORA_SINGLETON_H

#include <stdlib.h>
#include "thread_mutex.h"

namespace pandora
{

template <typename T>
class SingletonBase
{
protected:
    SingletonBase(){}
    ~SingletonBase(){}
public:
    static T& Instance()
    {
        if (!instance_)
        {
            CThreadGuard guard(&lock_);
            if (!instance_)
            {
                instance_ = new T();
                atexit(Destory);
            }
        }

        return *instance_;
    }
private:
    static void Destory()
    {
        if (instance_)
        {
            delete instance_;
            instance_ = NULL;
        }
    }
private:
    static CThreadMutex lock_;
    static T* volatile instance_;
};

template <typename T>
CThreadMutex SingletonBase<T>::lock_;

template <typename T>
T* volatile SingletonBase<T>::instance_;

template <typename T>
class Singleton : public SingletonBase<T>
{
};

}

#endif
