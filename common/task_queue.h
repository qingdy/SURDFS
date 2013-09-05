/************************************************************
 * Copyright Sohu R&D  2012
 *
 * Author: @landyliu
 * Description:  TaskQueue basede on stl queue, multi-thread safe
 * Version: 1.0
 *************************************************************/

#ifndef BLADESTORE_TASK_QUEUE_H
#define BLADESTORE_TASK_QUEUE_H 

#include <stdint.h>
#include <stdbool.h>
#include <semaphore.h>
#include <pthread.h>
#include <errno.h>
#include <queue>

namespace bladestore
{
namespace common
{

template<typename T>
class TaskQueue
{
public:
    TaskQueue() : max_node_number_(0) {}
    ~TaskQueue() {}
    bool Init(uint32_t max_node_number);

    /**
     *	永久等待直到Push成功
     *	@args
     *		1> [pTask] <in> 插入节点的指针
     *	@return
     *		0成功 -1失败
     */
    bool WaitTillPush(const T& node);
    /**
     *	永久等待直到Pop成功
     *	@args
     *		1> [ppTask] <out> 获取节点指针的指针
     *	@return
     *		0成功 -1失败
     */
    bool WaitTillPop(T& Node);

    /**
     *	等待一段时间直到Push成功
     *	@args
     *		1> [pTask] <in> 插入节点的指针
     *		2> [usec]  <in> 等待时间 ，微秒
     *	@return
     *		0成功 -1失败
     */
    bool WaitTimePush(const T& Node, uint32_t usec);

    /**
     *	等待一段时间直到Pop成功
     *	@args
     *		1> [ppTask] <out> 获取节点指针的指针
     *		2> [usec]  <in> 等待时间 ，微秒
     *	@return
     *		0成功 -1失败
     */
    bool WaitTimePop(T& Node, uint32_t usec);

    /**
     *	队列是否为空
     *	@return
     *		true>为空, false> 不为空
     */
    bool IsEmpty() const;

    /**
     *	队列节点个数
     *	@return
     *		队列节点个数
     */
    uint32_t Size() const;

    /**
     *	队列节点最多可容纳的节点数
     *	@return
     *		队列最大容量
     */
    uint32_t max_node_number() const { return max_node_number_; }

private:
    std::queue<T> queue_;
    //存在节点的个数
    sem_t exist_nodes_;
    //空节点的个数
    sem_t empty_nodes_; 
    //队列互斥锁
    pthread_mutex_t lock_;
    //队列节点最大个数
    uint32_t max_node_number_;
};

/********************************************************************
 @para n 
    [IN]  指定队列的最大支撑容量
 @return
    true> 成功，false> 失败
 *********************************************************************/
template<typename T>
bool TaskQueue<T>::Init(uint32_t n)
{
    int result = 0;
    max_node_number_ = n;
    result = sem_init(&exist_nodes_, 0, 0);
    if (result == -1)
        return false;
    result = sem_init(&empty_nodes_, 0, max_node_number_);
    if (result == -1)
        return false;

    result = pthread_mutex_init(&lock_, NULL);
    if (result == -1)
        return false;

    return true;
}

/********************************************************************
 功能描述: 永久等待直到Push成功
 输入参数: pTask> 插入节点的指针
 输出参数:
 返   回 : true> 成功，false> 失败
 *********************************************************************/
template<typename T>
bool TaskQueue<T>::WaitTillPush(const T& node)
{
    int flag = 0;
    while (1) {
        flag = sem_wait(&empty_nodes_);
        if (-1 == flag) {
            if (errno == EINTR)
                continue;
            else
                return false;
        } else 
            break;
    }

    flag = pthread_mutex_lock(&lock_);
    if (flag == -1) {
        pthread_mutex_unlock(&lock_);
        return false;
    }
    queue_.push(node);
    flag = sem_post(&exist_nodes_);
    if (flag == -1) {
        pthread_mutex_unlock(&lock_);
        return false;
    }
    flag = pthread_mutex_unlock(&lock_);
    if (flag == -1) {
        pthread_mutex_unlock(&lock_);
        return false;
    }
    return true;
}

/********************************************************************
 功能描述: 永久等待直到Pop成功
 输入参数:
 输出参数: Node> 获取节点指针的指针
 返   回 : true> 成功，false> 失败
 *********************************************************************/
template<typename T>
bool TaskQueue<T>::WaitTillPop(T& node)
{
    int flag = 0;
    while (1) {
        flag = sem_wait(&exist_nodes_);
        if (-1 == flag) {
            if (errno == EINTR)
                continue;
            else
                return false;
        } else
            break;
    }

    flag = pthread_mutex_lock(&lock_);
    if (flag == -1) {
        pthread_mutex_unlock(&lock_);
        return false;
    }
    node = queue_.front();
    queue_.pop();
    flag = sem_post(&empty_nodes_);
    if (flag == -1) {
        pthread_mutex_unlock(&lock_);
        return false;
    }
    flag = pthread_mutex_unlock(&lock_);
    if (flag == -1) {
        pthread_mutex_unlock(&lock_);
        return false;
    }

    return true;
}

/********************************************************************
 功能描述: 等待一段时间直到Push成功
 输入参数: pTask> 插入节点的指针
 usec> 等待时间 ，微秒
 输出参数:
 返   回 : true> 成功，false> 失败
 *********************************************************************/
template<typename T>
bool TaskQueue<T>::WaitTimePush(const T& node, uint32_t usec)
{
    struct timespec ts;
    ts.tv_sec = time(NULL);
    ts.tv_nsec = 0;
    if (usec >= 1000000) {
        ts.tv_sec += usec / 1000000;
        ts.tv_nsec = (usec % 1000000) * 1000;
    } else
        ts.tv_nsec = usec * 1000;

    int flag = 0;
    while (1) {
        flag = sem_timedwait(&empty_nodes_, &ts);
        if (-1 == flag) {
            if (errno == EINTR)
                continue;
            else if (errno == ETIMEDOUT)
                return false;
            else
                return false;
        } else
            break;
    }

    flag = pthread_mutex_lock(&lock_);
    if (flag == -1) {
        pthread_mutex_unlock(&lock_);
        return false;
    }
    queue_.push(node);
    flag = sem_post(&exist_nodes_);
    if (flag == -1) {
        pthread_mutex_unlock(&lock_);
        return false;
    }
    flag = pthread_mutex_unlock(&lock_);
    if (flag == -1) {
        pthread_mutex_unlock(&lock_);
        return false;
    }

    return true;
}

/********************************************************************
 功能描述: 等待一段时间直到Pop成功
 输入参数: usec> 等待时间，微秒
 输出参数: ppTask> 获取节点指针的指针
 返   回 : true>成功，false>失败
 *********************************************************************/
template<typename T>
bool TaskQueue<T>::WaitTimePop(T& node, uint32_t usec)
{
    struct timespec ts;
    ts.tv_sec = time(NULL);
    ts.tv_nsec = 0;
    if (usec >= 1000000) {
        ts.tv_sec += usec / 1000000;
        ts.tv_nsec = (usec % 1000000) * 1000;
    } else
        ts.tv_nsec = usec * 1000;

    int flag = 0;
    while (1) {
        flag = sem_timedwait(&exist_nodes_, &ts);
        if (-1 == flag) {
            if (errno == EINTR)
                continue;
            else if (errno == ETIMEDOUT)
                return false;
            else
                return false;
        } else
            break;
    }

    flag = pthread_mutex_lock(&lock_);
    if (flag == -1) {
        pthread_mutex_unlock(&lock_);
        return false;
    }
    node = queue_.front();
    queue_.pop();
    flag = sem_post(&empty_nodes_);
    if (flag == -1) {
        pthread_mutex_unlock(&lock_);
        return false;
    }
    flag = pthread_mutex_unlock(&lock_);
    if (flag == -1) {
        pthread_mutex_unlock(&lock_);
        return false;
    }

    return true;
}

/********************************************************************
 功能描述: 检查队列是否为空
 输入参数:
 输出参数:
 返   回 : true> 为空，false> 不为空
 *********************************************************************/
template<typename T>
bool TaskQueue<T>::IsEmpty() const
{
    return queue_.empty();
}

/********************************************************************
 功能描述: 获取队列当前元素的个数
 输入参数:
 输出参数:
 返   回 : 队列当前元素个数
 *********************************************************************/
template<typename T>
uint32_t TaskQueue<T>::Size() const
{
    return (uint32_t) queue_.size();
}
}//end of namespace common
} //end of namespace bladestore
#endif

