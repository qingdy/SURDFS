/*
 * Copyright (c) 2011,  Sohu R&D
 * All rights reserved.
 * 
 * File   name: circle_list.h
 * Description: implement circle list.
 * 
 * Version : 1.0
 * Author  : shiqin@sohu-inc.com
 * Date    : 12-2-3
 *
 */

#ifndef PANDORA_SYSTEM_CIRCLEQUEUE_H
#define PANDORA_SYSTEM_CIRCLEQUEUE_H

#include <string.h>
#include "thread_mutex.h"

namespace pandora
{

#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName)\
                TypeName(const TypeName&);\
            void operator=(const TypeName&)
#endif

template <class Type>
class CircleQueue
{
public:
    CircleQueue(int m_capacity);
    ~CircleQueue();
    
    int capacity();
    // @retval:the current item number in the list.
    int length();

    /* insert an item into list.
     * @retval: return false if the list is full, return true if the insert success.
     */ 
    bool Enqueue(Type* const m_pin);
    
    /* get an item from list.
     * @param m_pout [OUTPUT]: the address to get output item.
     * @retval: return false if the list is empty, return true if get the item successfully.
     */
    bool Dequeue(Type* m_pout);
    void Clear();

private:
    Type* buffer_;
    int capacity_;
    int head_;
    int tail_;
    CThreadMutex list_mutex_;
    DISALLOW_COPY_AND_ASSIGN(CircleQueue);
};

}//namespace pandora

#endif//PANDORA_CIRCLEQUEUE_H
