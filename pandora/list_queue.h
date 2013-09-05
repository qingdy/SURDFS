/*
 * Copyright (c) 2011,  Sohu R&D
 * All rights reserved.
 * 
 * File   name: list_queue.h
 * Description: implement list queue.
 * 
 * Version : 1.0
 * Author  : shiqin@sohu-inc.com
 * Date    : 12-2-3
 *
 */

#ifndef PANDORA_SYSTEM_LISTQUEUE_H
#define PANDORA_SYSTEM_LISTQUEUE_H

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
class ListQueue
{

typedef struct ListNode
{
    Type val_;
    ListNode* next_;
}Node;

public:
    ListQueue();
    ~ListQueue();
    
    // @retval:the current item number in the list.
    int length();
    
    void Enqueue(Type* const m_pin);

    /* get an item from list.
     * @param m_pout [OUTPUT]: the address to get output item.
     * @retval: return false if the list is empty, return true if get the item successfully.
     */
    bool Dequeue(Type* m_pout);
    void Clear();

private:
    CThreadMutex list_mutex_;
    Node* head_;
    Node* tail_;
    int length_;
    DISALLOW_COPY_AND_ASSIGN(ListQueue);
};

}//namespace pandora

#endif//PANDORA_SYSTEM_LISTQUEUE_H
