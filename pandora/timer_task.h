/*
* Copyright (c) 2012£¬Sohu R&D
* All rights reserved.
* 
* File   name : timer.h
* Description : 
* 
* Version : 1.0
* Author  : binghan@sohu-inc.com
* Date    : 2012-02-06
*/

#ifndef PANDORA_TIMER_TASK_H
#define PANDORA_TIMER_TASK_H

#include "handle.h"
#include "shared.h"

namespace pandora
{
/*
 * TimerTask is a template class.
 * It has a virtual method RunTimerTask.
 * User have to extends TimerTask and impl RunTimerTask method if want to use it.
 */
class TimerTask : virtual public Shared
{
public:
    virtual ~TimerTask(){}
    // Task logic that have to run
    virtual void RunTimerTask() = 0;
};

typedef Handle<TimerTask> TimerTaskPtr;

}
#endif
