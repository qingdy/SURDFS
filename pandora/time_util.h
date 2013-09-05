/*
* Copyright (c) 2012£¬Sohu R&D
* All rights reserved.
* 
* File   name : time_util.h
* Description : brief wrap to time operation, including get current time,  time format transfer and so on
* 
* Version : 1.0
* Author  : binghan@sohu-inc.com
* Date    : 2012-01-31
*/

#ifndef PANDORA_TIMEUTIL_H
#define PANDORA_TIMEUTIL_H

#include <stdint.h>
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <string.h>

namespace pandora 
{

class TimeUtil 
{
public:
    // Get current time, the return value unit is microsecond
    static int64_t GetTime();
    // Get Monotonic Time, the return value unit is microsecond
    static int64_t GetMonotonicTime(); 
    // Transfer int format to format like 20120201112723
    static char * TimeToStr(time_t t, char *dest);
    // Transfer int format to format like 2012-02-01 11:27:23
    static char * TimeToLocalTimeStr(time_t t, char *dest);
    // Transfer time str like 2012020201112723 to int time whose unit is sec
    static int StrToTime(char *str);
};

}
#endif
