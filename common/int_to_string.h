/*
 * =====================================================================================
 *
 *       Filename:  int_to_string.h
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  03/29/2012 09:34:29 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  ZQ (), 
 *   Organization:  
 *
 * =====================================================================================
 */
#ifndef int_to_string_H
#define int_to_string_H
#include <string>
#include <stdio.h>

using namespace std;
inline string Int32ToString(int32_t a)
{
	char *tmp = new char[1024];
	sprintf(tmp,"%d",a);
	string final_string = tmp;
	delete[] tmp;
	return final_string;
}

inline string Int64ToString(int64_t a)
{
	char *tmp = new char[1024];
	sprintf(tmp,"%ld",a);
	string final_string = tmp;
	delete[] tmp;
	return final_string;
}
#endif
