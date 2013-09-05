/*
* Copyright (c) 2011£¬Sohu R&D
* All rights reserved.
* 
* File   name£ºstring_util.h
* Description: This file defines the class ¡­
* 
* Version £º1.0
* Author  £ºboliang@sohu-inc.com
* Date    £º2012-1-11
*
*/

#ifndef PANDORA_STRING_UTIL_H
#define PANDORA_STRING_UTIL_H

#include <string>
#include <vector>

namespace pandora{

//Upper all chars in str_from and store this string
int32_t StringToUpper(std::string &str_from);

//Upper all chars in str_from and store a new string
int32_t StringToUpper(const std::string &str_from, std::string &str_to);

//Lower all chars in str_from and store this string
int32_t StringToLower(std::string &str_from);

//Lower all chars in str_from and store a new string
int32_t StringToLower(const std::string &str_from, std::string &str_to);

//
int32_t StringTrimLeft(std::string &str_from);

//
int32_t StringTrimLeft(const std::string &str_from, std::string &str_to);

//
int32_t StringTrimRight(std::string &str_from);

//
int32_t StringTrimRight(const std::string &str_from, std::string &str_to);

//
int32_t StringTrim(std::string &str_from);

//
int32_t StringTrim(const std::string &str_from, std::string &str_to);

//
int32_t SpiltString(const std::string &str_from, const char* delim, std::vector<std::string> &vec_string);

//
std::string FormatByteSize(double bytes);

//
int32_t ReplaceString(const std::string &str_from, std::string &str_to,const std::string &str_oldsub, const std::string &str_newsub);

//
int32_t ReplaceString_All(const std::string &str_from, std::string &str_to,const std::string &str_oldsub, const std::string &str_newsub);


}

#endif

