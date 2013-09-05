/*
* Copyright (c) 2011£¬Sohu R&D
* All rights reserved.
* 
* File   name£ºchar_util.h
* Description: This file defines the class ¡­
* 
* Version £º1.0
* Author  £ºboliang@sohu-inc.com
* Date    £º2012-1-11
*
*/

#ifndef PANDORA_CHAR_UTIL_H
#define PANDORA_CHAR_UTIL_H


namespace pandora{

//Upper all chars in str_from and store this char[]
int32_t CharsToUpper(char *str_from);

//Upper all chars in str_from and store a new char[]
int32_t CharsToUpper(const char *str_from, char *str_to, int32_t size);

//Lower all chars in str_from and store this string
int32_t CharsToLower(char *str_from);

//Lower all chars in str_from and store a new string
int32_t CharsToLower(const char *str_from, char *str_to, int32_t size);

//
int32_t CharsTrimLeft(const char *str_from, char *str_to, int32_t size);

//
int32_t CharsTrimRight(const char *str_from, char *str_to, int32_t size);


//
int32_t CharsTrim(const char *str_from, char *str_to, int32_t size);

/*
//
std::string FormatByteSize(double bytes);

//
int32_t ReplaceString(const std::string &str_from, std::string &str_to,const std::string &str_oldsub, const std::string &str_newsub);

//
int32_t ReplaceString_All(const std::string &str_from, std::string &str_to,const std::string &str_oldsub, const std::string &str_newsub);
*/

}

#endif

