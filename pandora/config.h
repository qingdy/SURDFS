/*
 * Copyright (c) 2011,  Sohu R&D
 * All rights reserved.
 * 
 * File   name: config.h
 * Description: config class; implement read and write for config files.
 * 
 * Version : 1.0
 * Author  : shiqin@sohu-inc.com
 * Date    : 12-1-13
 *
 */

#ifndef PANDORA_CONFIG_H
#define PANDORA_CONFIG_H

#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName)\
                TypeName(const TypeName&);\
            void operator=(const TypeName&)
#endif

#include <stdio.h>
#include <stdlib.h>
#include <map>
#include <vector>
#include <iterator>
#include <string>
using namespace std;

namespace pandora
{

class Config
{
public:
    Config() {};
    ~Config() {};
    
    bool Load(const string& m_path);

    // @param m_defalut: [input], the default value to retrun in case the key is not found. 
    string GetString(const string& m_section,  const string& m_key,  const string& m_default = "");
    int64_t GetInt(const string& m_section,  const string& m_key,  const int64_t m_default = 0);
    float GetFloat(const string& m_section,  const string& m_key,  const float m_default = 0.0);

    /*  
     * @param m_return_lists: [OUTPUT], store the lists of string tokens.
     * @return int: return the size of return list. 
     */
    int GetStringLists(const string& m_section,  const string& m_key,  vector<string>& m_return_lists,  const string& m_delim = ";");

    /*  
     * @param m_return_lists: [OUTPUT], store the lists of int tokens.
     * @return int: return the size of return list. 
     */
    int GetIntLists(const string& m_section,  const string& m_key,  vector<int>& m_return_lists,  const string& m_delim = ";");

    /* 
     * @param m_create: [input], decide if the key should be create when it is not found.
     * @return bool: return false if cannot find the key, and m_create is set as false.
     */
    bool SetString(const string& m_section, const string& m_key, const string& m_value, bool m_create = true);
    bool SetInt(const string& m_section, const string& m_key, const int& m_value, bool m_create = true);
    bool SetFloat(const string& m_section, const string& m_key, const float& m_value, bool m_create = true); 

    /*
     * @param m_keys: [OUTPUT], store the return name of keys in special section.
     * @return int: return the number of keys.
     */     
    int GetKeys(const string& m_section, vector<string>& m_keys);
    
    /*
     * @param m_sections: [OUTPUT], store the return name of sections.
     * @return int: return the number of sections.
     */     
    int GetSections(vector<string>& m_sections);

    string ToString();

private:
    map<string, map<string, string> > config_items_;
    inline string IntToStr(const int m_in);
    inline string FloatToStr(const float m_in);
    inline string Trim(string& m_str);
    DISALLOW_COPY_AND_ASSIGN(Config);
};

}//namespace pandora

#endif//PANDORA_CONFIG_H
