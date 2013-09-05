/*
 * Copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: client.cpp
 * Description: the class client.
 *
 * Version:1.0
 * Author: guili
 */

#ifndef BLADESTORE_CLIENT_CLIENT_CONF_H
#define BLADESTORE_CLIENT_CLIENT_CONF_H

#include <string>
#include "config.h"

using namespace pandora;
using std::string;

namespace bladestore
{
namespace client
{

class ClientConf
{
public:
    ClientConf();
    ~ClientConf();

    int Init();
    int Load();
    int LoadConfig(string filename);

    string working_dir() { return working_dir_;}
    string home_path() { return home_path_;}
    string log_dir_path() { return log_dir_path_;}
    string nameserver_addr() { return nameserver_addr_;}

    int32_t log_max_file_size() { return log_max_file_size_;}
    int32_t log_max_file_count() { return log_max_file_count_;}
    int32_t cache_max_size() { return cache_max_size_;}
    int32_t buffer_size() { return buffer_size_;}

private:
    const static string config_file_name_;

    Config blade_config_;

    string working_dir_;
    string home_path_;
    string log_dir_path_;
    string nameserver_addr_;

    int32_t log_max_file_size_;
    int32_t log_max_file_count_;
    int32_t cache_max_size_;
    int32_t buffer_size_;
};
}  // end of namespace client
}  // end of namespace bladestore
#endif
