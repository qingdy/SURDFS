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

#include "client_conf.h"
#include "blade_common_define.h"
#include "log.h"

using namespace bladestore::common;
namespace bladestore
{

namespace client
{

const std::string ClientConf::config_file_name_="../conf/client.conf";

ClientConf::ClientConf()
{
}

ClientConf::~ClientConf()
{
}

int ClientConf::Init()
{
    int ret = Load();
    if (BLADE_SUCCESS == ret) {
        working_dir_ = blade_config_.GetString("client", "working_dir", "/");
        if (working_dir_.empty()) {
            fprintf(stderr, "Client config file error: working_dir is illegal.");
            ret = BLADE_ERROR;
        }

        home_path_ = blade_config_.GetString("client", "home_path", "/");
        if (home_path_.empty()) {
            fprintf(stderr, "Client config file error: home_path is illegal.");
            ret = BLADE_ERROR;
        }

        log_dir_path_ = blade_config_.GetString("client", "log_dir_path", "../log");
        if (log_dir_path_.empty()) {
            fprintf(stderr, "Client config file error: log_dir_path is illegal.");
            ret = BLADE_ERROR;
        }

        log_max_file_size_ = blade_config_.GetInt("client", "log_max_file_size", 100000000);
        if (log_max_file_size_ < 100 * 1024) {
            fprintf(stderr, "Client config file error: log_max_file_size set too small, recommended is %d.", 100 * 1024 * 1024);
            ret = BLADE_ERROR;
        }

        log_max_file_count_  = blade_config_.GetInt("client", "log_max_file_count", 20);
        if (log_max_file_count_ < 2) {
            fprintf(stderr, "Client config file error: log_max_file_count set too small, recommended is %d.", 20);
            ret = BLADE_ERROR;
        }

        cache_max_size_ = blade_config_.GetInt("client", "cache_max_size", 1024);
        if (cache_max_size_ < 256) {
            fprintf(stderr, "Client config file error: cache_max_size set too small, recommended is %d.", 1024);
            ret = BLADE_ERROR;
        }

        buffer_size_ = blade_config_.GetInt("client", "buffer_size", 10485760);  // 10M
        // 1k <= buffer_size <= 100M
        if ((unsigned)buffer_size_ < CLIENT_MIN_BUFFERSIZE || (unsigned)buffer_size_ > CLIENT_MAX_BUFFERSIZE) {
            fprintf(stderr, "Client config file error: buffer_size is illegal, recommended is %d.", 10485760);
            ret = BLADE_ERROR;
        }

        nameserver_addr_ = blade_config_.GetString("nameserver", "nameserver_addr", "");
        if (nameserver_addr_.empty()) {
            fprintf(stderr, "Client config file error: nameserver_addr is illegal.");
            ret = BLADE_ERROR;
        }
    }

    return ret;
}

int ClientConf::Load()
{
    return LoadConfig(config_file_name_);
}

int ClientConf::LoadConfig(std::string filename)
{
    bool ret = blade_config_.Load(filename);
    if(!ret) {
        LOGV(MSG_ERROR, "Load config from file %s failed.", filename.c_str());
        return BLADE_ERROR;
    }
    return BLADE_SUCCESS;
}

}
}
