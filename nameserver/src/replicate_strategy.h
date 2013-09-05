/*
 * Copyright (c) 2012,  Sohu R&D
 * All rights reserved.
 *
 * File   name: replicate_strategy.h
 * Description: define ReplicaStrategy
 *
 * Version : 0.1
 * Author  : yeweimin
 * Date    : 12-5-16
 */

#ifndef BLADESTORE_NAMESERVER_REPLICATE_STRATEGY_H
#define BLADESTORE_NAMESERVER_REPLICATE_STRATEGY_H

#include <vector>
#include <string>
#include "layout_manager.h"

using namespace std;

namespace bladestore 
{
namespace nameserver
{
#define MAX_LOAD_SCORE 1
#define DS_CHOOSE_LEVEL 0.6

class ReplicateStrategy
{
public:
    ReplicateStrategy(BladeLayoutManager &layout_manager);
    ~ReplicateStrategy();

    /*
     * \Used for client to add a new block 
     * \param[in] num_replica : the replication num of this block  
     * \param[in] client_ip : client's ip 
     * \param[out] results : dataservers which has get pipelined
     */
    void choose_target(int16_t num_replica, string client_ip, vector<uint64_t> &results);

    /*
     * \Used for ds to replica some block
     * \param[in] num_replica : the replication num of this block
     * \param[in] writer : dataserver's id
     * \param[in] choosen_ds : dataservers which has choosed
     * \param[in] exclude_ds : Dataservers which should not be considered
     * \param[out] results : dataservers which has get pipelined
     */
    void choose_target(int16_t num_replica, uint64_t writer, vector<uint64_t> &choosen_ds, vector<uint64_t> &results);


    /*
     * \Get pipeline of dataservers
     * \param[in] client_ip : client's ip
     * \param[out] results : dataservers which has get pipelined
     */
    void get_pipeline(string client_ip, vector<uint64_t> &results);

private:
    void get_pipeline(uint64_t writer, vector<uint64_t> &results);  

    uint64_t choose_target(int16_t num_replica, uint64_t writer, vector<uint64_t> &exclude_ds, 
            int16_t max_ds_per_rack, vector<uint64_t> &results);

    uint64_t choose_local_ds(uint64_t writer, vector<uint64_t> &exclude_ds, int16_t max_ds_per_rack, vector<uint64_t> &results);

    uint64_t choose_local_rack_ds(uint64_t writer, vector<uint64_t> &exclude_ds, int16_t max_ds_per_rack, vector<uint64_t> &results);

    uint64_t choose_remote_rack_ds(uint64_t writer, vector<uint64_t> &exclude_ds, int16_t max_ds_per_rack, vector<uint64_t> &results);

    uint64_t choose_random_ds(vector<uint64_t> &exclude_ds, int16_t max_ds_per_rack, vector<uint64_t> &results);

    void choose_random_ds(int16_t num_replica, vector<uint64_t> &exclude_ds, int16_t  max_ds_per_rack, vector<uint64_t> &results);
 
    uint64_t writer_ip_to_id(const string ip);


private:
    BladeLayoutManager &layout_manager_;
};//end of class ReplicateStrategy

}//end of namespace nameserver 
}//end of namespace bladestore 

#endif
