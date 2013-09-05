#include <assert.h>
#include <set>
#include <algorithm>
#include <utility>
#include "replicate_strategy.h"
#include "blade_net_util.h"
#include "blade_common_define.h"
#include "blade_rwlock.h"

using namespace bladestore::common;
using bladestore::common::BladeNetUtil;

namespace bladestore
{
namespace nameserver
{

ReplicateStrategy::ReplicateStrategy(BladeLayoutManager &layout_manager) : layout_manager_(layout_manager)
{

}

ReplicateStrategy::~ReplicateStrategy()    
{
}

void ReplicateStrategy::choose_target(int16_t num_replica, string client_ip, vector<uint64_t> &results)
{
    if (0 == num_replica)
        return;

    vector<uint64_t> choosen_ds; 
    uint64_t writer = writer_ip_to_id(client_ip);    
    LOGV(LL_DEBUG, "client_ip : %s , writer : %ld", client_ip.c_str(), writer);
    choose_target(num_replica, writer, choosen_ds, results); 
    LOGV(LL_DEBUG, "*****************************choose_target succeed!");
}

void ReplicateStrategy::choose_target(int16_t num_replica, uint64_t writer, vector<uint64_t> &choosen_ds, vector<uint64_t> &results)
{
    if (0 == num_replica)
        return;

    //get total dataservers' number
    int32_t ds_num = layout_manager_.get_alive_ds_size(); 
    LOGV(LL_DEBUG, "******************************ds num : %d", ds_num);

    int16_t total_replica_num = num_replica + choosen_ds.size();
    if (total_replica_num > ds_num) {
        LOGV(LL_INFO, "ds_num is little than total_replica_num!");
        num_replica = ds_num - choosen_ds.size();
        if (num_replica <= 0) {
            LOGV(LL_INFO, "Now num_replica = 0, Dont't need to choose!");
            return;
        }
        total_replica_num = ds_num;
    }

    //get the max dataserver num of each rack
    int16_t rack_count = 0;
    layout_manager_.racks_mutex_.rlock()->lock();
    RACKS_MAP_ITER rack_it = layout_manager_.get_racks_map().begin();
    for (; rack_it != layout_manager_.get_racks_map().end(); ++rack_it) {
        if ((rack_it->second).size() != 0)
            ++rack_count;
    }
    layout_manager_.racks_mutex_.rlock()->unlock();
    int16_t max_ds_per_rack = (total_replica_num - 1) / rack_count + 2;
    
    vector<uint64_t> exclude_ds;
    for (vector<uint64_t>::iterator it = choosen_ds.begin(); it != choosen_ds.end(); ++it)
        exclude_ds.push_back(*it);
    results = choosen_ds;

    uint64_t local_ds = choose_target(num_replica, writer, exclude_ds, max_ds_per_rack, results);

    for (vector<uint64_t>::iterator it = choosen_ds.begin(); it != choosen_ds.end(); ++it) {
        vector<uint64_t>::iterator results_it = find(results.begin(), results.end(), *it);
        if (results_it != results.end())
            results.erase(results_it);
    }

    get_pipeline((0 == writer) ? local_ds : writer, results);
}

void ReplicateStrategy::get_pipeline(string client_ip, vector<uint64_t> &results)
{
    if (results.size() == 0)
        return;
    uint64_t writer = writer_ip_to_id(client_ip);
    get_pipeline(writer, results);
    LOGV(LL_DEBUG, "*********************get_pipeline succeed!");  
}

void ReplicateStrategy::get_pipeline(uint64_t writer, vector<uint64_t> &results)
{
    if (results.size() == 0) 
        return;

    layout_manager_.server_mutex_.rlock()->lock();
    if (0 == writer || layout_manager_.get_dataserver_info(writer) == NULL) {
        int random = rand() % results.size();
        LOGV(LL_DEBUG, "random : %d", random);
        writer = results[random];
    }

    vector<uint64_t> new_results; 
    //第一个放writer 
    for (vector<uint64_t>::iterator it = results.begin(); it != results.end();) {
        if ((*it) == writer) {
            new_results.push_back(*it);
            it = results.erase(it);
        } else 
            ++it;
    }

    //接下来放置和writer同机架的DS 
    for (vector<uint64_t>::iterator it = results.begin(); it != results.end();) {
        if ((layout_manager_.get_dataserver_info(writer))->rack_id_ == (layout_manager_.get_dataserver_info(*it))->rack_id_) {
            new_results.push_back(*it);
            it = results.erase(it);     
        } else 
            ++it;
    }

    //按机架放置   
    vector<uint64_t>:: iterator it;
    while(0 != results.size()) {
        it = results.begin(); 
        uint64_t choosen_ds = *it;
        new_results.push_back(*it);
        results.erase(it);  
        for (it = results.begin(); it != results.end();) {
            if ((layout_manager_.get_dataserver_info(choosen_ds))->rack_id_ == (layout_manager_.get_dataserver_info(*it))->rack_id_) {
                new_results.push_back(*it); 
                it = results.erase(it);   
            } else 
                ++it;
        }
    }
    layout_manager_.server_mutex_.rlock()->unlock(); 
    results = new_results;  
}

uint64_t ReplicateStrategy::choose_target(int16_t num_replica, uint64_t writer, vector<uint64_t> &exclude_ds, int16_t max_ds_per_rack, vector<uint64_t> &results)
{
    if (0 == num_replica)
        return writer;

    int16_t test_replica = num_replica; 
    int16_t results_num = results.size();
    bool new_block = (0 == results_num);
    layout_manager_.server_mutex_.rlock()->lock();
    if (0 == writer || !layout_manager_.get_dataserver_info(writer))
        writer = 0;
    layout_manager_.server_mutex_.rlock()->unlock();
    if (0 == writer && !new_block)
        writer = results[0];

    switch(results_num) {
        case 0 :
            writer = choose_local_ds(writer, exclude_ds, max_ds_per_rack, results);
            LOGV(LL_DEBUG, "**********choose_local_ds succeed!");
            if (--num_replica == 0)
                break;
        case 1 :
            if (results.size() != 0)
                choose_remote_rack_ds(results[0], exclude_ds, max_ds_per_rack, results);
            else 
                choose_random_ds(exclude_ds, max_ds_per_rack, results);
            LOGV(LL_DEBUG, "*********choose_remote_rack_ds succeed!");
            if (--num_replica == 0)
                break;
        case 2 :
        {
            layout_manager_.server_mutex_.rlock()->lock(); 
            DataServerInfo *ds1_info = layout_manager_.get_dataserver_info(results[0]);
            DataServerInfo *ds2_info = layout_manager_.get_dataserver_info(results[1]);
            if (!ds1_info || !ds2_info) {
                LOGV(LL_ERROR, "Has to Rechoose! exclude_ds size: %ld", exclude_ds.size());
                //results.clear();
                layout_manager_.server_mutex_.rlock()->unlock(); 
                if ((layout_manager_.get_alive_ds_size() - exclude_ds.size()) < test_replica) {
                    test_replica = layout_manager_.get_alive_ds_size() - exclude_ds.size();
                    LOGV(LL_DEBUG, "num_replica change to %d", test_replica);
                }
                return choose_target(test_replica, writer, exclude_ds, max_ds_per_rack, results);
            }
            int32_t ds1_rack = ds1_info->rack_id_;
            int32_t ds2_rack = ds2_info->rack_id_; 
            LOGV(LL_DEBUG, "ds1_rack : %d , ds2_rack : %d", ds1_rack, ds2_rack);
            layout_manager_.server_mutex_.rlock()->unlock(); 

            if (ds1_rack == ds2_rack) {
                choose_remote_rack_ds(results[0], exclude_ds, max_ds_per_rack, results);
            } else {
                choose_local_rack_ds(writer, exclude_ds, max_ds_per_rack, results);
            }
            LOGV(LL_DEBUG, "*************choose third ds succeed!");
            if (--num_replica == 0) 
                break;
        }
        default :
            LOGV(LL_DEBUG, "num_replica : %d", num_replica); 
            choose_random_ds(num_replica, exclude_ds, max_ds_per_rack, results);
    }
    return writer;
}

uint64_t ReplicateStrategy::choose_local_ds(uint64_t writer, vector<uint64_t> &exclude_ds, int16_t max_ds_per_rack, vector<uint64_t> &results)
{
    LOGV(LL_DEBUG, "*************************IN choose_local_ds!");
    layout_manager_.server_mutex_.rlock()->lock();  
    if (0 == writer || layout_manager_.get_dataserver_info(writer) == NULL) {
        layout_manager_.server_mutex_.rlock()->unlock(); 
        return choose_random_ds(exclude_ds, max_ds_per_rack, results);
    }

    vector<uint64_t>::const_iterator it = find(exclude_ds.begin(), exclude_ds.end(), writer);
    if (it == exclude_ds.end()) {
        exclude_ds.push_back(writer); 
        int64_t free_space = layout_manager_.get_dataserver_info(writer)->get_free_space();
        double load_score = layout_manager_.get_dataserver_info(writer)->load_score_;
        if ((BLADE_BLOCK_SIZE * 20) < free_space && load_score < DS_CHOOSE_LEVEL) { 
            layout_manager_.server_mutex_.rlock()->unlock();
            results.push_back(writer);
            return writer;
        }
    }
    layout_manager_.server_mutex_.rlock()->unlock();  
    return choose_local_rack_ds(writer, exclude_ds, max_ds_per_rack, results);
}

uint64_t ReplicateStrategy::choose_local_rack_ds(uint64_t writer, vector<uint64_t> &exclude_ds, int16_t max_ds_per_rack, vector<uint64_t> &results)
{
    LOGV(LL_DEBUG, "************************IN choose_local_rack_ds!");
    layout_manager_.server_mutex_.rlock()->lock();  
    if (0 == writer || layout_manager_.get_dataserver_info(writer) == NULL) {
        layout_manager_.server_mutex_.rlock()->unlock(); 
        return choose_random_ds(exclude_ds, max_ds_per_rack, results);
    }
    
    int32_t rack_id = layout_manager_.get_dataserver_info(writer)->rack_id_;

    //test if results have too many ds in this rack
    int32_t ds_count_per_rack = 0;
    for (vector<uint64_t>::const_iterator it = results.begin(); it != results.end(); ++it) {
        assert(layout_manager_.get_dataserver_info(*it));
        if (rack_id == layout_manager_.get_dataserver_info(*it)->rack_id_) {
            ++ds_count_per_rack;
            if (ds_count_per_rack >= max_ds_per_rack) {
                layout_manager_.server_mutex_.rlock()->unlock(); 
                LOGV(LL_INFO, "too many ds in this rack, choose remote rack ds!");
                return choose_remote_rack_ds(writer, exclude_ds, max_ds_per_rack, results);
            }
        }
    }

    set<DataServerInfo *>::size_type ds_test_count = 0;
    for (vector<uint64_t>::const_iterator it = exclude_ds.begin(); it != exclude_ds.end(); ++it) {
        //LOGV(LL_DEBUG, "exclude_ds : %s", BladeNetUtil::addr_to_string(*it).c_str());
        if (layout_manager_.get_dataserver_info(*it) && (layout_manager_.get_dataserver_info(*it)->rack_id_ == rack_id))
            ++ds_test_count;
    }
    layout_manager_.server_mutex_.rlock()->unlock();

    layout_manager_.racks_mutex_.rlock()->lock(); 
    set<DataServerInfo *> & rack_dataservers = (layout_manager_.get_racks_map())[rack_id];
    uint64_t choosen_ds = 0;
    int64_t free_space = 0;
    double load_score = MAX_LOAD_SCORE;
    do {
        //LOGV(LL_DEBUG, "ds_test_count : %ld", ds_test_count);
        if (rack_dataservers.size() == 0 || ds_test_count++ == rack_dataservers.size())
        {
            layout_manager_.racks_mutex_.rlock()->unlock();
            return choose_random_ds(exclude_ds, max_ds_per_rack, results);
            //return choose_remote_rack_ds(writer, exclude_ds, max_ds_per_rack, results);
        }
        for (set<DataServerInfo *>::const_iterator it = rack_dataservers.begin(); it != rack_dataservers.end(); ++it) {
            if (load_score > (*it)->load_score_ && (find(exclude_ds.begin(), exclude_ds.end(), (*it)->dataserver_id_) == exclude_ds.end())) {
                load_score = (*it)->load_score_;
                choosen_ds = (*it)->dataserver_id_;
                free_space = (*it)->get_free_space();
            }
        }
        exclude_ds.push_back(choosen_ds);
        //LOGV(LL_DEBUG, "exclude_ds, choosen_ds : %s", BladeNetUtil::addr_to_string(choosen_ds).c_str());  
    }while((BLADE_BLOCK_SIZE * 20) >= free_space);
   
    layout_manager_.racks_mutex_.rlock()->unlock();
    results.push_back(choosen_ds);
    return choosen_ds;
}

uint64_t ReplicateStrategy::choose_remote_rack_ds(uint64_t writer, vector<uint64_t> &exclude_ds, int16_t max_ds_per_rack, vector<uint64_t> &results)
{
    LOGV(LL_DEBUG, "******************IN choose_remote_rack_ds!");
    layout_manager_.server_mutex_.rlock()->lock();
    if (0 == writer || layout_manager_.get_dataserver_info(writer) == NULL) {
        layout_manager_.server_mutex_.rlock()->unlock();
        return choose_random_ds(exclude_ds, max_ds_per_rack, results);
    }
    int32_t exclude_rack = layout_manager_.get_dataserver_info(writer)->rack_id_;   
    layout_manager_.server_mutex_.rlock()->unlock();

    layout_manager_.racks_mutex_.rlock()->lock();

    //if only has 1 rack,choose_local_rack_ds
    int16_t rack_count = 0;
    set<int32_t> test_rack_set;
    for (RACKS_MAP_ITER it = layout_manager_.get_racks_map().begin(); it != layout_manager_.get_racks_map().end(); ++it) {
        if (it->first != exclude_rack)
            test_rack_set.insert(it->first);
        if ((it->second).size() != 0)
            ++rack_count;
    }
    if (rack_count == 1) {
        layout_manager_.racks_mutex_.rlock()->unlock();
        return choose_local_rack_ds(writer, exclude_ds, max_ds_per_rack, results);
    }
    //LOGV(LL_DEBUG, "set size : %d", test_rack_set.size());

    //choose remote rack 
    set<DataServerInfo *>::size_type ds_test_count;
    int32_t random_rack = exclude_rack;
    do {
        if (test_rack_set.empty()) {
            LOGV(LL_INFO, "Test every remote rack,has to choose local rack!");
            layout_manager_.racks_mutex_.rlock()->unlock();
            return choose_local_rack_ds(writer, exclude_ds, max_ds_per_rack, results);
        }
        int random = rand() % layout_manager_.get_racks_map().size();
        RACKS_MAP_ITER racks_it = layout_manager_.get_racks_map().begin();
        for (int i = 0; i < random; ++i)
            ++racks_it;
        random_rack = racks_it->first;
        if (test_rack_set.count(random_rack) > 0) {
            //LOGV(LL_DEBUG, "random_rack : %d, in test_rack_set", random_rack);
            test_rack_set.erase(random_rack);
        } else {
            //LOGV(LL_DEBUG, "random_rack : %d, not in test_rack_set", random_rack);
            random_rack = exclude_rack;
            continue;
        }
       
        ds_test_count = 0;
        //LOGV(LL_DEBUG, "exclude_ds size : %d", exclude_ds.size()); 
        layout_manager_.server_mutex_.rlock()->lock();
        for (vector<uint64_t>::const_iterator it = exclude_ds.begin(); it != exclude_ds.end(); ++it) {
            if (layout_manager_.get_dataserver_info(*it) && (layout_manager_.get_dataserver_info(*it)->rack_id_ == random_rack))
                ++ds_test_count;
        }
        if (ds_test_count >= (layout_manager_.get_racks_map())[random_rack].size()) {
            random_rack = exclude_rack;
            layout_manager_.server_mutex_.rlock()->unlock();
            LOGV(LL_DEBUG, "ds_test_count : %d is bigger!", ds_test_count);
            continue;
        }

        //test if this rack has to many ds 
        int32_t ds_per_rack = 0;
        for (vector<uint64_t>::const_iterator it = results.begin(); it != results.end(); ++it) {
            if (layout_manager_.get_dataserver_info(*it) && (layout_manager_.get_dataserver_info(*it)->rack_id_ == random_rack))
                ++ds_per_rack;
        }
        if (ds_per_rack >= max_ds_per_rack) {
            random_rack = exclude_rack;
            layout_manager_.server_mutex_.rlock()->unlock();
            LOGV(LL_DEBUG, "ds_per_rack : %d is bigger", ds_per_rack);
            continue;
        }
        layout_manager_.server_mutex_.rlock()->unlock();
    }while(exclude_rack == random_rack);

    int64_t free_space = 0;
    uint64_t choosen_ds = 0;
    double load_score = MAX_LOAD_SCORE;
    set<DataServerInfo *> & rack_dataservers = (layout_manager_.get_racks_map())[random_rack];
    do {
        if (rack_dataservers.size() == 0 || ds_test_count++ == rack_dataservers.size()) {
            layout_manager_.racks_mutex_.rlock()->unlock();
            return choose_random_ds(exclude_ds, max_ds_per_rack, results);
        }
        for (set<DataServerInfo *>::const_iterator it = rack_dataservers.begin(); it != rack_dataservers.end(); ++it) {
            if ((load_score > (*it)->load_score_) && (find(exclude_ds.begin(), exclude_ds.end(), (*it)->dataserver_id_) == exclude_ds.end())) {
                load_score = (*it)->load_score_;
                choosen_ds = (*it)->dataserver_id_;
                free_space = (*it)->get_free_space();
            }
        }
        exclude_ds.push_back(choosen_ds);
    }while((BLADE_BLOCK_SIZE * 20) >= free_space);
    
    layout_manager_.racks_mutex_.rlock()->unlock();
    results.push_back(choosen_ds);
    return choosen_ds;   
}

uint64_t ReplicateStrategy::choose_random_ds(vector<uint64_t> &exclude_ds, int16_t max_ds_per_rack, vector<uint64_t> &results)
{
    vector<uint64_t>::size_type old_count = results.size();
    choose_random_ds(1, exclude_ds, max_ds_per_rack, results);

    vector<uint64_t>::size_type new_count = results.size();
    if ((new_count - old_count) == 1)
        return results[new_count - 1];
    else 
        return 0;
}

void ReplicateStrategy::choose_random_ds(int16_t num_replica, vector<uint64_t> &exclude_ds, int16_t  max_ds_per_rack, vector<uint64_t> &results)
{
    //retest num_replica
    int32_t ds_num = layout_manager_.get_alive_ds_size();
    int16_t test_replica_num = num_replica + exclude_ds.size();
    if (test_replica_num > ds_num) {
        num_replica = ds_num - exclude_ds.size();
        LOGV(LL_INFO, "in choose_random_ds, num_replica change to %d", num_replica);
    }

    layout_manager_.racks_mutex_.rlock()->lock();   
    for (int16_t i = 0; i < num_replica; ++i) {
        int32_t random_rack;
        uint64_t choosen_ds = 0;
        int64_t free_space = 0;
        do {
            int random = rand() % layout_manager_.get_racks_map().size();
            RACKS_MAP_ITER racks_it = layout_manager_.get_racks_map().begin();
            for (int j = 0; j < random; ++j)
                ++racks_it;
            random_rack = racks_it->first; 
            LOGV(LL_INFO, "rack id :%d, exclude_ds size: %ld", random_rack, exclude_ds.size());
            if (exclude_ds.size() == layout_manager_.get_alive_ds_size()) {
                LOGV(LL_ERROR, "No ds to choose!");
                return;
            }

            set<DataServerInfo *>::size_type ds_test_count = 0;
            layout_manager_.server_mutex_.rlock()->lock();
            for (vector<uint64_t>::const_iterator it = exclude_ds.begin(); it != exclude_ds.end(); ++it) { 
                if (layout_manager_.get_dataserver_info(*it) && layout_manager_.get_dataserver_info(*it)->rack_id_ == random_rack)
                    ++ds_test_count;
            }
            if (ds_test_count >= (layout_manager_.get_racks_map())[random_rack].size()) {
                layout_manager_.server_mutex_.rlock()->unlock();
                free_space = BLADE_BLOCK_SIZE - 1;
                continue;
            }

            int32_t ds_per_rack_count = 0;
            for (vector<uint64_t>::const_iterator it = results.begin(); it != results.end(); ++it) {
                if (layout_manager_.get_dataserver_info(*it) && layout_manager_.get_dataserver_info(*it)->rack_id_ == random_rack)
                    ++ds_per_rack_count;
            }
            if (ds_per_rack_count >= max_ds_per_rack) {
                layout_manager_.server_mutex_.rlock()->unlock();
                free_space = BLADE_BLOCK_SIZE - 1;
                continue;
            }
            layout_manager_.server_mutex_.rlock()->unlock();

            set<DataServerInfo *> &rack_dataservers = (layout_manager_.get_racks_map())[random_rack];
            double load_score = MAX_LOAD_SCORE;
            for (set<DataServerInfo *>::const_iterator it = rack_dataservers.begin(); it != rack_dataservers.end(); ++it) {
                if ((load_score > (*it)->load_score_) && (find(exclude_ds.begin(), exclude_ds.end(), (*it)->dataserver_id_) == exclude_ds.end())) {
                    load_score = (*it)->load_score_;
                    choosen_ds = (*it)->dataserver_id_;
                    free_space = (*it)->get_free_space();
                }
            }
            exclude_ds.push_back(choosen_ds);
        }while((BLADE_BLOCK_SIZE * 20) >= free_space);

        results.push_back(choosen_ds);
    }
    layout_manager_.racks_mutex_.rlock()->unlock();
}

uint64_t ReplicateStrategy::writer_ip_to_id(const string ip)
{
    uint64_t writer = 0;
    int port;
    double load_score = MAX_LOAD_SCORE;
    layout_manager_.server_mutex_.rlock()->lock(); 
    SERVER_MAP & server_map = layout_manager_.get_server_map();
    for (SERVER_MAP_ITER it = server_map.begin(); it != server_map.end(); ++it) {
        string ds_ip = BladeNetUtil::AddrToIP(it->first, &port);

        //存在与writer同ip的dataserver
        if (ip == ds_ip && it->second->is_alive()) {
            LOGV(LL_DEBUG, "writer ip == ds_ip"); 
            double test_load_score = it->second->load_score_;
            LOGV(LL_DEBUG, "test_load_score : %f, load_score : %f", test_load_score, load_score);
            if (test_load_score <= load_score) {
                writer = it->first;
                load_score = test_load_score; 
            }
        }
    }
    layout_manager_.server_mutex_.rlock()->unlock();  
    return writer;
}

}//end of namespace nameserver
}//end of namespace bladestore
