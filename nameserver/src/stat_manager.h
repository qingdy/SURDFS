/*
 *version  :  1.0
 *author   :  funing
 *date     :  2012-8-8
 *
 */
#ifndef BLADESTORE_NAMESERVER_STAT_MANAGER_H
#define BLADESTORE_NAMESERVER_STAT_MANAGER_H

#include <stdint.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>

#include "cthread.h"
#include "atomic.h"
#include "singleton.h"

#ifdef GTEST
#define private public
#define protected public
#endif


namespace bladestore
{
namespace nameserver
{
using namespace pandora;
using namespace std;
class StatManager : public Runnable
{
public:
	StatManager();
	~StatManager();
    void Start();    
	void Run(CThread * thread,void * arg);	

public:
    std::string ip_port_;
    std::string vip_;
    std::string stat_;
    
    atomic64_t read_queue_length_;
    atomic64_t write_queue_length_;
    atomic64_t read_times_;
    atomic64_t read_error_times_;
    atomic64_t write_times_;
    atomic64_t write_error_times_;
    atomic64_t safe_write_times_;
    atomic64_t safe_write_error_times_;
    atomic64_t delete_times_;
    atomic64_t delete_error_times_;
    atomic64_t copy_times_;
    atomic64_t copy_error_times_;

    atomic64_t dir_nums_;
    atomic64_t file_nums_;
    atomic64_t block_nums_;

private:
    void StatPrint();
    void Clear();

    std::string stat_log_;
    int64_t stat_check_interval_;
    ofstream outfile_;
    bool is_run_;

    CThread stat_check_;
    //DISALLOW_COPY_AND_ASSIGN(StatManager);
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif

