/*
 * Sohu R&D  2012
 *
 * File Description:
 *      The monitor class of dataserver 
 *
 * Author   : @
 * Version  : 1.0
 * Date     : 2012-08-15
 */
#ifndef BLADESTORE_DATASERVER_STAT_MANAGER_H
#define BLADESTORE_DATASERVER_STAT_MANAGER_H

#include <stdint.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>

#include "cthread.h"
#include "atomic.h"
#include "singleton.h"

namespace bladestore
{
namespace dataserver
{
using namespace pandora;
using namespace std;
class DataServerImpl;
class DataServerStatManager : public Runnable
{
public:
    DataServerStatManager();
    ~DataServerStatManager();
    bool Start(DataServerImpl * ds_impl);    
    void Run(CThread * thread, void * arg);

public:
    std::string ip_port_;

    int64_t total_storage_;//总容量
    int64_t used_storage_;//已使用容量
    atomic64_t block_nums_;                  //本地存储block的总数目
    atomic64_t request_connection_num_;  //当前时间间隔内的连接总数(作为服务器收到的连接总数)
    atomic64_t courrent_connection_num_;  //当前时间点的连接总数

    atomic64_t client_queue_length_;     //client队列的长度
    atomic64_t ns_queue_length_;         //ns队列的长度
    atomic64_t sync_queue_length_;       //同步队列长度

    atomic64_t read_bytes_;              //当前时间间隔内本地磁盘读的字节总数（包括处理client端的读请求以及复制本地block到其他dataserver）
    atomic64_t read_packet_times_;        //当前时间间隔内读操作的packet数目（client端发过来的读请求）
    atomic64_t read_packet_error_times_;  //当前时间间隔内读失败的packet数目
    
    atomic64_t write_bytes_;              //当前时间间隔内本地磁盘写的字节总数
    atomic64_t write_packet_times_;       //当前时间间隔内写的packet数目（普通写和安全写总和）
    atomic64_t write_packet_error_times_; //当前时间间隔内写失败的packet数目（普通写和安全写总和）

    atomic64_t normal_write_block_times_; //当前时间间隔内普通写block总数目（包括成功的和失败的）
    atomic64_t normal_write_block_complete_times_; //当前时间间隔内普通写block完成提交的数目

    atomic64_t safe_write_block_times_;   //当前时间间隔内安全写block总数目（包括成功的和失败的）
    atomic64_t safe_write_block_complete_times_; //当前时间间隔内安全写block完成提交的数目

    atomic64_t delete_block_times_;        //当前时间间隔内删除block总数目（包括成功的和失败的）
    atomic64_t delete_block_error_times_; 

    atomic64_t transfer_block_times_;       //当前时间间隔内发往其他DS的block总数目（包括成功的和失败的）
    atomic64_t transfer_block_error_times_; //当前时间间隔内发往其他DS的block失败数目

    atomic64_t replicate_packet_times_;      //当前时间间隔内复制写packet总数目（包括成功的和失败的）
    atomic64_t replicate_packet_error_times_;//当前时间间隔内复制写packet失败的数目

    atomic64_t replicate_block_times_;       //当前时间间隔内复制写block总数目（包括成功的和失败的）
    atomic64_t replicate_block_complete_times_; //当前时间间隔内复制写block完成提交的数目
private:
    void StatPrint();
    void Clear();

    std::string stat_log_;
    int64_t stat_check_interval_;
    ofstream outfile_;
    DataServerImpl * ds_impl_;
    bool is_run_;

    CThread stat_check_;
    DISALLOW_COPY_AND_ASSIGN(DataServerStatManager);
};

}//end of namespace dataserver
}//end of namespace bladestore
#endif

