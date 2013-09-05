/*
 * Sohu R&D  2012
 *
 * File Description:
 *      
 * Author   : @CYY
 * Version  : 1.0
 * Date     : 2012-06-01
 */
#ifndef BLADESTORE_FS_INTERFACE_H
#define BLADESTORE_FS_INTERFACE_H

#include <ext/hash_map>
#include "time_util.h"
#include "timer.h"
#include "handle.h"
#include "shared.h"
#include "thread_mutex.h"
#include "amframe.h"

#include "block.h"
#include "file_utils.h"
#include "blade_rwlock.h"
#include "blade_common_define.h"


namespace bladestore
{
namespace dataserver
{
using pandora::AmFrame;
using pandora::Log;
using pandora::TimerTask;
using pandora::Timer;
using pandora::Handle;
using namespace bladestore::common;
//------------------------class Blockstatus-------------------//
class BlockStatus: public Shared
{
public:
    BlockStatus();
    ~BlockStatus();
    int64_t blockfile_fd() const {return blockfile_fd_;}
    int64_t metafile_fd() const {return metafile_fd_;}
    FileUtils &  meta_file()  {return meta_file_;}
    int64_t block_id() const {return block_id_;}
    int32_t block_version() const {return block_version_;}
    int64_t block_length() const {return block_length_;}
    uint64_t src_id() const {return src_id_;}
    uint64_t des_id() const {return des_id_;}
    char *  checksum_buf() const {return checksum_buf_;}
    int8_t  mode() const {return mode_;}
    AmFrame::StreamEndPoint & end_point()  {return end_point_;}
    int16_t ret_code() const {return ret_code_;}
    int64_t last_io_time() const {return last_io_time_;}
    static int64_t metafile_size() {return metafile_size_;} 

    void    set_blockfile_fd(int64_t fd) {blockfile_fd_ = fd;}
    void    set_metafile_fd(int64_t fd) {metafile_fd_ = fd;}
    void    set_block_id(int64_t block_id) {block_id_ = block_id;}
    void    set_block_version(int32_t block_version) {block_version_ = block_version;}
    void    set_block_length(int64_t block_length) {block_length_ = block_length;}
    void    set_src_id(uint64_t src_id) {src_id_ = src_id;}
    void    set_des_id(uint64_t des_id) {des_id_ = des_id;}
    void    set_checksum_buf(char * buf) {checksum_buf_ = buf;}
    void    set_mode(int8_t mode) {mode_ = mode;}
    void    set_end_point(AmFrame::StreamEndPoint * end_point) {end_point_ = *end_point;}
    void    set_ret_code(int16_t ret_code) {ret_code_ = ret_code;}
    void    set_last_io_time(int64_t last_io_time) {last_io_time_ = last_io_time;}

    void    AddBlockLength(int64_t length) {block_length_ += length;}

private:
    int64_t blockfile_fd_;
    int64_t metafile_fd_;
    FileUtils meta_file_;
    int64_t block_id_;
    int32_t block_version_;
    int64_t block_length_;
    uint64_t src_id_;
    uint64_t des_id_;
    char *  checksum_buf_;//load meta_file into memory
    const static int64_t metafile_size_; 
    int8_t  mode_;// 0 for common write 1 for safewrite 
    AmFrame::StreamEndPoint  end_point_;
    int16_t ret_code_;
    int64_t last_io_time_;

    DISALLOW_COPY_AND_ASSIGN(BlockStatus);
};
//---------------------------class BlockCacheCheck------------------//
class FSDataInterface;  

class BlockCacheCheck : public TimerTask
{
public:
    BlockCacheCheck(FSDataInterface * fs_interface) : fs_interface_(fs_interface){}
    virtual ~BlockCacheCheck(){}
    void    RunTimerTask();

private:
    FSDataInterface * fs_interface_;

    DISALLOW_COPY_AND_ASSIGN(BlockCacheCheck);
};
//-------------------class FSDataInterface----------------//
class FSDataSet;

typedef pandora::Handle<BlockStatus> BlockStatusPtr;
typedef __gnu_cxx::hash_map<int64_t, BlockStatusPtr>::iterator BLOCK_CACHE_ITER;
typedef pandora::Handle<BlockCacheCheck> BlockCacheCheckPtr;

class FSDataInterface  
{
public:
    FSDataInterface(FSDataSet * fs_dataset);
    ~FSDataInterface();

    BlockStatusPtr PrepareRead(Block & b);
    BlockStatusPtr PrepareWrite(Block & b, int8_t write_mode = 0); //is_safe_write: 0 for common write ,1 for safewrite
    BlockStatusPtr GetBlockStatus(Block & b);
    BlockStatusPtr PrepareReplicate(Block & b); //use for replicate
    int32_t       Complete(Block & b, int8_t mode);//complete write 
    void          CheckBlock();
    int64_t       BlockIdToFileId(int64_t block_id);
private:
    FSDataSet * fs_dataset_;
    CRWLock     rw_lock_;
    CThreadMutex read_mutex_;
    CThreadMutex write_mutex_;
    CThreadMutex replicate_mutex_;
    TimerPtr       timer_;
    int64_t     check_interval_sec_;
    int64_t     block_expire_time_;
    BlockCacheCheckPtr blockcache_check_;
    __gnu_cxx::hash_map<int64_t, BlockStatusPtr> block_cache_;

    DISALLOW_COPY_AND_ASSIGN(FSDataInterface);
};

}//end of namespace dataserver
}//end of namespace bladestore

#endif
