#include "unistd.h"

#include "log.h"
#include "mempool.h"
#include "singleton.h"
#include "timer_task.h"
#include "time_wrap.h"
#include "socket_context.h"
#include "fsdataset.h"
#include "dataserver_impl.h"
#include "dataserver_stat_manager.h"
#include "fs_interface.h"

namespace bladestore
{
namespace dataserver
{
const int64_t BlockStatus::metafile_size_ = 
                    BLADE_BLOCK_SIZE/BLADE_CHUNK_SIZE*BLADE_CHECKSUM_SIZE + 1;

BlockStatus::BlockStatus()
{
    checksum_buf_ = NULL;
    block_length_ = 0;
}

BlockStatus::~BlockStatus()
{
    if (NULL != checksum_buf_)
    {
        MemPoolFree(checksum_buf_);
        checksum_buf_ = NULL;
    }
}

void BlockCacheCheck::RunTimerTask()
{
    if (NULL == fs_interface_)
    {
       LOGV(LL_ERROR, "fs_interface is NULL");
    }
    else
    {
        fs_interface_->CheckBlock();
    }
}

FSDataInterface::FSDataInterface(FSDataSet * fs_dataset)
                                : fs_dataset_(fs_dataset)
{

    blockcache_check_ = new BlockCacheCheck(this);
    //XXX adjust these two const
    check_interval_sec_ = DS_CHECK_INTERVAL;  // 60s
    block_expire_time_ = DS_BLOCK_EXPIRE_TIME; // 210s
    timer_ = new pandora::Timer();
    timer_->ScheduleRepeated(blockcache_check_, Time(check_interval_sec_));
}

FSDataInterface::~FSDataInterface()
{
}

BlockStatusPtr FSDataInterface::PrepareRead(Block & b)
{

    read_mutex_.Lock();
    BlockStatusPtr block_status;
    rw_lock_.rlock()->lock();
    //读的情况有很多，普通写后读，安全写中途中断后读。
    BLOCK_CACHE_ITER iter = block_cache_.find(b.block_id());
    if (iter != block_cache_.end() ) 
    {
        block_status = iter->second; 
        rw_lock_.rlock()->unlock();
        read_mutex_.Unlock();
        if (RET_SUCCESS == (block_status->ret_code()) && 
                    (kReadMode == (block_status->mode())||
                    (kWriteCompleteMode == block_status->mode())||
                    (kSafeWriteMode == block_status->mode())) && 
                    (block_status->block_version() == b.block_version())) 
        {
            // this can only be safe_write corrupt before complete   and here can be removed
            if (kReadMode != (block_status->mode())&&(0 == block_status->block_length()))
            {
                    int64_t block_length = fs_dataset_->GetBlockLength(b);
                    if (0 > block_length) 
                    {
                        block_status = new BlockStatus();
                        block_status->set_ret_code(RET_READ_ERROR);
                        block_status->set_mode(kReadMode);
                        //block file may be removed 
                        LOGV(LL_ERROR, "get block_length:-1 read,blk_id:%ld,"
                             "block_version:%d", b.block_id(), b.block_version());
                        return block_status; 
                    }
                    else 
                    {
                        block_status->set_block_length(block_length);
                    }
            }
            block_status->set_last_io_time(TimeUtil::GetTime());
            LOGV(LL_DEBUG, "prep read:blk_length:%ld;blk_id:%ld,"
                        "block_version:%d;", block_status->block_length(), 
                        b.block_id(), b.block_version());
            return block_status; 
        } 
        else 
        {
            int16_t ret_code_temp;
            if ((RET_SUCCESS == block_status->ret_code()) && 
                    ((kReadMode == block_status->mode())||
                    (kWriteCompleteMode == block_status->mode())|| 
                    (kSafeWriteMode == block_status->mode())) && 
                    (block_status->block_version() != b.block_version())) 
            {
                ret_code_temp = RET_READ_VERSION_NOT_MATCH;
            } 
            else 
            {
                ret_code_temp = RET_OPERATION_ON_GOING;
            }

            block_status = new BlockStatus();
            
            block_status->set_ret_code(ret_code_temp);
            block_status->set_mode(kReadMode);

            LOGV(LL_ERROR, "on going;prep read,blk_id:%ld,block_version:%d,"
                "ret_code:%d", b.block_id(), b.block_version(), ret_code_temp);
            return block_status; 
        }
    }
    rw_lock_.rlock()->unlock();

    block_status = new BlockStatus();
    block_status->set_mode(kReadMode);
    // is there a need to check NULL ,this function will not return NULL
    ReadBlockInfo * read_block_fd = fs_dataset_->ReadBlockAndMetaFile(b);
    if (NULL == read_block_fd) 
    {
        LOGV(LL_ERROR,  "fs_dataset op return NULL");
        block_status->set_ret_code(RET_READ_ERROR);
        read_mutex_.Unlock();
        return block_status;
    }
    block_status->set_ret_code(read_block_fd->return_code_);
    if (RET_SUCCESS == read_block_fd->return_code_) 
    {
        LOGV(LL_ERROR,  "ret_code != RET_SUCCESS: %d", block_status->ret_code());
        delete read_block_fd;
        read_mutex_.Unlock();
        return block_status;
    }
    char * checksum_buf =  static_cast<char *>
                                  (MemPoolAlloc(block_status->metafile_size()));
    block_status->set_checksum_buf(checksum_buf);
    if (!(block_status->checksum_buf())) 
    {
        LOGV(LL_ERROR,  "mempoolalloc checksum_buf_ error");
        block_status->set_ret_code(RET_READ_MEMMALLOC_ERROR);
        delete read_block_fd;
        read_mutex_.Unlock();
        return block_status;
    }
    memset(block_status->checksum_buf(), 0, block_status->metafile_size());

    block_status->set_blockfile_fd(read_block_fd->block_fd_);
    block_status->set_metafile_fd(read_block_fd->meta_fd_);

    block_status->set_block_id(b.block_id()); 
    block_status->set_block_version(b.block_version());
    block_status->set_block_length(read_block_fd->block_length_);
    LOGV(LL_DEBUG,  "prepare read block_id:%ld,block_length:%ld",
         block_status->block_id(), block_status->block_length());

    block_status->meta_file().set_fd(block_status->metafile_fd());

    int64_t bytes_read = block_status->meta_file().pread(
                                    block_status->checksum_buf(),
                                    block_status->metafile_size() - 1 , 0);
    //XXX for monitor
    AtomicAdd64(&Singleton<DataServerStatManager>::Instance().read_bytes_, (int64_t)bytes_read);

    if (bytes_read != block_status->metafile_size() - 1) 
    {
        LOGV(LL_ERROR,  "read checksum_ into buf_ error");
        block_status->set_ret_code(RET_READ_PREAD_ERROR);
        delete read_block_fd;
        read_mutex_.Unlock();
        return block_status;
    }

    block_status->set_last_io_time(TimeUtil::GetTime());

    rw_lock_.wlock()->lock();
    block_cache_[b.block_id()] = block_status;
    rw_lock_.wlock()->unlock();

    read_mutex_.Unlock();
    delete read_block_fd;
    return block_status;
}

BlockStatusPtr FSDataInterface::PrepareWrite(Block & b, int8_t write_mode)
{
    write_mutex_.Lock();
    BlockStatusPtr block_status = new BlockStatus();
    switch (write_mode) 
    {
        case  kWriteMode:
        case  kSafeWriteMode:
            break;
        default: 
        {
            block_status->set_ret_code(RET_WRITE_MODE_INVALID);
            write_mutex_.Unlock();
            return block_status;
        }
    }
    rw_lock_.rlock()->lock();

    BLOCK_CACHE_ITER iter = block_cache_.find(b.block_id());
    if (iter != block_cache_.end()) 
    {
            //version is match  and mode is match too
        if ((iter->second->block_version() == b.block_version()) &&
                (iter->second->mode() == write_mode)) 
        {
            block_status = iter->second;
            rw_lock_.rlock()->unlock();
            write_mutex_.Unlock();
            return block_status; 
        }
        else 
        {
            if (iter->second->block_version() != b.block_version()) 
            {
                block_status->set_ret_code(RET_PIPELINE_VERSION_NOT_MATCH);
            }
            else 
            {
                block_status->set_ret_code(RET_OPERATION_ON_GOING);
            }
            rw_lock_.rlock()->unlock();
            write_mutex_.Unlock();
            return block_status;
        }
    }

    rw_lock_.rlock()->unlock();

    //XXX for monitor
    if (kWriteMode == write_mode) 
    {
        AtomicInc64(&Singleton<DataServerStatManager>::Instance().normal_write_block_times_);
    }
    else 
    {
        AtomicInc64(&Singleton<DataServerStatManager>::Instance().safe_write_block_times_);
    }

    WriteBlockFd * write_block_fd = fs_dataset_->WriteBlockAndMetaFd(b);
    if (NULL == write_block_fd) 
    {
        LOGV(LL_ERROR,  "preparewrite fs_dataset op return NULL");
        block_status->set_ret_code(RET_ERROR);
        write_mutex_.Unlock();
        return block_status;
    }
    LOGV(LL_INFO, "prepare write blk_id:%d,blk_fd:%d;meta_fd:%d;"
                "return_code:%d,write_mode:%d", b.block_id(),
                write_block_fd->block_fd_, write_block_fd->meta_fd_,
                write_block_fd->return_code_, write_mode);
    
    block_status->set_ret_code(write_block_fd->return_code_);

    if (RET_SUCCESS == block_status->ret_code()) 
    {    
        block_status->set_mode(write_mode);
        block_status->set_last_io_time(TimeUtil::GetTime());
        block_status->set_metafile_fd(write_block_fd->meta_fd_);
        block_status->set_blockfile_fd(write_block_fd->block_fd_);
        block_status->set_block_id(b.block_id()); 
        block_status->set_block_version(b.block_version());
        block_status->set_block_length(0);
        block_status->meta_file().set_fd(write_block_fd->meta_fd_);
        LOGV(LL_DEBUG, "ftruncate fd%d, writeblockfd:%d",
             block_status->metafile_fd(), write_block_fd->meta_fd_);

        int ret = block_status->meta_file().ftruncate(
                                    block_status->metafile_size() - 1);
        if (0 != ret) 
        {
            LOGV(LL_ERROR, "ftruncate_ error%s", strerror(errno));
            block_status->set_ret_code(RET_WRITE_FTRUNCATE_ERROR);
            delete write_block_fd;
            write_mutex_.Unlock();
            return block_status;
        }

        block_status->set_checksum_buf(static_cast<char *>(MemPoolAlloc(
                                        block_status->metafile_size())));
        if (!(block_status->checksum_buf())) 
        {
            LOGV(LL_ERROR,  "mempoolalloc checksum_buf_ error");
            block_status->set_ret_code(RET_WRITE_MEMMALLOC_ERROR);
            delete write_block_fd;
            write_mutex_.Unlock();
            return block_status;
        }
        memset(block_status->checksum_buf(), 0, block_status->metafile_size());
        block_status->set_ret_code(write_block_fd->return_code_);

        rw_lock_.wlock()->lock();
        block_cache_[b.block_id()] = block_status;
        rw_lock_.wlock()->unlock();
    } 
    else 
    {
        LOGV(LL_ERROR,  "prepare write ret_code:%d, not RET_SUCCESS",
                    block_status->ret_code());
    }
    write_mutex_.Unlock();
    delete write_block_fd;
    return block_status;
}

BlockStatusPtr FSDataInterface::GetBlockStatus(Block & b)
{
    rw_lock_.rlock()->lock();
    BLOCK_CACHE_ITER iter = block_cache_.find(b.block_id());

    if ((block_cache_.end() != iter) && 
            (iter->second->block_version() == b.block_version())) 
    {
            rw_lock_.rlock()->unlock();
            return iter->second; 
    }
    rw_lock_.rlock()->unlock();
    BlockStatusPtr block_status = new BlockStatus();
    block_status->set_ret_code(RET_WRITE_ITEM_NOT_IN_MAP);
    return block_status;
}

BlockStatusPtr FSDataInterface::PrepareReplicate(Block & b)
{
    replicate_mutex_.Lock();
    BlockStatusPtr block_status = new BlockStatus();

    rw_lock_.rlock()->lock();
    BLOCK_CACHE_ITER iter = block_cache_.find(b.block_id());

    if (iter != block_cache_.end()) 
    {
            //version is match  and mode is match too
        if ((iter->second->block_version() == b.block_version()) &&
                (kReplicateMode == iter->second->mode())) 
        {
            block_status = iter->second;
            rw_lock_.rlock()->unlock();
            replicate_mutex_.Unlock();
            return block_status; 
        } 
        else 
        {
            block_status->set_ret_code(RET_OPERATION_ON_GOING);
            rw_lock_.rlock()->unlock();
            replicate_mutex_.Unlock();
            return block_status;
        }
    }

    rw_lock_.rlock()->unlock();
    //XXX for monitor
    AtomicInc64(&Singleton<DataServerStatManager>::Instance().replicate_block_times_);

    WriteBlockFd * replicate_block_fd = fs_dataset_->PrepareReplicate(b);
    if (NULL == replicate_block_fd) 
    {
        LOGV(LL_ERROR,  "preparereplicate fs_dataset return NULL");
        block_status->set_ret_code(RET_ERROR);
        replicate_mutex_.Unlock();
        return block_status;
   }
    LOGV(LL_INFO, "prepare write blk_id:%d,blk_fd:%d;meta_fd:%d;"
                "return_code:%d", b.block_id(), replicate_block_fd->block_fd_,
                replicate_block_fd->meta_fd_, replicate_block_fd->return_code_);
    
    block_status->set_ret_code(replicate_block_fd->return_code_);

    if (RET_SUCCESS == block_status->ret_code()) 
    {
    
        block_status->set_mode(kReplicateMode);
        block_status->set_last_io_time(TimeUtil::GetTime());
        block_status->set_metafile_fd(replicate_block_fd->meta_fd_);
        block_status->set_blockfile_fd(replicate_block_fd->block_fd_);
        block_status->set_block_id(b.block_id()); 
        block_status->set_block_version(b.block_version());
        block_status->set_block_length(0);
        block_status->meta_file().set_fd(block_status->metafile_fd());
        int ret = block_status->meta_file().ftruncate(
                                    block_status->metafile_size() - 1);
        if (0 != ret) 
        {
            LOGV(LL_ERROR, "ftruncate_ error");
            block_status->set_ret_code(RET_WRITE_FTRUNCATE_ERROR);
            delete replicate_block_fd;
            replicate_mutex_.Unlock();
            return block_status;
        }

        block_status->set_checksum_buf(static_cast<char *>(MemPoolAlloc(
                                        block_status->metafile_size())));
        if (!(block_status->checksum_buf())) 
        {
            LOGV(LL_ERROR,  "mempoolalloc checksum_buf_ error");
            block_status->set_ret_code(RET_WRITE_MEMMALLOC_ERROR);
            delete replicate_block_fd;
            replicate_mutex_.Unlock();
            return block_status;
        }
        memset(block_status->checksum_buf(), 0, block_status->metafile_size());
        block_status->set_ret_code(replicate_block_fd->return_code_);

        rw_lock_.wlock()->lock();
        block_cache_[b.block_id()] = block_status;
        rw_lock_.wlock()->unlock();
    } 
    else 
    {
        LOGV(LL_ERROR,  "prepare write ret_code:%d, not RET_SUCCESS",
                    block_status->ret_code());
    }
    replicate_mutex_.Unlock();
    delete replicate_block_fd;
    return block_status;
}


int32_t FSDataInterface::Complete(Block & b, int8_t mode)
{
    switch (mode) 
    {
        case  kWriteMode:
        {
            //XXX for monitor
            AtomicInc64(&Singleton<DataServerStatManager>::
                    Instance().normal_write_block_complete_times_);
            break;
        }
        case  kReplicateMode:
        {
            //XXX for monitor
            AtomicInc64(&Singleton<DataServerStatManager>::
                    Instance().replicate_block_complete_times_);
            break;
        }
        case  kSafeWriteMode:
        {
            //XXX for monitor
            AtomicInc64(&Singleton<DataServerStatManager>::
                    Instance().safe_write_block_complete_times_);
            break;
        }
        default: 
        {
            LOGV(LL_ERROR, "invalid mode for blk_id:%d,blk_version:%d", 
                 b.block_id(), b.block_version());
            return BLADE_ERROR;
        }
    }
    int32_t ret_code = BLADE_SUCCESS;
    rw_lock_.wlock()->lock();
    BLOCK_CACHE_ITER iter = block_cache_.find(b.block_id());
    if (block_cache_.end() != iter) 
    {
        if (iter->second->block_version() != b.block_version()) 
        {
            LOGV(LL_ERROR, "blk_version not match for blk_id:%d while"
                        "completing", b.block_id());
            rw_lock_.wlock()->unlock();
            return BLADE_ERROR; 
        }
        int8_t mode_local = iter->second->mode();
        if (mode != mode_local) 
        {
            LOGV(LL_ERROR, "complete mode not match  blk_id:%d", b.block_id());
            rw_lock_.wlock()->unlock();
            return BLADE_ERROR;
        }
        switch (mode_local)
        {
            case  kSafeWriteMode:
            {
                LOGV(LL_DEBUG, "complete safe_write blk_id:%d,", b.block_id());
                iter->second->set_mode(kWriteCompleteMode);
                break;
            }
            case  kWriteMode:
            {
                LOGV(LL_DEBUG, "complete normal write blk_id:%d", b.block_id());
                int64_t bytes_write = iter->second->meta_file().pwrite(
                                      iter->second->checksum_buf(),
                                      iter->second->metafile_size() - 1, 0);
                //XXX for monitor
                AtomicAdd64(&Singleton<DataServerStatManager>::
                        Instance().write_bytes_, (int64_t)bytes_write);

                if (bytes_write < iter->second->metafile_size() - 1) 
                {
                    LOGV(LL_ERROR, "complete pwrite error %d,!", b.block_id());
                    ret_code = BLADE_ERROR;
                }
                iter->second->set_mode(kWriteCompleteMode);
                break;
            }
            case  kReplicateMode:
            {
                LOGV(LL_DEBUG, "complete replicate blk_id:%d", b.block_id());
                int64_t bytes_write = iter->second->meta_file().pwrite(
                                            iter->second->checksum_buf(),
                                            iter->second->metafile_size() - 1, 0);
                //XXX for monitor
                AtomicAdd64(&Singleton<DataServerStatManager>::
                        Instance().write_bytes_, (int64_t)bytes_write);

                if (bytes_write < iter->second->metafile_size() - 1) 
                {
                    LOGV(LL_ERROR, "complete pwrite error %d,!", b.block_id());
                    ret_code = BLADE_ERROR;
                }
                else
                {
                    //here let fs_dataset_ knows how to handle the file
                    iter->second->set_mode(kReplicateCompleteMode);
                }
                fs_dataset_->CloseFile(b, iter->second->blockfile_fd(),
                            iter->second->metafile_fd(), iter->second->mode());
                block_cache_.erase(iter);
                break;
            }
            default:
            {
                LOGV(LL_ERROR, "complete invalid mode:%d,blk_id:%d,!",
                            iter->second->mode(), b.block_id());
                ret_code = BLADE_ERROR;
                break;
            }
        }
        rw_lock_.wlock()->unlock();
        return ret_code;
    }
    rw_lock_.wlock()->unlock();
    return BLADE_ERROR;
}

void FSDataInterface::CheckBlock()
{
    int64_t     now = TimeUtil::GetTime();
    int64_t expire_time = now - block_expire_time_;
    rw_lock_.wlock()->lock();
    BLOCK_CACHE_ITER iter = block_cache_.begin();
    for ( ; iter != block_cache_.end(); )
    {
        LOGV(LL_INFO, "come into check script. blk_id%d", iter->first);
        if (iter->second->last_io_time() < expire_time) 
        {
            switch (iter->second->mode()) 
            {
                case kWriteCompleteMode :
                case kSafeWriteMode :
                case kReplicateMode :
                case kWriteMode :
                {
                    LOGV(LL_INFO,  "come into *writeMode:%d", 
                                iter->second->mode());
                    Block b = Block(iter->second->block_id(),
                                    iter->second->block_length(),
                                    iter->second->block_version());
                    fs_dataset_->CloseFile(b, iter->second->blockfile_fd(),
                              iter->second->metafile_fd(), iter->second->mode());
                    // handle the connection
                    Singleton<AmFrame>::Instance().CloseEndPoint(
                                                iter->second->end_point());
                    LOGV(LL_INFO, "close endpoint;blkid:%d,fd:%d",
                         iter->first, iter->second->end_point().GetFd());
                    block_cache_.erase(iter++);
                    break;
                }
                case kReadMode : 
                {
                    LOGV(LL_INFO,  "come into KReadMode");
                    Block b = Block(iter->second->block_id(), 0, 
                                    iter->second->block_version());
                    fs_dataset_->CloseFile(b, iter->second->blockfile_fd(),
                              iter->second->metafile_fd(), iter->second->mode());
                    block_cache_.erase(iter++);
                    LOGV(LL_INFO,  "erase by read ");
                    break;
                }
                default : 
                {
                    LOGV(LL_INFO,  "invalid status mode! %d", iter->second->mode());
                    block_cache_.erase(iter++);
                    break;
                }
            }//end of switch
        }//end of if
        else
            iter++;
    }
    rw_lock_.wlock()->unlock();
}

int64_t FSDataInterface::BlockIdToFileId(int64_t block_id)
{
    return fs_dataset_->BlockIdToFileId(block_id);
}
}//end of namespace dataserver
}//end of namespace bladestore
