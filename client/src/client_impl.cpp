/*
 * Copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: client_impl.cpp
 * Description: the class ClientImpl.
 */

#include <fcntl.h>
#include <pthread.h>
#include "mempool.h"
#include "client_impl.h"
#include "blade_net_util.h"
#include "client_lease_checker.h"
#include "client_conf.h"
#include "client_lru_cache.h"
#include "located_blocks.h"
#include "client_cache_node.h"
#include "client_current_block.h"
#include "blade_file_info.h"
#include "blade_crc.h"
#include "is_valid_path_packet.h"
#include "get_block_locations_packet.h"
#include "abandon_block_packet.h"
#include "add_block_packet.h"
#include "renew_lease_packet.h"
#include "complete_packet.h"
#include "get_listing_packet.h"
#include "mkdir_packet.h"
#include "create_packet.h"
#include "write_packet_packet.h"
#include "write_pipeline_packet.h"
#include "read_block_packet.h"
#include "get_file_info_packet.h"
#include "delete_file_packet.h"
#include "rename_packet.h"

namespace bladestore
{
namespace client
{
ClientImpl::ClientImpl()
{
    ds_conn_handler_ = NULL;
    lru_cache_ = NULL;
    lease_checker_ = NULL;
    client_conf_ = NULL;
    current_block_ = NULL;
    chunk_buffer_ = NULL;
}

ClientImpl::~ClientImpl()
{
    DisconnectToNameServer();

    if(NULL != lease_checker_) {
        pthread_t lc_tid = lease_checker_->GetThreadId();
        int pcret = pthread_cancel(lc_tid);
        if (pcret)
            LOGV(LL_ERROR, "LeaseChecker thread cancel error");
        delete lease_checker_;
        lease_checker_ = NULL;
    }
    if(NULL != lru_cache_) {
        delete lru_cache_;
        lru_cache_ = NULL;
    }
    if(NULL != client_conf_) {
        delete client_conf_;
        client_conf_ = NULL;
    }
    if(NULL != current_block_) {
        delete current_block_;
        current_block_ = NULL;
    }
    if(NULL != chunk_buffer_) {
        delete chunk_buffer_;
        chunk_buffer_ = NULL;
    }
    MemPoolDestory();
}

int32_t ClientImpl::Init()
{
    lru_cache_ = new LRUCache();
    lease_checker_ = new LeaseChecker(this);
    client_conf_ = new ClientConf();
    current_block_ = new CurrentBlock();
    chunk_buffer_ = new ChunkBuffer();
    if (NULL == lru_cache_ || NULL == lease_checker_ || NULL == client_conf_ || NULL == current_block_)
        return BLADE_ERROR;

    if (BLADE_ERROR == client_conf_->Init()) {
        fprintf(stderr, "Config file init error");
        abort();
    }

    MemPoolInit();
    string nameserver_addr = client_conf_->nameserver_addr();
    string log_dir_path = client_conf_->log_dir_path();
    int32_t log_max_file_size = client_conf_->log_max_file_size();
    int32_t log_max_file_count = client_conf_->log_max_file_count();
    int32_t cache_max_size = client_conf_->cache_max_size();
    chunk_buffer_->bufsz = client_conf_->buffer_size();

    ::pandora::Log::logger_.SetLogPrefix(log_dir_path.c_str(), "client_log");
    ::pandora::Log::logger_.set_max_file_size(log_max_file_size);
    ::pandora::Log::logger_.set_max_file_count(log_max_file_count);

    if (BLADE_ERROR == lru_cache_->Init(cache_max_size))
        return BLADE_ERROR;

    if (BLADE_SUCCESS != lease_checker_->Start())
        return BLADE_ERROR;
    lease_checker_->UpdateLease(0);

    chunk_buffer_->buf = (char *)malloc(chunk_buffer_->bufsz);
    if (NULL == chunk_buffer_->buf)
        return BLADE_ERROR;

    int32_t connret = ConnectToNameServer(nameserver_addr);
    if (BLADE_ERROR == connret)
        return BLADE_ERROR;
    return BLADE_SUCCESS;
}

int32_t ClientImpl::MakeDirectory(const string& dirname)
{
    int64_t parentfid;
    string name;
    if (BLADE_ERROR == GetPathComponents(dirname, &parentfid, name, true))
        return CLIENT_INVALID_PATH;

    LOGV(LL_DEBUG, "Making dir: %s, parentfid: %ld.", dirname.c_str(), parentfid);
    MkdirPacket *p = new MkdirPacket(parentfid, name);
    assert(p);
    ResponseSync *response = new ResponseSync(OP_MKDIR);
    assert(response);

    BladePacket *ret_packet = RequestToNameServer(p, response, conn_handler_);
    if (!ret_packet) {
        LOGV(LL_ERROR, "Mkdir:RequestToNameServer returned NULL.");
        delete response;
        // could have to reconnect to nameserver
        if (BladeNetUtil::GetSockError(conn_handler_->end_point().GetFd())) {
            LOGV(LL_ERROR, "There is a socket error, reconnect to nameserver.");
            DisconnectToNameServer();
            int connret = ConnectToNameServer(client_conf_->nameserver_addr());
            if (BLADE_ERROR == connret)
                LOGV(LL_ERROR, "ReconnectToNameServer error");
        }
        return -1;
    }
    MkdirReplyPacket *reply = static_cast<MkdirReplyPacket*>(ret_packet);
    int32_t ret = reply->ret_code();
    int64_t file_id = reply->file_id();
    delete response;
    switch (ret) {
        case RET_SUCCESS:
            LOGV(LL_INFO, "MKDIR with ret SUCCESS.");
            lru_cache_->AddCacheNode(dirname, file_id, BLADE_FILE_TYPE_DIR);
            return ret;
        case RET_DIR_EXIST:
            LOGV(LL_ERROR, "MKDIR with ret DIR_EXIST.");
            return -ret;
        case RET_INVALID_DIR_NAME:
            LOGV(LL_ERROR, "MKDIR with ret INVALID_DIR_NAME.");
            return -ret;
        case RET_INVALID_PARAMS:
            LOGV(LL_ERROR, "MKDIR with ret INVALID_PARAMS.");
            return -ret;
        default:
            LOGV(LL_ERROR, "MKDIR with UNEXPECTED ERROR.");
            return -1;
    }
}

int32_t ClientImpl::IsValidPath(const string& dirname)
{
    int64_t parentfid;
    string name;
    if (BLADE_ERROR == GetPathComponents(dirname, &parentfid, name, false))
        return CLIENT_INVALID_PATH;

    IsValidPathPacket *p = new IsValidPathPacket(parentfid, name);
    assert(p);
    ResponseSync *response = new ResponseSync(OP_ISVALIDPATH);
    assert(response);

    BladePacket *ret_packet = RequestToNameServer(p, response, conn_handler_);
    if (!ret_packet) {
        LOGV(LL_ERROR, "IsValidPath:RequestToNameServer returned NULL.");
        delete response;

        if (BladeNetUtil::GetSockError(conn_handler_->end_point().GetFd())) {
            LOGV(LL_ERROR, "There is a socket error, reconnect to nameserver.");
            DisconnectToNameServer();
            int connret = ConnectToNameServer(client_conf_->nameserver_addr());
            if (BLADE_ERROR == connret)
                LOGV(LL_ERROR, "ReconnectToNameServer error");
        }
        return -1;
    }

    IsValidPathReplyPacket *reply = static_cast<IsValidPathReplyPacket*>(ret_packet);
    int32_t ret = reply->ret_code();
    delete response;
    switch (ret) {
        case RET_SUCCESS:
            LOGV(LL_INFO, "IS_VALID_PATH with ret SUCCESS.");
            return ret;
        case RET_DIR_NOT_EXIST:
            LOGV(LL_ERROR, "IS_VALID_PATH with ret DIR_NOT_EXIST.");
            return -ret;
        case RET_NOT_DIR:
            LOGV(LL_ERROR, "IS_VALID_PATH with ret NOT_DIR.");
            return -ret;
        default:
            LOGV(LL_ERROR, "IS_VALID_PATH with UNEXPECTED ERROR.");
            return -1;
    }
}

vector<string> ClientImpl::GetListing(const string& dirname)
{
    int64_t parentfid;
    string name;
    if (BLADE_ERROR == GetPathComponents(dirname, &parentfid, name, false))
        return vector<string>();

    if (!name.empty()) {
        FileInfo finfo = GetFileInfo(dirname);
        if (finfo.get_file_id() >= ROOTFID) {
            parentfid = finfo.get_file_id();
            name.clear();
        }
    }

    GetListingPacket *p = new GetListingPacket(parentfid, name);
    assert(p);
    ResponseSync *response = new ResponseSync(OP_GETLISTING);
    assert(response);

    BladePacket *ret_packet = RequestToNameServer(p, response, conn_handler_);
    if (!ret_packet) {
        LOGV(LL_ERROR, "GetListing:RequestToNameServer returned NULL.");
        delete response;

        if (BladeNetUtil::GetSockError(conn_handler_->end_point().GetFd())) {
            LOGV(LL_ERROR, "There is a socket error, reconnect to nameserver.");
            DisconnectToNameServer();
            int connret = ConnectToNameServer(client_conf_->nameserver_addr());
            if (BLADE_ERROR == connret)
                LOGV(LL_ERROR, "ReconnectToNameServer error");
        }
        return vector<string>();
    }
    GetListingReplyPacket *reply = static_cast<GetListingReplyPacket*>(ret_packet);
    switch (reply->ret_code()) {
        case RET_SUCCESS:
            LOGV(LL_INFO, "GET_LISTING with ret SUCCESS.");
            break;
        case RET_NO_FILES:
            LOGV(LL_INFO, "GET_LISTING with ret NO_FILES.");
            break;
        case RET_FILE_NOT_EXIST:
            LOGV(LL_ERROR, "GET_LISTING with ret FILE_NOT_EXIST.");
            break;
        default:
            LOGV(LL_ERROR, "GET_LISTING with UNEXPECTED ERROR.");
            LOGV(LL_ERROR, "Maybe this diretory has too many items.");
    }
    vector<string> ret = reply->file_names();
    delete response;
    return ret;
}

int64_t ClientImpl::Create(const string& filename, int8_t oflag,
                          int16_t replication)
{
    int64_t parentfid;
    string name;
    if (BLADE_ERROR == GetPathComponents(filename, &parentfid, name, true))
        return CLIENT_INVALID_PATH;

    CreatePacket *p = new CreatePacket(parentfid, name, oflag, replication);
    assert(p);
    ResponseSync *response = new ResponseSync(OP_CREATE);
    assert(response);

    BladePacket *ret_packet = RequestToNameServer(p, response, conn_handler_);
    if (!ret_packet) {
        LOGV(LL_ERROR, "Create:RequestToNameServer returned NULL.");
        delete response;

        if (BladeNetUtil::GetSockError(conn_handler_->end_point().GetFd())) {
            LOGV(LL_ERROR, "There is a socket error, reconnect to nameserver.");
            DisconnectToNameServer();
            int connret = ConnectToNameServer(client_conf_->nameserver_addr());
            if (BLADE_ERROR == connret)
                LOGV(LL_ERROR, "ReconnectToNameServer error");
        }
        return -1;
    }
    CreateReplyPacket *reply = static_cast<CreateReplyPacket*>(ret_packet);
    int64_t ret = reply->ret_code();
    int64_t fid = reply->file_id();
    delete response;
    switch (ret) {
        case RET_SUCCESS:
            current_block_->set_offset(0);
            current_block_->set_file_id(fid);
            current_block_->set_block_offset(0);
            current_block_->set_length(0);
            current_block_->set_file_length(0);
            LOGV(LL_INFO, "CREATE with ret SUCCESS.filename: %s, fid: %ld.",
                 filename.c_str(), fid);
            break;
        case RET_FILE_EXIST:
            LOGV(LL_ERROR, "CREATE with ret FILE_EXIST, fid:%ld.", fid);
            return -ret;
        case RET_INVALID_PARAMS:
            LOGV(LL_ERROR, "CREATE with ret INVALID_PARAMS.");
            return -ret;
        case RET_INVALID_DIR_NAME:
            LOGV(LL_ERROR, "CREATE with ret INVALID_DIR_NAME.");
            return -ret;
        case RET_NOT_FILE:
            LOGV(LL_ERROR, "CREATE with ret NOT_FILE.");
            return -ret;
        case RET_CREATE_FILE_ERROR:
            LOGV(LL_ERROR, "CREATE with ret CREATE_FILE_ERROR.");
            return -ret;
        default:
            LOGV(LL_ERROR, "CREATE with UNEXPECTED ERROR.");
            return -1;
    }

    LOGV(LL_DEBUG, "AddCacheNode in Create, filename: %s, fid: %ld.",
         filename.c_str(), fid);
    lru_cache_->AddCacheNode(filename, fid, BLADE_FILE_TYPE_FILE);

    if (kSafeWriteMode == oflag) {
        current_block_->set_mode(oflag);
    } else {
        current_block_->set_mode(kWriteMode);
    }

    return fid;
}

int64_t ClientImpl::Open(string filename, int8_t oflag)
{
    CacheNode *file_cache = lru_cache_->LookupByFileName(filename);
    if (NULL == file_cache)
        file_cache = lru_cache_->AddCacheNode(filename, 0, BLADE_FILE_TYPE_FILE);

    if (file_cache->file_type_ != BLADE_FILE_TYPE_FILE) {
        LOGV(LL_ERROR, "NOT a regular file, open error.");
        return CLIENT_OPEN_FILE_ERROR;
    }

    if (file_cache->fid_ < ROOTFID) {
        LOGV(LL_INFO, "Request to nameserver for file %s.", filename.c_str());
        LocatedBlockSet lb = GetBlockLocationsFromNS(filename, 0,
                                                     BLADE_PREFETCHSIZE);
        if (lb.fid_) {
            // refresh cache node
            file_cache->RefreshCacheNode(lb);
            lru_cache_->AddToFileIDMap(file_cache);
            current_block_->set_file_id(lb.fid_);
            current_block_->set_offset(0);
            current_block_->set_block_offset(0);
            current_block_->set_file_length(lb.file_length_);
            current_block_->set_mode(kReadMode);
            return lb.fid_;
        } else {
            LOGV(LL_ERROR, "File open error.");
            return CLIENT_OPEN_FILE_ERROR;
        }
    } else {
        LOGV(LL_INFO, "File %s already cached, file id: %ld.",
             filename.c_str(), file_cache->fid_);
        //lru_cache_->AddToFileIDMap(file_cache);
        current_block_->set_file_id(file_cache->fid_);
        current_block_->set_offset(0);
        current_block_->set_block_offset(0);
        current_block_->set_file_length(file_cache->file_length_);
        current_block_->set_mode(kReadMode);
        return file_cache->fid_;
    }
}

ssize_t ClientImpl::Read(int64_t file_id, void *buf, size_t nbytes)
{
    ssize_t already_read_len = 0;
    bool needto_connect_ds = true;
    if (file_id <= 0 || buf == NULL || nbytes < 0) {
        LOGV(LL_ERROR, "Illegal parameters.");
        return already_read_len;
    }

    //read file meta info from cache
    CacheNode *file_cache = lru_cache_->LookupByFileID(file_id);
    if (NULL == file_cache) {
        LOGV(LL_ERROR, "File not open. file id:%ld", file_id);
        return already_read_len;
    }

    int64_t previous_offset;
    int64_t lseek_offset = current_block_->lseek_offset();
    current_block_->set_lseek_offset(0);
    if (lseek_offset)
        previous_offset = lseek_offset;
    else
        previous_offset = current_block_->offset() + current_block_->block_offset();

    int64_t file_left_len = file_cache->file_length_ - previous_offset;
    if (!file_left_len) {
        LOGV(LL_INFO, "Reached the end of the File. file id:%ld.", file_id);
        if (ds_conn_handler_)
            DisconnectToDataServer();
        return already_read_len;
    }
    LocatedBlockSet blocks = GetBlockLocations(file_cache, previous_offset);

    //read data from Dataserver, read from one block then another
    bool flag_loop = true;

    // cotrol the times to refresh lru when read error, default: 1
    int32_t refresh_num = CLIENT_LRU_REFRESH_WHEN_ERROR;

    for (vector<LocatedBlock>::const_iterator iter = blocks.located_block_.begin();
         iter != blocks.located_block_.end() && flag_loop && (refresh_num >= 0);) {

        LOGV(LL_DEBUG, "READ NowReadBlock :%ld",iter->block_id());
        LOGV(LL_DEBUG, "READ Nowdataserver_num: %d.", iter->dataserver_num());

        //if read the same block, don't need to connect Dataserver again
        if (iter->block_id() == current_block_->block_id()) {
            needto_connect_ds = false;
            if (lseek_offset)
                current_block_->set_block_offset(lseek_offset % BLADE_BLOCK_SIZE);
        } else {
            current_block_->set_block_id(iter->block_id());
            current_block_->set_block_version(iter->block_version());
            if (lseek_offset)
                current_block_->set_block_offset(lseek_offset % BLADE_BLOCK_SIZE);
            else
            	current_block_->set_block_offset(0);
            current_block_->set_offset(iter->offset());
            current_block_->set_length(iter->length());
            needto_connect_ds = true;
        }
        lseek_offset = 0;

        int64_t needto_get_len = current_block_->length() - current_block_->block_offset();
        //if buffer has no enough data left
        if (needto_get_len >= (int64_t)(nbytes - already_read_len)) {
            needto_get_len = nbytes - already_read_len;
            flag_loop = false;
        }
        //set block offset
        int64_t needto_read_offset = current_block_->block_offset();
        if (needto_get_len + needto_read_offset > current_block_->length())
            needto_get_len = current_block_->length() - needto_read_offset;

        //connect to Dataserver
        //if cannot connect to Dataserver, need to switch to another one
        if (!ds_conn_handler_)
            needto_connect_ds = true;

        //connect to the Dataserver in list
        int8_t ds_num = 0; 
        int8_t dataserver_num = iter->dataserver_num();
        while (ds_num < iter->dataserver_num()) {
            int32_t conn_error = 0;
            if (needto_connect_ds) {
                if (ds_conn_handler_)
                    DisconnectToDataServer();
                conn_error = ConnectToDataServer(iter->dataserver_id()[ds_num]);
            }
            if (conn_error) {
                needto_connect_ds = true;
                ++ds_num;
                continue;
            }
            //send read block packet
            int32_t unit_total_count = (needto_get_len + BLADE_MAX_PACKET_DATA_LENGTH - 1) / BLADE_MAX_PACKET_DATA_LENGTH;
            ResponseSync *response = new ResponseSync(OP_READBLOCK, unit_total_count);
            LOGV(LL_DEBUG, "blk_id:%ld offset:%ld len:%ld", iter->block_id(), needto_read_offset, needto_get_len);
            int64_t read_block_len = ReadBlock(response, needto_read_offset, needto_get_len);
            LOGV(LL_DEBUG, "ReadBlock :%ld read_block_len: %ld", iter->block_id(), read_block_len);

            if (read_block_len <= 0) {
                LOGV(LL_ERROR, "Read block return 0. block id:%ld", current_block_->block_id());
                if (response) {
                    delete response;
                    response = NULL;
                }
                // In next loop client will reconnect dataserver
                needto_connect_ds = true;
                ++ds_num;
                continue;
            }
            bool ret_wait = WaitForRWPackets(response, OP_READBLOCK, (char *)buf, already_read_len);
 
            if (!ret_wait || response->response_error()) {
                LOGV(LL_ERROR, "error in read packet to ds.");
                if (response) {
                    delete response;
                    response = NULL;
                }
                needto_connect_ds = true;
                ++ds_num;
                continue;
            } else {
                LOGV(LL_DEBUG, "success in read packet to ds.");
                if (response) {
                    delete response;
                    response = NULL;
                }
            }

            already_read_len += read_block_len;
            current_block_->set_block_offset(current_block_->block_offset() + read_block_len);
            LOGV(LL_DEBUG, "already_read_len:%d, read_block_len:%ld.",already_read_len, read_block_len);
            ++iter;
            break;
        }//end while

        //when all the Dataserver cannot be connected
        if (ds_num == dataserver_num) {
            if (refresh_num > 0) {
                LOGV(LL_INFO, "No dataserver response. block id:%ld, refresh.", current_block_->block_id());
                // the first time, refresh lru cache
                LocatedBlockSet lb = GetBlockLocationsFromNS(
                        file_cache->filename_, current_block_->offset(),
                        file_cache->file_length_ - current_block_->offset());
                if (lb.fid_ > 0)
                    file_cache->RefreshCacheNode(lb);

                --refresh_num;
            } else {
                LOGV(LL_ERROR, "No dataserver response. block id:%ld.", current_block_->block_id());
                return already_read_len;
            }
        }
    }  // end for
    return already_read_len;
}

ssize_t ClientImpl::Write(int64_t file_id, void *buf, size_t nbytes)
{
    if (current_block_->mode() == kSafeWriteMode) {
        LOGV(LL_DEBUG, "safe write.");
        return WriteToServer(file_id, buf, nbytes);
    }
    if (chunk_buffer_->start + nbytes < chunk_buffer_->bufsz) {
        memcpy(chunk_buffer_->buf + chunk_buffer_->start, (char *)buf, nbytes);
        chunk_buffer_->start += nbytes;
        LOGV(LL_DEBUG, "normal small write. now buffer length:%ld", chunk_buffer_->start);
        return nbytes;
    }

    if (chunk_buffer_->start + nbytes == chunk_buffer_->bufsz) {
        LOGV(LL_DEBUG, "normal write. need to write to server.");
        memcpy(chunk_buffer_->buf + chunk_buffer_->start, (char *)buf, nbytes);
        ssize_t ret = WriteToServer(file_id, chunk_buffer_->buf, chunk_buffer_->bufsz);
        ssize_t previous = chunk_buffer_->start;
        chunk_buffer_->start = 0;
        return ret - previous;
    } else {
        LOGV(LL_DEBUG, "large write. need to write to server multitimes.");
        size_t send_out = 0;
        size_t data_left = 0;
        ssize_t ret = 0;

        do {
            memcpy(chunk_buffer_->buf + chunk_buffer_->start, (char *)buf, chunk_buffer_->bufsz - chunk_buffer_->start);
            data_left = chunk_buffer_->start + nbytes - chunk_buffer_->bufsz;
            ret = WriteToServer(file_id, chunk_buffer_->buf, chunk_buffer_->bufsz);
            send_out += ret - chunk_buffer_->start;
            chunk_buffer_->start = 0;
        } while (ret != (ssize_t)chunk_buffer_->bufsz);

        return send_out;
    }
}

// now only used in read
off_t ClientImpl::Lseek(int64_t file_id, off_t offset, int64_t whence)
{
    if (file_id <= ROOTFID) {
        LOGV(LL_ERROR, "Invalid file id");
        return -1;
    } else if (file_id != current_block_->file_id()) {
        LOGV(LL_ERROR, "file %ld not opened.", file_id);
        return -1;
    } else if (current_block_->mode() != kReadMode) {
        LOGV(LL_ERROR, "not in read mode");
        return -1;
    }

    //TODO if multithread read&write supportd, should store the offset of all known files
    off_t resultoff = 0;
    switch (whence) {
        case SEEK_SET:
            resultoff = offset;
            break;
        case SEEK_CUR:
            resultoff = current_block_->offset() + current_block_->block_offset() + offset;
            break;
        case SEEK_END:
            resultoff = current_block_->file_length() + offset;
            break;
        default:
            LOGV(LL_ERROR, "Lseek invalid param");
            return -1;
    }

    //if (resultoff >= 0 && resultoff <= current_block_->file_length()) {
    if (resultoff >= 0 && resultoff <= current_block_->file_length()) {
        //current_block_->set_block_offset(resultoff % BLADE_BLOCK_SIZE);
        //current_block_->set_offset((resultoff / BLADE_BLOCK_SIZE) * BLADE_BLOCK_SIZE);
        current_block_->set_lseek_offset(resultoff);
		//printf("blockoffset: %ld, offset: %ld.\n", current_block_->block_offset(), current_block_->offset());
        //return current_block_->offset() + current_block_->block_offset();
        return resultoff;
    } else {
        LOGV(LL_ERROR, "Lseek Invalid offset");
        return -1;
    }
}

int32_t ClientImpl::Close(int64_t file_id)
{
    if (current_block_->file_id() != file_id) {
        LOGV(LL_ERROR, "file %ld not opened.", file_id);
        return -1;
    }

    if (current_block_->mode() == kWriteMode) {
        if (chunk_buffer_->start != 0) {
            LOGV(LL_INFO, "%ld data left, now write to server.", chunk_buffer_->start);
            ssize_t ret = WriteToServer(file_id, chunk_buffer_->buf, chunk_buffer_->start);
            if (ret != chunk_buffer_->start) {
                chunk_buffer_->start = 0;
                return -1;
            }
            chunk_buffer_->start = 0;
        } else {
            if (current_block_->block_id() == 0) {  // Writting file length == 0
                LOGV(LL_WARN, "Didn't write or read data?");
                return BLADE_SUCCESS;
            }
        }

        if (RET_SUCCESS != Complete(file_id, current_block_->block_id(), current_block_->length())) {
            LOGV(LL_ERROR, "Block id :%ld of fileid: %ld not completed", current_block_->block_id(), file_id);
            if (RET_SUCCESS != AbandonBlock(current_block_->block_id())) {
                LOGV(LL_ERROR, "AbondonBlock error, block id: %ld.", current_block_->block_id());
            }
            LOGV(LL_INFO, "AbandonBlock, now file length: %ld.", current_block_->file_length());
            current_block_->CleanAll();
            return -1;
        }
    }
    if (current_block_->mode() == kSafeWriteMode) {
        if (current_block_->block_id() == 0) {  // Writting file length == 0
            LOGV(LL_WARN, "Didn't write or read data?");
            return BLADE_SUCCESS;
        }

        if (RET_SUCCESS != Complete(file_id, current_block_->block_id(), current_block_->length())) {
            LOGV(LL_ERROR, "Block id :%ld of fileid: %ld not completed", current_block_->block_id(), file_id);
            current_block_->CleanAll();
            return -1;
        }
    }
    if (current_block_->mode() == kReadMode) {
        if (current_block_->block_id() == 0) {  // Reading file length == 0
            LOGV(LL_WARN, "Didn't write or read data?");
            return BLADE_SUCCESS;
        }
    }
    current_block_->CleanAll();
    return BLADE_SUCCESS;
}

const FileInfo ClientImpl::GetFileInfo(const string& filename)
{
    int64_t parentfid;
    string name;
    if (BLADE_ERROR == GetPathComponents(filename, &parentfid, name, false)) {
        LOGV(LL_ERROR, "CLIENT_INVALID_PATH");
        return FileInfo();
    }

    GetFileInfoPacket *p = new GetFileInfoPacket(parentfid, name);
    assert(p);
    ResponseSync *response = new ResponseSync(OP_GETFILEINFO);
    assert(response);

    BladePacket *ret_packet = RequestToNameServer(p, response, conn_handler_);
    if (!ret_packet) {
        LOGV(LL_ERROR, "GetFileInfo:RequestToNameServer returned NULL.");
        FileInfo ret = FileInfo();
        delete response;

        if (BladeNetUtil::GetSockError(conn_handler_->end_point().GetFd())) {
            LOGV(LL_ERROR, "There is a socket error, reconnect to nameserver.");
            DisconnectToNameServer();
            int connret = ConnectToNameServer(client_conf_->nameserver_addr());
            if (BLADE_ERROR == connret)
                LOGV(LL_ERROR, "ReconnectToNameServer error");
        }
        return ret;
    }
    GetFileInfoReplyPacket *reply = static_cast<GetFileInfoReplyPacket*>(ret_packet);
    FileInfo ret = reply->file_info();

    switch (reply->ret_code()) {
        case RET_SUCCESS:
            LOGV(LL_INFO, "GET_FILE_INFO with ret SUCCESS.");
            if(!name.empty()) {
                lru_cache_->AddCacheNode(filename, ret.get_file_id(), ret.get_file_type());
            }
            break;
        case RET_FILE_NOT_EXIST:
            LOGV(LL_ERROR, "GET_FILE_INFO with ret FILE_NOT_EXIST.");
            break;
        case RET_INVALID_PARAMS:
            LOGV(LL_ERROR, "GET_FILE_INFO with ret INVALID_PARAMS.");
            break;
        default:
            LOGV(LL_ERROR, "GET_FILE_INFO with UNEXPECTED ERROR.");
    }
    delete response;
    return ret;
}

int32_t ClientImpl::DeleteFile(const string& filename)
{
    int64_t parentfid;
    string name;
    if (BLADE_ERROR == GetPathComponents(filename, &parentfid, name, false))
        return CLIENT_INVALID_PATH;

    DeleteFilePacket *p = new DeleteFilePacket(parentfid, name, filename);
    assert(p);
    ResponseSync *response = new ResponseSync(OP_DELETEFILE);
    assert(response);

    BladePacket *ret_packet = RequestToNameServer(p, response, conn_handler_);
    if (!ret_packet) {
        LOGV(LL_ERROR, "DeleteFile:RequestToNameServer returned NULL.");
        delete response;

        if (BladeNetUtil::GetSockError(conn_handler_->end_point().GetFd())) {
            LOGV(LL_ERROR, "There is a socket error, reconnect to nameserver.");
            DisconnectToNameServer();
            int connret = ConnectToNameServer(client_conf_->nameserver_addr());
            if (BLADE_ERROR == connret)
                LOGV(LL_ERROR, "ReconnectToNameServer error");
        }
        return -1;
    }
    DeleteFileReplyPacket *reply = static_cast<DeleteFileReplyPacket*>(ret_packet);
    int32_t ret = reply->ret_code();
    delete response;
    switch (ret) {
        case RET_SUCCESS:
            lru_cache_->DeleteCacheNode(filename);
            LOGV(LL_INFO, "DELETE_FILE with ret SUCCESS.");
            return ret;
        case RET_DIR_NOT_EMPTY:
            LOGV(LL_ERROR, "DELETE_FILE with ret DIR_NOT_EMPTY.");
            return -ret;
        case RET_LAST_BLOCK_NO_COMPLETE:
            LOGV(LL_ERROR,
                 "DELETE_FILE with ret LAST_BLOCK_NO_COMPLETE.");
            return -ret;
        case RET_FILE_NOT_EXIST:
            LOGV(LL_ERROR, "DELETE_FILE with ret FILE_NOT_EXIST.");
            return -ret;
        case RET_DIR_NOT_EXIST:
            LOGV(LL_ERROR, "DELETE_FILE with ret DIR_NOT_EXIST.");
            return -ret;
        case RET_NOT_DIR:
            LOGV(LL_ERROR, "DELETE_FILE with ret NOT_DIR.");
            return -ret;
        case RET_NOT_FILE:
            LOGV(LL_ERROR, "DELETE_FILE with ret NOT_FILE.");
            return -ret;
        case RET_NOT_PERMITTED:
            LOGV(LL_ERROR, "DELETE_FILE with ret NOT_PERMITTED.");
            return -ret;
        case RET_GETALLOC_ERROR:
            LOGV(LL_ERROR, "DELETE_FILE with ret GETALLOC_ERROR.");
            return -ret;
        default:
            LOGV(LL_ERROR, "DELETE_FILE with UNEXPECTED ERROR.");
            return -1;
    }
}

int32_t ClientImpl::RenewLease(int64_t block_id)
{
    RenewLeasePacket *p = new RenewLeasePacket(block_id);
    assert(p);
    ResponseSync *response = new ResponseSync(OP_RENEWLEASE);
    assert(response);

    BladePacket *ret_packet = RequestToNameServer(p, response, conn_handler_);
    if (!ret_packet) {
        LOGV(LL_ERROR, "RenewLease:RequestToNameServer returned NULL.");
        delete response;
        return -1;
    }
    RenewLeaseReplyPacket *reply = static_cast<RenewLeaseReplyPacket*>(ret_packet);
    int32_t ret = reply->get_ret_code();
    delete response;
    switch (ret) {
        case RET_SUCCESS:
            LOGV(LL_INFO, "RENEW_LEASE with ret SUCCESS.");
            return ret;
        case RET_BLOCK_NOT_EXIST:
            LOGV(LL_ERROR, "RENEW_LEASE with ret BLOCK_NOT_EXIST.");
            return -ret;
        default:
            LOGV(LL_ERROR, "RENEW_LEASE with UNEXPECTED ERROR.");
            return -1;
    }
}

int32_t ClientImpl::ConnectToNameServer(const string& nameserver_addr)
{
    conn_handler_ = new ClientStreamSocketHandler(BladeCommonData::amframe_);
    assert(conn_handler_);
    LOGV(LL_INFO, "Connectting to NameServer: %s.", nameserver_addr.c_str());
    int32_t error = BladeCommonData::amframe_.AsyncConnect(
                            nameserver_addr.c_str(), conn_handler_);
    LOGV(LL_INFO, "error: %d.", error);
    if (error < 0) {
        LOGV(LL_ERROR, "AsyncConnect error for connection to nameserver.");
        return BLADE_ERROR;
    } else {
        LOGV(LL_INFO, "Connect to nameserver successfully.");
        return BLADE_SUCCESS;
    }
}

void ClientImpl::DisconnectToNameServer()
{
    int32_t error =
            BladeCommonData::amframe_.CloseEndPoint(conn_handler_->end_point());
    if (!error) {
        LOGV(LL_INFO, "Close connection to nameserver successfully.");
    } else {
        LOGV(LL_INFO, "Close connection to nameserver failed, error=%d", error);
    }
}

int32_t ClientImpl::ConnectToDataServer(uint64_t dataserver_id)
{
    string ds_ip_port = BladeNetUtil::AddrToString(dataserver_id);
    LOGV(LL_INFO, "Connect to dataserver, DataserverId: %ld.", dataserver_id);
    LOGV(LL_INFO, "Dataserver ip && port: %s.", ds_ip_port.c_str());

    ds_conn_handler_ = new ClientStreamSocketHandler(BladeCommonData::amframe_);
    assert(ds_conn_handler_);
    int32_t error = BladeCommonData::amframe_.AsyncConnect(ds_ip_port.c_str(),
                                                           ds_conn_handler_);

    if (error < 0) {
        LOGV(LL_ERROR, "AsyncConnect error for connection to dataserver.");
        return CLIENT_CAN_NOT_CONNECT_DATASERVER;
    } else {
        LOGV(LL_INFO, "Connect to Dataserver returned normally.");
        return 0;
    }
}

void ClientImpl::DisconnectToDataServer()
{
    int32_t error = BladeCommonData::amframe_.CloseEndPoint(ds_conn_handler_->end_point());
    if (!error) {
        LOGV(LL_INFO, "Close connection to dataserver successfully.");
        ds_conn_handler_ = NULL;
    } else {
        LOGV(LL_INFO, "Close connection to dataserver failed, error=%d", error);
    }
}

LocatedBlockSet ClientImpl::GetBlockLocationsFromNS(const string& filename,
                                              int64_t offset, int64_t length)
{
    LOGV(LL_DEBUG, "GetBlockLocationsFromNS, fileName: %s, offset: %ld, length: %ld.",
         filename.c_str(), offset, length);
    if (offset < 0 || length <= 0) {
        LOGV(LL_ERROR, "GetBlockLocationsFromNS: Invalid params");
        return LocatedBlockSet();
    }

    int64_t parentfid;
    string name;
    if (BLADE_ERROR == GetPathComponents(filename, &parentfid, name, false))
        return LocatedBlockSet();

    GetBlockLocationsPacket *p =
                    new GetBlockLocationsPacket(parentfid, name, offset, length);
    assert(p);
    ResponseSync *response = new ResponseSync(OP_GETBLOCKLOCATIONS);
    assert(response);

    BladePacket *ret_packet = RequestToNameServer(p, response, conn_handler_);
    if (!ret_packet) {
        LOGV(LL_ERROR, "GetBlockLocationsFromNS:RequestToNameServer returned NULL.");
        delete response;

        if (BladeNetUtil::GetSockError(conn_handler_->end_point().GetFd())) {
            LOGV(LL_ERROR, "There is a socket error, reconnect to nameserver.");
            DisconnectToNameServer();
            int connret = ConnectToNameServer(client_conf_->nameserver_addr());
            if (BLADE_ERROR == connret)
                LOGV(LL_ERROR, "ReconnectToNameServer error");
        }
        return LocatedBlockSet();
    }

    GetBlockLocationsReplyPacket *reply = static_cast<GetBlockLocationsReplyPacket*>(ret_packet);
    LocatedBlockSet ret(reply->file_id(),
                        reply->file_length(),
                        reply->v_located_block());

    switch (reply->ret_code()) {
        case RET_SUCCESS:
            LOGV(LL_INFO, "GET_BLOCK_LOCATIONS with ret SUCCESS.");
            break;
        case RET_NOT_FILE:
            LOGV(LL_ERROR, "GET_BLOCK_LOCATIONS with ret NOT_FILE.");
            break;
        case RET_INVALID_OFFSET:
            LOGV(LL_ERROR, "GET_BLOCK_LOCATIONS with ret INVALID_OFFSET.");
            break;
        case RET_INVALID_PARAMS:
            LOGV(LL_ERROR, "GET_BLOCK_LOCATIONS with ret INVALID_PARAMS.");
            break;
        case RET_LOOKUP_FILE_ERROR:
            LOGV(LL_ERROR, "GET_BLOCK_LOCATIONS with ret LOOKUP_FILE_ERROR.");
            break;
        case RET_GETALLOC_ERROR:
            LOGV(LL_ERROR, "GET_BLOCK_LOCATIONS with ret GETALLOC_ERROR.");
            break;
        default:
            LOGV(LL_ERROR, "GET_BLOCK_LOCATIONS with UNEXPECTED ERROR.");
    }
    delete response;
    LOGV(LL_DEBUG, "GetBlockLocationsFromNS ret, fid: %ld, file_length: %ld.",
         ret.fid_, ret.file_length_);
    return ret;
}

LocatedBlock ClientImpl::AddBlock(int64_t file_id)
{
    AddBlockPacket *p = new AddBlockPacket(file_id, current_block_->mode());
    assert(p);
    ResponseSync *response = new ResponseSync(OP_ADDBLOCK);
    assert(response);

    BladePacket *ret_packet = RequestToNameServer(p, response, conn_handler_);
    if (!ret_packet) {
        LOGV(LL_ERROR, "AddBlock:RequestToNameServer returned NULL.");
        delete response;

        if (BladeNetUtil::GetSockError(conn_handler_->end_point().GetFd())) {
            LOGV(LL_ERROR, "There is a socket error, reconnect to nameserver.");
            DisconnectToNameServer();
            int connret = ConnectToNameServer(client_conf_->nameserver_addr());
            if (BLADE_ERROR == connret)
                LOGV(LL_ERROR, "ReconnectToNameServer error");
        }
        return LocatedBlock();
    }

    AddBlockReplyPacket *reply = static_cast<AddBlockReplyPacket*>(ret_packet);
    LocatedBlock ret = reply->located_block();
    
    switch (reply->ret_code()) {
        case RET_SUCCESS:
            lease_checker_->UpdateLease(ret.block_id());
            LOGV(LL_INFO, "ADD_BLOCK with ret SUCCESS.");
            break;
        case RET_LAST_BLOCK_NO_COMPLETE:
            LOGV(LL_ERROR, "ADD_BLOCK with ret LAST_BLOCK_NO_COMPLETE.");
            break;
        case RET_FILE_COMPLETED:
            LOGV(LL_ERROR, "ADD_BLOCK with ret FILE_COMPLETED.");
            break;
        case RET_NO_DATASERVER:
            LOGV(LL_ERROR, "ADD_BLOCK with ret NO_DATASERVER.");
            break;
        case RET_INVALID_PARAMS:
            LOGV(LL_ERROR, "ADD_BLOCK with ret INVALID_PARAMS.");
            break;
        case RET_GETALLOC_ERROR:
            LOGV(LL_ERROR, "ADD_BLOCK with ret GETALLOC_ERROR.");
            break;
        case RET_ALOC_BLOCK_ERROR:
            LOGV(LL_ERROR, "ADD_BLOCK with ret ALOC_BLOCK_ERROR.");
            break;
        case RET_LAYOUT_INSERT_ERROR:
            LOGV(LL_ERROR, "ADD_BLOCK with ret LAYOUT_INSERT_ERROR.");
            break;
        default:
            LOGV(LL_ERROR, "ADD_BLOCK with UNEXPECTED ERROR.");
    }
    delete response;
    return ret;
}

int32_t ClientImpl::AbandonBlock(int64_t block_id)
{
    AbandonBlockPacket *p = new AbandonBlockPacket(current_block_->file_id(),
                                                   block_id);
    assert(p);
    ResponseSync *response = new ResponseSync(OP_ABANDONBLOCK);
    assert(response);

    BladePacket *ret_packet =  RequestToNameServer(p, response, conn_handler_);
    if (!ret_packet) {
        LOGV(LL_ERROR, "AbandonBlock:RequestToNameServer returned NULL.");
        delete response;

        if (BladeNetUtil::GetSockError(conn_handler_->end_point().GetFd())) {
            LOGV(LL_ERROR, "There is a socket error, reconnect to nameserver.");
            DisconnectToNameServer();
            int connret = ConnectToNameServer(client_conf_->nameserver_addr());
            if (BLADE_ERROR == connret)
                LOGV(LL_ERROR, "ReconnectToNameServer error");
        }
        return -1;
    }

    AbandonBlockReplyPacket *reply = static_cast<AbandonBlockReplyPacket*>(ret_packet);
    int32_t ret = reply->ret_code();
    delete response;
    switch (ret) {
        case RET_SUCCESS:
            LOGV(LL_INFO, "ABANDON_BLOCK with ret SUCCESS.");
            current_block_->set_block_offset(0);
            current_block_->set_length(0);
            current_block_->set_file_length(current_block_->offset());
            lease_checker_->UpdateLease(0);
            current_block_->set_block_id(0);
            return ret;
        case RET_INVALID_BLOCKID:
            LOGV(LL_ERROR, "ABANDON_BLOCK with ret INVALID_BLOCKID.");
            return -ret;
        case RET_BLOCK_NO_LEASE:
            LOGV(LL_ERROR, "ABANDON_BLOCK with ret BLOCK_NO_LEASE.");
            return -ret;
        case RET_FILE_NOT_EXIST:
            LOGV(LL_ERROR, "ABANDON_BLOCK with ret FILE_NOT_EXIST.");
            return -ret;
        case RET_GETALLOC_ERROR:
            LOGV(LL_ERROR, "ABANDON_BLOCK with ret GETALLOC_ERROR.");
            return -ret;
        case RET_ERROR:
            LOGV(LL_ERROR, "ABANDON_BLOCK with ret ERROR.");
            return -ret;
        default:
            LOGV(LL_ERROR, "ABANDON_BLOCK with UNEXPECTED ERROR.");
            return -1;
    }
}

int32_t ClientImpl::Complete(int64_t file_id, int64_t block_id,
                             int64_t block_size)
{
    if (current_block_->length()) {
        int64_t send_len = WritePacket(NULL, 1, 0, NULL); 
        if (send_len < 0) {
            LOGV(LL_ERROR, "WritePacket error when complete block %ld", block_id);
            return -1;
        }
    }
    LOGV(LL_DEBUG, "dataserver num: %d.", current_block_->GetDataServerNum());
    CompletePacket *p = new CompletePacket(file_id, block_id, block_size);
    assert(p);
    ResponseSync *response = new ResponseSync(OP_COMPLETE);
    assert(response);
    BladePacket *ret_packet = RequestToNameServer(p, response, conn_handler_);
    if (!ret_packet) {
        LOGV(LL_ERROR, "Complete:RequestToNameServer returned NULL.");
        delete response;

        if (BladeNetUtil::GetSockError(conn_handler_->end_point().GetFd())) {
            LOGV(LL_ERROR, "There is a socket error, reconnect to nameserver.");
            DisconnectToNameServer();
            int connret = ConnectToNameServer(client_conf_->nameserver_addr());
            if (BLADE_ERROR == connret)
                LOGV(LL_ERROR, "ReconnectToNameServer error");
        }
        return -1;
    }

    CompleteReplyPacket *reply = static_cast<CompleteReplyPacket*>(ret_packet);
    int32_t ret = reply->ret_code();
    LocatedBlock *current_write_block = new LocatedBlock(
                                    current_block_->block_id(),
                                    current_block_->block_version(),
                                    current_block_->offset(),
                                    current_block_->length(),
                                    current_block_->dataserver_id());
    assert(current_write_block);
    delete response;
    switch (ret) {
        case RET_SUCCESS:{
            LOGV(LL_INFO, "COMPLETE with ret SUCCESS, BlockID: %ld.", current_block_->block_id());
            CacheNode * file_cache = lru_cache_->LookupByFileID(file_id);
            if (NULL != file_cache){
                file_cache->RefreshCacheNode(*current_write_block);
//                LOGV(LL_DEBUG, "Cache, FileID: %ld.", file_id);
//                LOGV(LL_DEBUG, "Cache, block_id: %ld.", current_write_block->block_id());
//                LOGV(LL_DEBUG, "Cache, offset: %ld.", current_write_block->offset());
//                LOGV(LL_DEBUG, "Cache, length: %ld.", current_write_block->length());
//                LOGV(LL_DEBUG, "Cache, dataserver_num: %ld.", current_write_block->dataserver_num());
            }
            lease_checker_->UpdateLease(0);
            current_block_->set_block_id(0);
            current_block_->set_file_length(current_block_->offset()+current_block_->length());
            current_block_->set_length(0);
            current_block_->set_offset(0);
            current_block_->set_block_offset(0);
            if (ds_conn_handler_)
                DisconnectToDataServer();
            return ret;
        }
        case RET_INVALID_BLOCKID:
            LOGV(LL_ERROR, "COMPLETE with ret INVALID_BLOCKID.");
            return -ret;
        case RET_BLOCK_NO_LEASE:
            LOGV(LL_ERROR, "COMPLETE with ret BLOCK_NO_LEASE.");
            return -ret;
        case RET_INVALID_PARAMS:
            LOGV(LL_ERROR, "COMPLETE with ret INVALID_PARAMS.");
            return -ret;
        case RET_FILE_NOT_EXIST:
            LOGV(LL_ERROR, "COMPLETE with ret RET_FILE_NOT_EXIST.");
            return -ret;
        default:
            LOGV(LL_ERROR, "COMPLETE with UNEXPECTED ERROR.");
            return -1;
    }
}

LocatedBlockSet ClientImpl::GetBlockLocations(CacheNode * file_cache,
                                              int64_t offset)
{
    //先在缓存中找前面的，一直到找不到了再把offset设为现在的位置
    int64_t temp_offset = offset;
    int64_t length = file_cache->file_length_ - offset;
    LocatedBlockSet blocks = file_cache->ReadCacheNode(temp_offset, length);

    if (blocks.fid_ <= 0) {
        blocks = GetBlockLocationsFromNS(file_cache->filename_,
                                         temp_offset,
                                         length);
        if (blocks.fid_)
            file_cache->RefreshCacheNode(blocks);
    }
    length = file_cache->file_length_ - temp_offset;

    if(length > 0) {
        LocatedBlockSet temp_blocks = GetBlockLocationsFromNS(
                                                    file_cache->filename_,
                                                    temp_offset,
                                                    length);
        if (temp_blocks.fid_) {
            file_cache->RefreshCacheNode(temp_blocks);
            blocks.located_block_.insert(blocks.located_block_.end(),
                                         temp_blocks.located_block_.begin(),
                                         temp_blocks.located_block_.end());
        }
    }
    LOGV(LL_DEBUG, "temp offset :%ld and offset:%ld.", temp_offset,offset);
    return blocks;
}

int64_t ClientImpl::ReadBlock(ResponseSync *response, int64_t offset, int64_t length)
{
    int64_t block_id = current_block_->block_id();
    int64_t block_version = current_block_->block_version();
    int64_t sequence = 1;
    size_t offset_loop = offset;
    size_t length_loop = BLADE_MAX_PACKET_DATA_LENGTH;

    while (sequence <= response->unit_total_count()) {
        if ((int64_t)(offset_loop + length_loop) > (offset + length)) {
            length_loop = (offset + length) - offset_loop;
        }
        ReadBlockPacket *p = new ReadBlockPacket(block_id, block_version, (int64_t)offset_loop, length_loop, sequence);
        assert(p);
        if (BLADE_SUCCESS != p->Pack()) {
            LOGV(LL_ERROR, "pack ReadBlockPacket error.");
            delete p;
            return 0;
        }
        if (RWRequestToDataServer(p, sequence, response, ds_conn_handler_)) {
            LOGV(LL_INFO, "seq %ld send.", sequence);
            offset_loop += length_loop;
            ++sequence;
        } else {
            LOGV(LL_DEBUG, "return 0");
            return 0;
        }
    }
    LOGV(LL_DEBUG, "return %ld.", length);
    return length;
}

ssize_t ClientImpl::WriteToServer(int64_t file_id, void *buf, size_t nbytes)
{
    //need to send each packet
    size_t needto_send_len = BLADE_MAX_PACKET_DATA_LENGTH;
    //length left in one block
    size_t needto_write_len = BLADE_BLOCK_SIZE - current_block_->length();
    size_t already_send_len = 0;
    size_t already_write_len = 0;

    if (file_id <= ROOTFID || buf == NULL || nbytes <= 0) {
        LOGV(LL_ERROR, "Illegal parameters.");
        return -1;
    }
    if (current_block_->length() == BLADE_BLOCK_SIZE)
        needto_write_len = BLADE_BLOCK_SIZE;
    //consider buf size - nbytes is rather small
    if (nbytes < needto_write_len)
        needto_write_len = nbytes;
    if (nbytes < needto_send_len)
        needto_send_len = ((nbytes < needto_write_len) ? nbytes : needto_write_len);

    LOGV(LL_DEBUG, "start write data from buffer to Dataserver");
    while (already_write_len < nbytes) {
        if (!PrepareWritePacket(file_id))
            return already_write_len;

        already_send_len = 0;
        needto_send_len = BLADE_MAX_PACKET_DATA_LENGTH;
        if (nbytes < needto_send_len)
            needto_send_len = ((nbytes < needto_write_len)?nbytes:needto_write_len);
        if (needto_write_len < needto_send_len)
            needto_send_len = needto_write_len;

        //start to send data packet.send as a unit
        int32_t unit_total_count = (needto_write_len + BLADE_MAX_PACKET_DATA_LENGTH - 1) / BLADE_MAX_PACKET_DATA_LENGTH;
        int32_t unit_count = 1;
        ResponseSync *response = new ResponseSync(OP_WRITEPACKET, unit_total_count);
        assert(response);
        LOGV(LL_DEBUG, "needto_write_len:%d;needto_send_len:%d.", needto_write_len, needto_send_len);

        for (size_t offset_loop = 0; unit_count <= unit_total_count; ++unit_count) {
            LOGV(LL_DEBUG, "unit_total_count:%d;unit_count:%d.", unit_total_count, unit_count);
            LOGV(LL_DEBUG, "dataserver_num:%d.", current_block_->GetDataServerNum());

            for(int i = 0; i < (current_block_->GetDataServerNum()); i++) {
                LOGV(LL_DEBUG, "ds %d:%s", i+1, BladeNetUtil::AddrToString(current_block_->dataserver_id()[i]).c_str());
            }
            LOGV(LL_DEBUG, "blockid: %ld, block_version: %d, block_length:%ld",
                 current_block_->block_id(), current_block_->block_version(), current_block_->length());
            LOGV(LL_DEBUG, "needto_send_len: %d, sequence: %d", needto_send_len, unit_count);

            int64_t send_len = WritePacket(response, unit_count, needto_send_len,
                                ((char *)buf) + already_write_len + offset_loop);

            if (send_len < 0) {
                LOGV(LL_ERROR, "WritePacket error, BlockID: %ld, written:%ld",
                     current_block_->block_id(), current_block_->length());
                if (current_block_->mode() == kWriteMode) {
                    if (RET_SUCCESS != AbandonBlock(current_block_->block_id())) {
                        LOGV(LL_ERROR, "AbondonBlock error, block id: %ld.", current_block_->block_id());
                    }
                    LOGV(LL_INFO, "AbandonBlock, now file length: %ld.", current_block_->file_length());
                }
                return already_write_len;
            }

            current_block_->set_block_offset(current_block_->block_offset() + send_len);
            current_block_->set_length(current_block_->length() + send_len);
            current_block_->set_file_length(current_block_->file_length() + send_len);

            already_send_len += needto_send_len;
            offset_loop += needto_send_len;

            if ((already_send_len + needto_send_len) > needto_write_len)
                needto_send_len = (((needto_write_len - already_send_len) < 0) ? 0 : (needto_write_len - already_send_len));
            else
                needto_send_len = BLADE_MAX_PACKET_DATA_LENGTH;
        }//end for

        if (current_block_->mode() == kWriteMode) {
            if (false == WaitForRWPackets(response, OP_WRITEPACKET, NULL, 0)) {
                if (RET_SUCCESS != AbandonBlock(current_block_->block_id())) {
                    LOGV(LL_ERROR, "AbondonBlock error, block id: %ld.", current_block_->block_id());
                }
                LOGV(LL_INFO, "AbandonBlock, now file length: %ld.", current_block_->file_length());
            }
        }

        if (response->response_error()) {
            LOGV(LL_ERROR, "error in write packet to ds.");
            delete response;
            response = NULL;
            return -1;
        } else {
            delete response;
            response = NULL;
        }

        already_write_len += needto_write_len;
        //if the data length left in buffer is smaller than BLADE_BLOCK_SIZE
        if ((already_write_len + BLADE_BLOCK_SIZE) > nbytes)
            needto_write_len = nbytes - already_write_len;
        else
            needto_write_len = BLADE_BLOCK_SIZE;
    }//end while
    return already_write_len;
}

int32_t ClientImpl::WritePipeline()
{
    int64_t file_id = current_block_->file_id();
    int64_t block_id = current_block_->block_id();
    int32_t block_version = current_block_->block_version();
    int8_t mode = current_block_->mode();
    const vector<uint64_t>& dataserver_id = current_block_->dataserver_id();

    WritePipelinePacket *p = new WritePipelinePacket(file_id, block_id, block_version, mode, dataserver_id);
    assert(p);
    if (BLADE_SUCCESS != p->Pack()) {
        LOGV(LL_ERROR, "pack WritePipelinePacket error.");
        delete p;
        return -1;
    }
    ResponseSync *response = new ResponseSync(OP_WRITEPIPELINE);
    assert(response);
    BladePacket *ret_packet = RequestToDataServer(p, response, ds_conn_handler_);
    if (!ret_packet) {
        LOGV(LL_ERROR, "WritePipeline:RequestToDataServer returned NULL.");
        delete response;
        return -1;
    }
    WritePipelineReplyPacket *reply = static_cast<WritePipelineReplyPacket*>(ret_packet);
    int32_t ret = reply->get_ret_code();
    delete response;
    switch (ret) {
        case RET_SUCCESS:
            LOGV(LL_INFO, "WRITE PIPELINE SUCCESS.");
            return ret;
        case RET_OPERATION_ON_GOING:
            LOGV(LL_ERROR, "WRITE PIPELINE with ret OPERATION_ON_GOING.");
            return -ret;
        case RET_BLOCK_FILE_EXISTS:
            LOGV(LL_ERROR, "WRITE PIPELINE with ret BLOCK_FILE_EXISTS.");
            return -ret;
        case RET_BLOCK_FILE_CREATE_ERROR:
            LOGV(LL_ERROR, "WRITE PIPELINE with ret BLOCK_FILE_CREATE_ERROR.");
            return -ret;
        default:
            LOGV(LL_ERROR, "WRITE PIPELINE with UNEXPECTED ERROR ret:%d.",ret);
            return -1;
    }
}

int64_t ClientImpl::WritePacket(ResponseSync *response, int32_t unit_count, int64_t length, char *data)
{
    LOGV(LL_DEBUG, "In Write_packet");
    int64_t block_id = current_block_->block_id();
    int32_t block_version = current_block_->block_version();
    int64_t block_offset = current_block_->length();
    int8_t  target_num = current_block_->GetDataServerNum();
    int64_t sequence = unit_count;
    int64_t chunk_num;
    int64_t checksum_len;

    //send empty packet to Dataserver to imform a block is completed
    if (length == 0 && data == NULL) {
        LOGV(LL_DEBUG, "this is the empty data packet.");
        WritePacketPacket *p = new WritePacketPacket(block_id, block_version,
                                        block_offset, 0, target_num, 0, 0, NULL, NULL);
        assert(p);
        if (BLADE_SUCCESS != p->Pack()) {
            LOGV(LL_ERROR, "pack WritePacketPacket error.");
            delete p;
            return -1;
        }
        ResponseSync *response_sync = new ResponseSync(OP_WRITEPACKET);
        assert(response_sync);
        response_sync->set_is_sync_packet(true);
        BladePacket *ret_packet = RequestToDataServer(p, response_sync, ds_conn_handler_);
        if (!ret_packet) {
            LOGV(LL_ERROR, "WritePacket:RequestToDataServer returned NULL.");
            delete response_sync;
            return -1;
        }
        WritePacketReplyPacket *reply = static_cast<WritePacketReplyPacket*>(ret_packet);
        int32_t ret = reply->get_ret_code();
        delete response_sync;
        if (ret == RET_SUCCESS)
            return 0;
        else {
            LOGV(LL_ERROR, "WRITE PACKET with UNEXPECTED ERROR.");
            return -1;
        }
    }//endif

    //checksum
    int64_t first_checksum_length = (BLADE_CHUNK_SIZE - (block_offset % BLADE_CHUNK_SIZE))%BLADE_CHUNK_SIZE;
    if (first_checksum_length) {
        chunk_num = (length - first_checksum_length + BLADE_CHUNK_SIZE)/BLADE_CHUNK_SIZE;
    } else {
        chunk_num = length/BLADE_CHUNK_SIZE;
    }
    if ((length - first_checksum_length)%BLADE_CHUNK_SIZE) {
        chunk_num++;
    }
    checksum_len = chunk_num * BLADE_CHECKSUM_SIZE;
    //send packet(not empty data packet)
    WritePacketPacket *p = new WritePacketPacket(block_id, block_version, block_offset,
                            sequence, target_num, length, checksum_len, data, NULL);
    assert(p);
    LOGV(LL_DEBUG, "WritePacket, block offset:%ld,length:%ld,checksum_len:%ld.", block_offset, length, checksum_len);
    int ret = p->Init();
    if (BLADE_SUCCESS != ret) {
        LOGV(LL_ERROR, "write packet init error.");
        delete p;
        return -1;
    }
    if (0 == first_checksum_length || length < first_checksum_length) {
        LOGV(LL_DEBUG, "crc generate, no first checksum length.");
        if (!Func::crc_generate(data, p->checksum(), length)) {
            LOGV(LL_ERROR, "crc generate error,block id:%ld sequence:%ld.", block_id, sequence);
            delete p;
            return -1;
        }
    } else {
        LOGV(LL_DEBUG, "crc generate, has first checksum length:%ld.", first_checksum_length);
        if (!Func::crc_generate(data, p->checksum(), length, first_checksum_length)) {
            LOGV(LL_ERROR, "crc generate error,block id:%ld sequence:%ld.", block_id, sequence);
            delete p;
            return -1;
        }
    }
    LOGV(LL_DEBUG, "block_id: %ld, block_v: %d, block_offset: %ld, seq:%ld.",
         block_id, block_version, block_offset, sequence);
    LOGV(LL_DEBUG, "sent data len: %ld, sent checksum len: %ld.", length, checksum_len);
    //send packet
    if (current_block_->mode() == kWriteMode) {
        if (BLADE_SUCCESS != p->Pack()) {
            LOGV(LL_ERROR, "pack WritePacketPacket error.");
            delete p;
            return -1;
        }
        if (RWRequestToDataServer(p, sequence, response, ds_conn_handler_))
            return length;
        else
            return -1;
    } else if (current_block_->mode() == kSafeWriteMode) {
        if (BLADE_SUCCESS != p->Pack()) {
            LOGV(LL_ERROR, "pack WritePacketPacket error.");
            delete p;
            return -1;
        }
        ResponseSync *response_safe = new ResponseSync(OP_WRITEPACKET);
        assert(response_safe);
        response_safe->set_is_sync_packet(true);
        BladePacket *ret_packet = RequestToDataServer(p, response_safe, ds_conn_handler_);
        if (!ret_packet) {
            LOGV(LL_ERROR, "WritePacket:RequestToDataServer returned NULL.");
            delete response_safe;
            return -1;
        }
        WritePacketReplyPacket *reply = static_cast<WritePacketReplyPacket*>(ret_packet);
        assert(reply);
        int32_t ret_safe = reply->get_ret_code();
        delete response_safe;
        if (ret_safe == RET_SUCCESS)
            return length;
        else {
            LOGV(LL_ERROR, "WRITE PACKET with UNEXPECTED ERROR.");
            return -1;
        }
    } else {
        return -1;
    }
}

bool ClientImpl::PrepareWritePacket(int64_t file_id)
{
    int64_t block_id;
    int64_t offset;
    int64_t length;
    int32_t ret;

    //build pipeline and addblock, try 3 times
    for (int8_t error_count = 0; error_count < 3; ++error_count) {
        length = current_block_->length();
        offset = current_block_->offset();
        block_id = current_block_->block_id();

        if ((length > 0)&&(length < BLADE_BLOCK_SIZE)) {
            return true;
        }

        //1.complete a block(if length reach the BLADE_BLOCK_SIZE)
        if (length == BLADE_BLOCK_SIZE) {
            if (RET_SUCCESS != Complete(file_id, block_id, length)) {
                LOGV(LL_INFO, "BlockID :%ld of fileid: %ld not completed", block_id, file_id);
                if (current_block_->mode() == kWriteMode) {
                    ret = AbandonBlock(block_id);
                    if (RET_SUCCESS != ret) {
                        //if Abandon error, NameServer will handle this
                        LOGV(LL_ERROR, "AbondonBlock error, BlockID: %ld.", block_id);
                    }
                }
                return false;
            }
        }

        //2.add block. apply a new block from Nameserver
        LocatedBlock current_write_block = AddBlock(file_id);
        current_block_->set_dataserver_id(current_write_block.dataserver_id());
        current_block_->set_length(0);
        if (current_block_->GetDataServerNum() == 0) {
            LOGV(LL_ERROR, "AddBlock error for file_id: %ld, block id: %ld.", file_id, current_block_->block_id());
            continue;
        }
        for(int i = 0; i < (current_block_->GetDataServerNum()); i++) {
            LOGV(LL_DEBUG, "ds %d:%s", i+1, BladeNetUtil::AddrToString(current_block_->dataserver_id()[i]).c_str());
        }
        current_block_->set_offset(current_write_block.offset());
        current_block_->set_block_offset(0);
        current_block_->set_block_id(current_write_block.block_id());
        current_block_->set_block_version(current_write_block.block_version());

        //3.connect to first Dataserver on the list(returned by add block)
        if (!(ConnectToDataServer(current_block_->dataserver_id()[0]))) {
            LOGV(LL_INFO,"Connect to Dataserver successfully.");

            //4.create a pipeline.
            ret = WritePipeline();
            if (RET_SUCCESS == ret) {
                return true;
            } else if (ret < 0) {
                LOGV(LL_ERROR, "WritePipeline error, abandon block id: %ld.", current_block_->block_id());
                ret = AbandonBlock(current_block_->block_id());
                if (RET_SUCCESS != ret) {
                    LOGV(LL_ERROR, "abondon block error, block id: %ld.", current_block_->block_id());
                    return false;
                }
            }
        } else {
            LOGV(LL_ERROR, "connect to ds error:%s.", BladeNetUtil::AddrToString(current_block_->dataserver_id()[0]).c_str());
            ret = AbandonBlock(current_block_->block_id());
            if (RET_SUCCESS != ret) {
                LOGV(LL_ERROR, "abondon block error, block id: %ld.", current_block_->block_id());
                return false;
            }
        }
    }//end for
    return false;
}

bool ClientImpl::WaitForRWPackets(ResponseSync *response, int16_t operation, char *buf, ssize_t already)
{
    if (response->unit_total_count() != response->response_count()) {
        response->cond().Lock();
        while (response->wait() == true) {
            response->cond().Wait();
        }
        response->cond().Unlock();
        if (response->response_count() != response->unit_total_count()) {
            LOGV(LL_DEBUG, "RW unit error to ds.blockid:%ld.", current_block_->block_id());
            return false;
        }
        LOGV(LL_DEBUG, "RW unit complete to ds.blockid:%ld,length:%ld.", current_block_->block_id(), current_block_->length());
    } else {
        LOGV(LL_DEBUG, "All expected packets are handled before wait.");
    }

    if (CheckAllRWPackets(response, operation)) {
        response->set_response_error(false);
    } else {
        return false;
    }

    if (operation == OP_WRITEPACKET) {
        return true;
    }

    int32_t buf_offset = 0;
    for (int i = 1; i <= response->unit_total_count(); i++) {
        map<int64_t, BladePacket*>::iterator iter = response->response_packets().find(i);
        if (iter != response->response_packets().end()) {
            ReadBlockReplyPacket *p = static_cast<ReadBlockReplyPacket*>(iter->second);
            assert(p);
            if (RET_SUCCESS != p->ret_code()) {
                LOGV(LL_ERROR, "read block ret:%d.", p->ret_code());
                return false;
            }
            int32_t data_length = p->data_length();
            memcpy((buf + already + buf_offset), p->data(), data_length);
            LOGV(LL_DEBUG, "ready to erase seq:%d.", i);
            response->EraseResponsePacket(iter);
            buf_offset += data_length;
        } else {
            return false;
        }
    }
    return true;
}

bool ClientImpl::CheckAllRWPackets(ResponseSync *response, int16_t operation)
{
    if (NULL == response)
        return false;
    if (response->request_packets().size() != response->response_packets().size()) {
        LOGV(LL_INFO, "request packet map not empty, need to resend.");

        map<int64_t, BladePacket*>::iterator it = response->response_packets().begin();
        while (it != response->response_packets().end()) {
            response->EraseRequestPacket(it->first);
            it++;
        }

        map<int64_t, BladePacket*>::iterator iter = response->request_packets().begin();
        while (iter != response->request_packets().end()) {
            LOGV(LL_DEBUG, "request packet %ld need to be resend.", iter->first);

            if (operation == OP_WRITEPACKET) {
                WritePacketPacket *packet = static_cast<WritePacketPacket*>(iter->second);
                assert(packet);
                WritePacketPacket *p = new WritePacketPacket(*packet);
                assert(p);
                ResponseSync *r = new ResponseSync(OP_WRITEPACKET);
                assert(r);

                r->set_is_sync_packet(true);
                int64_t seq = iter->first;
                BladePacket *ret_packet = RequestToDataServer(p, r, ds_conn_handler_);
                if (ret_packet) {
                    WritePacketReplyPacket *reply = static_cast<WritePacketReplyPacket*>(ret_packet);
                    if (reply->get_ret_code() == RET_SUCCESS) {
                        LOGV(LL_DEBUG, "resend packet seq %ld success.", seq);
                        LOGV(LL_DEBUG, "ready to erase seq:%ld.", seq);
                        response->EraseRequestPacket(iter++);
                        delete r;
                        continue;
                    }
                }
                LOGV(LL_ERROR, "resend packet seq %ld error.", seq);
                delete r;
                return false;
            } else if (operation == OP_READBLOCK) {
                ReadBlockPacket *packet = static_cast<ReadBlockPacket*>(iter->second);
                assert(packet);
                ReadBlockPacket *p = new ReadBlockPacket(*packet);
                assert(p);
                ResponseSync *r = new ResponseSync(OP_READBLOCK);
                assert(r);

                r->set_is_sync_packet(true);
                int64_t seq = iter->first;
                BladePacket *ret_packet = RequestToDataServer(p, r, ds_conn_handler_);
                if (ret_packet) {
                    ReadBlockReplyPacket *reply = static_cast<ReadBlockReplyPacket*>(ret_packet);
                    if (reply->ret_code() == RET_SUCCESS) {
                        LOGV(LL_DEBUG, "resend packet seq %ld success.", seq);
                        LOGV(LL_DEBUG, "ready to erase seq:%ld.", seq);
                        response->EraseRequestPacket(iter++);
                        ReadBlockReplyPacket *reply_copy = new ReadBlockReplyPacket(*reply);
                        response->AddResponsePacket(seq, reply_copy);
                        delete r;
                        continue;
                    }
                }
                LOGV(LL_ERROR, "resend packet seq %ld error.", seq);
                delete r;
                return false;
            } else {
                LOGV(LL_ERROR, "not a RW operation.");
            }
        }
    }
    return true;
}

int ClientImpl::GetPathComponents(const string origin_pathname, int64_t *parentfid,
                                  string &name, bool flag)
{
    CacheNode *file_cache = NULL;
    string pathname(origin_pathname);
    string path = "";

    string::size_type pathlen = pathname.size();
    string::size_type rslash = pathname.rfind('/');
    if (rslash + 1 == pathlen) {
        // path looks like: /.../.../; so get rid of the last '/'
        pathname.erase(rslash);
        pathlen = pathname.size();
        rslash = pathname.rfind('/');
    }

    if (pathlen ==0) {
        *parentfid = ROOTFID;
        name = "";
        return BLADE_SUCCESS;
    }

    // flag == true, name could not be empty
    if (flag) {
        path = pathname.substr(0, rslash);
        name = pathname.substr(rslash + 1);

        rslash = path.rfind('/');
        pathlen = path.size();
    } else {
        path = pathname.substr();
    }

    while (pathlen > 0) {
        file_cache = lru_cache_->LookupByFileName(path);
        if (file_cache && file_cache->fid_ >= ROOTFID) {
            *parentfid = file_cache->fid_;
            return BLADE_SUCCESS;
        }

        path = pathname.substr(0, rslash);
        name = pathname.substr(rslash + 1);
        rslash = path.rfind('/');
        pathlen = path.size();
    }

    if (pathlen == 0) {
        *parentfid = ROOTFID;
        return BLADE_SUCCESS;
    }

    return BLADE_ERROR;
}

}  // end of namespace client
}  // end of namespace bladestore
