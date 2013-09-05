/*
 * version : 1.0
 * author  : yeweimin
 * date    : 2012-6-13
 */

#include <gtest/gtest.h>

#include "blade_meta_manager.h"
#include "blade_packet_factory.h"
#include "create_packet.h"
#include "mkdir_packet.h"
#include "add_block_packet.h"
#include "is_valid_path_packet.h"
#include "get_file_info_packet.h"
#include "get_listing_packet.h"
#include "delete_file_packet.h"
#include "leave_ds_packet.h"
#include "bladestore_ops.h"
#include "blade_net_util.h"
#include "replicate_strategy.h"
#include "complete_packet.h"
#include "get_block_locations_packet.h"
#include "abandon_block_packet.h"
#include "bad_block_report_packet.h"
#include "block_report_packet.h"
#include "utility.h"
using namespace bladestore::nameserver;
using namespace bladestore::common;
using namespace bladestore::message;

namespace test
{
namespace nameserver
{
class FateNameServerImpl;

class FateMetaManager : public bladestore::nameserver::MetaManager 
{
public:
    FateMetaManager(FateNameServerImpl *fatenameserver);
    ~FateMetaManager();
    bool is_master();
    int32_t meta_blade_add_block(BladePacket *blade_packet, BladePacket * & blade_reply_packet);
    int32_t meta_blade_complete(BladePacket *blade_packet, BladePacket * & blade_reply_packet);
    int32_t meta_blade_get_block_locations(BladePacket *blade_packet, BladePacket * & blade_reply_packet);
    int32_t meta_blade_abandon_block(BladePacket *blade_packet, BladePacket * & blade_reply_packet);
    int32_t meta_blade_delete_file(BladePacket *blade_packet, BladePacket * & blade_reply_packet);

    FateNameServerImpl *fatenameserver_;
};

class FateNameServerImpl                                                                                                                
{
public:
    FateNameServerImpl(): lease_manager_(NULL)
    {
        meta_manager_ = new FateMetaManager(this);
    }   
    ~FateNameServerImpl()
    {   
        if(meta_manager_)
            delete meta_manager_;
        meta_manager_ = NULL;
    }   
    FateMetaManager * meta_manager()
    {   
        return meta_manager_;
    }   
    LeaseManager & get_lease_manager()
    {   
        return lease_manager_;
    }   
    LeaseManager lease_manager_;
    FateMetaManager * meta_manager_;
};

FateMetaManager::FateMetaManager(FateNameServerImpl *fatenameserver) : MetaManager(NULL), fatenameserver_(fatenameserver)
{   
}
FateMetaManager::~FateMetaManager()
{
}

bool FateMetaManager::is_master()
{   
    return false;
}

int32_t FateMetaManager::meta_blade_add_block(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "**********************IN ADDBLOCK*********************");                                                        
    AddBlockPacket *packet = static_cast<AddBlockPacket *>(blade_packet);

    int64_t file_id = packet->get_file_id();
    LOGV(LL_INFO, "fid: %ld", file_id);
    int8_t is_safe_write = packet->get_is_safe_write();

    int16_t ret_code = BLADE_ERROR;
    int64_t block_id = 0; 
    int64_t block_version = 0; 
    int64_t offset = 0; 
    vector<uint64_t> results;
    blade_reply_packet = new AddBlockReplyPacket(ret_code, block_id, block_version, offset, results);
    AddBlockReplyPacket *reply_packet = static_cast<AddBlockReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());

    MetaFattr * file_attr = get_meta_tree().getFattr(file_id);
    if(file_attr == NULL || file_attr->type == BLADE_DIR)
    {    
        LOGV(LL_ERROR, "invalid params!");
        ret_code = RET_INVALID_PARAMS;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }
    if((file_attr->filesize % BLADE_BLOCK_SIZE) != 0)
    {
        LOGV(LL_ERROR, "file is completed!");
        ret_code = RET_FILE_COMPLETED;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }

    if(0 == get_layout_manager().get_alive_ds_size())
    {
        LOGV(LL_ERROR, "ds num = 0!");
        ret_code = RET_NO_DATASERVER;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }   
    //检查文件最后一个块是否持有租约
    if(file_attr->chunkcount != 0 && true == fatenameserver_->get_lease_manager().has_file_valid_lease(file_id))
    {    
        LOGV(LL_ERROR, "last block is writing!");  
        ret_code = RET_LAST_BLOCK_NO_COMPLETE;
        reply_packet->set_ret_code(ret_code); 
        return BLADE_SUCCESS;
    }    
//    MetaChunkInfo *file_last_block = NULL;
//    if(file_attr->chunkcount != 0)
//    {
//        int err1 = get_meta_tree().getalloc(file_id, (file_attr->chunkcount - 1) * BLADE_BLOCK_SIZE, &file_last_block);
//        if(err1 != 0 || NULL == file_last_block)
//        {
//            LOGV(LL_ERROR, "getalloc error!");
//            ret_code = RET_GETALLOC_ERROR;
//            reply_packet->set_ret_code(ret_code);
//            return BLADE_SUCCESS;
//        }
//
//        if(true == fatenameserver_->get_lease_manager().has_valid_lease(file_last_block->chunkId))
//        {
//            LOGV(LL_ERROR, "last block is writing!");
//            ret_code = RET_LAST_BLOCK_NO_COMPLETE;
//            reply_packet->set_ret_code(ret_code);
//            return BLADE_SUCCESS;
//        }
//    }

    LOGV(LL_INFO, "offset:%ld", file_attr->filesize);
    int16_t num_replicas = file_attr->numReplicas;
    //分配DS
    int port;
    string client_ip = BladeNetUtil::addr_to_ip(BladeNetUtil::get_peer_id(packet->endpoint_->GetFd()), &port);
    LOGV(LL_INFO, "client_ip : %s", client_ip.c_str());
    ReplicateStrategy *replica_strategy = new ReplicateStrategy(get_layout_manager());
    replica_strategy->choose_target(num_replicas, client_ip, results);
    delete replica_strategy;

    int err = get_meta_tree().allocateChunkId(file_id, file_attr->filesize, &block_id, &block_version, &num_replicas);
    LOGV(LL_INFO, "num_replicas:%d, block_id:%ld", num_replicas, block_id);
    if(err != 0)
    {
        LOGV(LL_ERROR,"RET_ALOC_BLOCK_ERROR");
        ret_code = RET_ALOC_BLOCK_ERROR;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }   

    BladeBlockCollect *block_collect = new BladeBlockCollect();
    block_collect->block_id_ = block_id;
    block_collect->version_ = block_version;
    block_collect->replicas_number_ = num_replicas;
    for(vector<uint64_t>::iterator it = results.begin(); it != results.end(); ++it)     
        (block_collect->dataserver_set_).insert(*it);
    bool ret = get_layout_manager().insert(block_collect, true);
    if(false == ret)
    {
        LOGV(LL_ERROR, "layout insert error!");
        ret_code = RET_LAYOUT_INSERT_ERROR;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }

    get_layout_manager().server_mutex_.wlock()->lock();
    for(vector<uint64_t>::size_type ix = 0; ix != results.size(); ++ix)
        if(NULL != get_layout_manager().get_dataserver_info(results[ix]))
            (get_layout_manager().get_dataserver_info(results[ix])->blocks_hold_).insert(block_id);
    get_layout_manager().server_mutex_.wlock()->unlock();

    //add block to B+ tree
    get_meta_tree().assignChunkId(file_id, file_attr->filesize, block_id, block_version);
    //block num of file add 1
    ++(file_attr->chunkcount);
    LOGV(LL_DEBUG, "chunkcount :%ld", file_attr->chunkcount);
    
    //add lease 
    fatenameserver_->get_lease_manager().register_lease(file_id, block_id, block_version, is_safe_write);
    ret_code = RET_SUCCESS;
    LOGV(LL_INFO, "block_id:%ld, block_version:%ld, size:%ld", block_id, block_version, file_attr->filesize);
    for(vector<uint64_t>::size_type ix = 0; ix != results.size(); ++ix)
        LOGV(LL_INFO, "**************add_block ds_id : %ld, ip:%s", results[ix], 
                BladeNetUtil::addr_to_string(results[ix]).c_str());
    LocatedBlock *located_block = new LocatedBlock(block_id, block_version, file_attr->filesize, 0, results);
    reply_packet->set_ret_code(ret_code);
    reply_packet->set_dataserver_num(results.size());
    reply_packet->set_located_block(*located_block);
    delete located_block;
    return BLADE_SUCCESS;
}

int32_t FateMetaManager::meta_blade_complete(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "**********************IN COMPLETE*********************");    
    CompletePacket *packet = static_cast<CompletePacket *>(blade_packet);

    int64_t file_id = packet->get_file_id();
    int64_t block_id = packet->get_block_id();
    int32_t block_size = packet->get_block_size();
    LOGV(LL_INFO, "file_id : %ld, blockid:%ld , block_size:%d", file_id, block_id, block_size);

    int16_t ret_code = BLADE_ERROR;
    blade_reply_packet = new CompleteReplyPacket(ret_code);
    CompleteReplyPacket *reply_packet = static_cast<CompleteReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());
    if(block_size <= 0 || block_size > BLADE_BLOCK_SIZE) 
    {    
        LOGV(LL_ERROR, "block_size error");
        ret_code = RET_INVALID_PARAMS;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }    

    MetaFattr * file_attr = get_meta_tree().getFattr(file_id);
    if(NULL == file_attr)
    {    
        LOGV(LL_ERROR, "fid not exist!");
        ret_code = RET_FILE_NOT_EXIST;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }    
    //remove lease 
    int err1 = fatenameserver_->get_lease_manager().relinquish_lease(block_id);
    if(err1 != 0)
    {
        ret_code = RET_BLOCK_NO_LEASE;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }

    //add file's size                                                                                                               
    file_attr->set_file_size(file_attr->filesize + block_size);
    LOGV(LL_INFO, "filesize : %ld", file_attr->filesize);
    
    ret_code = RET_SUCCESS;
    reply_packet->set_ret_code(ret_code);
    return BLADE_SUCCESS;
}

int32_t FateMetaManager::meta_blade_get_block_locations(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "*************************IN GET_BLOCK_LOCATIONs*******************************");
    GetBlockLocationsPacket *packet = static_cast<GetBlockLocationsPacket *>(blade_packet);
    int64_t parent_id = packet->get_parent_id();
    string file_name = packet->get_file_name();
    int64_t offset = packet->get_offset();
    int64_t data_length = packet->get_data_length();
    LOGV(LL_INFO, "parent_id:%ld", parent_id);
    LOGV(LL_INFO, "file name:%s", file_name.c_str());
    LOGV(LL_INFO, "offset:%ld", offset);
    LOGV(LL_INFO, "length:%ld", data_length);

    int16_t ret_code = BLADE_ERROR;
    int64_t file_id = 0; 
    int64_t file_length = 0; 
    vector <LocatedBlock> result;
    blade_reply_packet = new GetBlockLocationsReplyPacket(ret_code, file_id, file_length, result);
    GetBlockLocationsReplyPacket *reply_packet = static_cast<GetBlockLocationsReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());

    if(offset < 0 || data_length <= 0 || file_name.empty() || file_name[0] == '/')
    {    
        LOGV(LL_ERROR, "invalid params!");
        ret_code = RET_INVALID_PARAMS;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }    

    MetaFattr * file_attr = get_meta_tree().lookupPath(parent_id, file_name); 
    if(file_attr == NULL)
    {    
        LOGV(LL_ERROR, "RET_LOOKUP_FILE_ERROR");
        ret_code = RET_LOOKUP_FILE_ERROR;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }
    else if(file_attr->type == BLADE_DIR)
    {
        LOGV(LL_ERROR, "it's a dir name");
        ret_code = RET_NOT_FILE;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }
    file_id = file_attr->fid;
    file_length = file_attr->filesize;
    LOGV(LL_INFO, "length:%ld , chunkcount:%ld ", file_length, file_attr->chunkcount);
    if(file_length == 0) { 
        reply_packet->set_ret_code(RET_SUCCESS);
        reply_packet->set_file_id(file_id);
        return BLADE_SUCCESS;
    } 
    int64_t start_count = offset / BLADE_BLOCK_SIZE + 1;
    if(start_count > file_attr->chunkcount)
    {
        LOGV(LL_ERROR, "invalid offset!");
        ret_code = RET_INVALID_OFFSET;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }
    int64_t end_count;
    if(0 == (offset + data_length) % BLADE_BLOCK_SIZE)
        end_count = (offset + data_length) / BLADE_BLOCK_SIZE;
    else 
        end_count = (offset + data_length) / BLADE_BLOCK_SIZE + 1;
    if(end_count > file_attr->chunkcount)
        end_count = file_attr->chunkcount;
    LOGV(LL_INFO, "start:%ld, end:%ld", start_count, end_count);

    //test if file's last block has lease
    if(file_attr->chunkcount != 0 && end_count == file_attr->chunkcount && true == fatenameserver_->get_lease_manager().has_file_valid_lease(file_id))
        --end_count; 
//    if(file_attr->chunkcount != 0 && end_count == file_attr->chunkcount)
//    {
//        LOGV(LL_DEBUG, "test last block is writing!");
//        MetaChunkInfo *file_last_block = NULL;
//        int err1 = get_meta_tree().getalloc(file_id, (file_attr->chunkcount - 1) * BLADE_BLOCK_SIZE, &file_last_block);
//        if(err1 != 0 || NULL == file_last_block)
//        {   
//            LOGV(LL_ERROR, "getalloc error!");
//            ret_code = RET_GETALLOC_ERROR;
//            reply_packet->set_ret_code(ret_code);
//            return BLADE_SUCCESS;
//        }
//        //has lease 
//        if(true == fatenameserver_->get_lease_manager().has_valid_lease(file_last_block->chunkId))
//            --end_count;
//    }

    vector <MetaChunkInfo *> file_blocks_info;
    get_meta_tree().getalloc(file_attr->fid, file_blocks_info);
    LOGV(LL_DEBUG, "file block size : %d", file_blocks_info.size());

    for(int64_t i = start_count; i <= end_count; ++i)
    {
        vector<uint64_t> ds_list;

        get_layout_manager().blocks_mutex_.rlock()->lock();
        LOGV(LL_DEBUG, "block id : %ld", file_blocks_info[i-1]->chunkId);
        set<uint64_t> ds_set =
            get_layout_manager().get_block_collect(file_blocks_info[i-1]->chunkId)->dataserver_set_;
        LOGV(LL_DEBUG, "ds list size : %d", ds_set.size());
        LOGV(LL_DEBUG, "blocks_map replica: %d", get_layout_manager().get_block_collect(file_blocks_info[i-1]->chunkId)->replicas_number_);
        for(set<uint64_t>::iterator it = ds_set.begin(); it != ds_set.end(); ++it)
            ds_list.push_back(*it);
        get_layout_manager().blocks_mutex_.rlock()->unlock();
        
        int port;
        string client_ip = BladeNetUtil::addr_to_ip(BladeNetUtil::get_peer_id(packet->endpoint_->GetFd()), &port);
        ReplicateStrategy *replica_strategy = new ReplicateStrategy(get_layout_manager()); 
        replica_strategy->get_pipeline(client_ip, ds_list);
        delete replica_strategy;

        int64_t block_length = 0;
        if((file_attr->filesize) % BLADE_BLOCK_SIZE && (i == file_attr->chunkcount)) 
            block_length = (file_attr->filesize) % BLADE_BLOCK_SIZE;
        else 
            block_length = BLADE_BLOCK_SIZE;
        LocatedBlock lb(file_blocks_info[i-1]->chunkId,
                file_blocks_info[i-1]->chunkVersion, file_blocks_info[i-1]->offset,
                block_length, ds_list);
        LOGV(LL_INFO, "block size : %ld", block_length);
        for(vector<uint64_t>::const_iterator cit = ds_list.begin(); cit != ds_list.end(); ++cit)
            LOGV(LL_INFO, "************ds ip : %s", BladeNetUtil::addr_to_string(*cit).c_str());
        result.push_back(lb);
    }
    ret_code = RET_SUCCESS;
    reply_packet->set_ret_code(ret_code);
    reply_packet->set_file_id(file_id);
    reply_packet->set_file_length(file_length);
    reply_packet->set_located_block_num(result.size());
    reply_packet->set_v_located_block(result);
    return BLADE_SUCCESS;
}

int32_t FateMetaManager::meta_blade_abandon_block(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "**********************IN ABANDONBLOCK*********************");             
    AbandonBlockPacket *packet = static_cast<AbandonBlockPacket *>(blade_packet);

    int64_t block_id = packet->block_id();
    int64_t fid = packet->file_id();
    LOGV(LL_INFO, "block_id: %ld", block_id);

    int16_t ret_code = BLADE_ERROR;
    blade_reply_packet = new AbandonBlockReplyPacket(ret_code);
    AbandonBlockReplyPacket *reply_packet = static_cast<AbandonBlockReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());

    int err1 = fatenameserver_->get_lease_manager().relinquish_lease(block_id);
    if(err1 != 0)
    {
        ret_code = RET_BLOCK_NO_LEASE;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }

    //remove blocks_map and server_map
    get_layout_manager().remove_block_collect(block_id);
    //remove B+ tree
    MetaFattr * file_attr = get_meta_tree().getFattr(fid);
    if(NULL == file_attr)
    {
        LOGV(LL_ERROR, "fid:%ld not exist!", fid);
        ret_code = RET_FILE_NOT_EXIST;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }
    MetaChunkInfo *file_last_block = NULL;
    int32_t err3 = get_meta_tree().getalloc(fid, (file_attr->chunkcount - 1) * BLADE_BLOCK_SIZE, &file_last_block);
    if(err3 != 0 || NULL == file_last_block)
    {
        LOGV(LL_ERROR, "getalloc error!");
        ret_code = RET_GETALLOC_ERROR;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }
    else if(file_last_block->chunkId != block_id)
    {
        ret_code = RET_INVALID_BLOCKID;
        return BLADE_SUCCESS;
    }
    get_meta_tree().delete_block(fid, file_last_block->offset, block_id);
    --(file_attr->chunkcount);
    ret_code = RET_SUCCESS;
    reply_packet->set_ret_code(ret_code);
    return BLADE_SUCCESS;
}

int32_t FateMetaManager::meta_blade_delete_file(BladePacket *blade_packet, BladePacket * & blade_reply_packet)
{
    LOGV(LL_INFO, "**********************IN DELETEFILE************************");        
    DeleteFilePacket *packet = static_cast<DeleteFilePacket *>(blade_packet);
    int64_t parent_id = packet->get_parent_id();
    string fname = packet->get_file_name();
    string path = packet->get_path();
    LOGV(LL_INFO, "parent_id:%ld, file_name:%s, path:%s", parent_id, fname.c_str(), path.c_str());

    int16_t ret_code = BLADE_ERROR;
    blade_reply_packet = new DeleteFileReplyPacket(ret_code);
    DeleteFileReplyPacket *reply_packet = static_cast<DeleteFileReplyPacket *>(blade_reply_packet);
    reply_packet->set_channel_id(packet->channel_id());

    if(fname[0] == '/') {
        LOGV(LL_ERROR, "fname is begin with /");
        ret_code = RET_INVALID_PARAMS;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }

    string file_name;
    int64_t dir_id;
    MetaFattr * file_attr = NULL;
    if(fname.empty()) {
        MetaDentry *dentry = get_meta_tree().getDentry(parent_id); //parent_id = file_id
        if(!dentry) {
            LOGV(LL_ERROR, "file don't exist!");
            ret_code = RET_FILE_NOT_EXIST;
            reply_packet->set_ret_code(ret_code);
            return BLADE_SUCCESS;
        }    
        dir_id = dentry->getDir();
        file_name = dentry->getName();
        file_attr = get_meta_tree().getFattr(parent_id);
    } else {
        string father_dir;
        if(bladestore::common::split_string(fname, '/', father_dir, file_name)) {
            LOGV(LL_INFO, "father_dir: %s, file_name:%s, has to lookupPath", father_dir.c_str(), file_name.c_str());
            MetaFattr *dir_attr = get_meta_tree().lookupPath(parent_id, father_dir);
            if(!dir_attr) {
                LOGV(LL_ERROR, "father_dir do not exist!");
                ret_code = RET_DIR_NOT_EXIST;
                reply_packet->set_ret_code(ret_code);
                return BLADE_SUCCESS;
            }    
            dir_id = dir_attr->fid;
        }    
        else {
            dir_id = parent_id;
            file_name = fname;
        }    
        file_attr = get_meta_tree().lookup(dir_id, file_name);
    }    

    if(NULL == file_attr) {
        LOGV(LL_ERROR, "file don't exist!");
        ret_code = RET_FILE_NOT_EXIST;
        reply_packet->set_ret_code(ret_code);
        return BLADE_SUCCESS;
    }
    if(file_attr->type == BLADE_DIR)
    {
        int err = get_meta_tree().rmdir(dir_id, file_name, path);
        if(err != 0)
        {
            if(err == -EPERM)
                ret_code = RET_NOT_PERMITTED;
            else if(err == -ENOENT)
                ret_code = RET_DIR_NOT_EXIST;
            else if(err == -ENOTEMPTY)
                ret_code = RET_DIR_NOT_EMPTY;
            else if(err == -ENOTDIR)
                ret_code = RET_NOT_DIR;
        }
        else 
            ret_code = RET_SUCCESS;
    }
    else if(file_attr->type == BLADE_FILE)
    {
        //test if file's last block is wrting!
        if(true == fatenameserver_->get_lease_manager().has_file_valid_lease(file_attr->fid))
        {
            LOGV(LL_ERROR, "last block is writing!");
            ret_code = RET_LAST_BLOCK_NO_COMPLETE;
            reply_packet->set_ret_code(ret_code);
            return BLADE_SUCCESS;
        }
//        MetaChunkInfo *file_last_block = NULL;
//        if(file_attr->chunkcount != 0)
//        {   
//            int err1 = get_meta_tree().getalloc(file_attr->fid, (file_attr->chunkcount - 1) * BLADE_BLOCK_SIZE, &file_last_block);
//            if(err1 != 0 || NULL == file_last_block)
//            {   
//                LOGV(LL_ERROR, "getalloc error!");
//                ret_code = RET_GETALLOC_ERROR;
//                reply_packet->set_ret_code(ret_code);
//                return BLADE_SUCCESS;
//            }   
//            if(true == fatenameserver_->get_lease_manager().has_valid_lease(file_last_block->chunkId))
//            {   
//                LOGV(LL_ERROR, "last block is writing!");
//                ret_code = RET_LAST_BLOCK_NO_COMPLETE;
//                reply_packet->set_ret_code(ret_code);
//                return BLADE_SUCCESS;
//            }   
//        } 

        //remove from b tree and map! 
        int err = get_meta_tree().remove(dir_id, file_name, path, NULL);
        if(err == -ENOENT)
            ret_code = RET_FILE_NOT_EXIST;
        else if(err == -EISDIR)
            ret_code = RET_NOT_FILE;
        else 
            ret_code = RET_SUCCESS;
    }
    else 
    {
        ret_code = RET_ERROR_FILE_TYPE;
    }
    reply_packet->set_ret_code(ret_code);  
    return BLADE_SUCCESS;
}


class TestMetaManager : public ::testing::Test
{
protected:
    static void SetUpTestCase()
    {
        nameserver = new FateNameServerImpl();
    }

    static void TearDownTestCase()
    {
        if(nameserver)
            delete nameserver;

        nameserver = NULL;
    }

    static FateNameServerImpl *nameserver;
    int32_t register_ds(int32_t rack_id, uint64_t ds_id)
    {
        DataServerInfo *ds_info = new DataServerInfo(rack_id, ds_id);
        ds_info->reset();
        ds_info->set_is_alive(true);
        ds_info->last_update_time_ = TimeUtil::GetTime();
        ds_info->dataserver_metrics_.total_space_ = 1000000000000000;
        ds_info->dataserver_metrics_.used_space_ = 0;
        ds_info->dataserver_metrics_.num_connection_ = 20; 
        ds_info->dataserver_metrics_.cpu_load_ = 2;

        nameserver->meta_manager()->get_layout_manager().server_mutex_.wlock()->lock();
        nameserver->meta_manager()->get_layout_manager().get_server_map().insert(SERVER_MAP::value_type(ds_id, ds_info));
        nameserver->meta_manager()->get_layout_manager().server_mutex_.wlock()->unlock();
                                                                                                
        nameserver->meta_manager()->get_layout_manager().racks_mutex_.wlock()->lock();
        RACKS_MAP_ITER rack_iter = nameserver->meta_manager()->get_layout_manager().get_racks_map().find(rack_id);
        if(rack_iter == nameserver->meta_manager()->get_layout_manager().get_racks_map().end())
        {   
            set<DataServerInfo *> temp;
            temp.insert(ds_info);
            nameserver->meta_manager()->get_layout_manager().get_racks_map().insert(RACKS_MAP::value_type(rack_id, temp));
            printf("**********************insert success!\n");
        }   
        else 
        {   
            (rack_iter->second).insert(ds_info);
        }   
        nameserver->meta_manager()->get_layout_manager().racks_mutex_.wlock()->unlock();

        return 0;
    }
};

FateNameServerImpl *  TestMetaManager::nameserver = NULL;

TEST_F(TestMetaManager, testRegisterDs)
{
    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    RegisterPacket *packet = new RegisterPacket(1, BladeNetUtil::string_to_addr("10.10.72.20"));
    packet->pack();
    BladePacket *blade_packet = packet_factory_->create_packet(OP_REGISTER);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    BladePacket *reply_packet = NULL;
    int16_t return_code = nameserver->meta_manager()->register_ds(blade_packet, reply_packet);
    EXPECT_EQ(BLADE_SUCCESS, return_code);

}

TEST_F(TestMetaManager, testUpdateDs)
{
    DataServerMetrics dataserver_metrics_;
    dataserver_metrics_.total_space_ = 1000000;
    dataserver_metrics_.used_space_ = 0;
    dataserver_metrics_.num_connection_ = 20;
    dataserver_metrics_.cpu_load_ = 2;
    HeartbeatPacket *packet = new HeartbeatPacket(1, BladeNetUtil::string_to_addr("10.10.72.20"), dataserver_metrics_);
    packet->pack();
    packet->get_net_data()->set_read_data(packet->content(), packet->length());
    packet->unpack();
    HeartbeatReplyPacket *reply_packet = new HeartbeatReplyPacket(BLADE_SUCCESS);;
    int16_t return_code = nameserver->meta_manager()->update_ds(packet, reply_packet);
    EXPECT_EQ(BLADE_SUCCESS, return_code);    
    EXPECT_EQ(20, nameserver->meta_manager()->get_layout_manager().get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.20"))->dataserver_metrics_.num_connection_);
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;

    packet = new HeartbeatPacket(1, BladeNetUtil::string_to_addr("10.10.72.25"), dataserver_metrics_);
    packet->pack();
    packet->get_net_data()->set_read_data(packet->content(), packet->length());
    packet->unpack();
    return_code = nameserver->meta_manager()->update_ds(packet, reply_packet);
    EXPECT_EQ(BLADE_ERROR, return_code);
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
}

TEST_F(TestMetaManager, testLeaveDs)
{
    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    LeaveDsPacket * packet = new LeaveDsPacket();
    packet->set_ds_id(BladeNetUtil::string_to_addr("10.10.72.20"));
    packet->pack();
    BladePacket * blade_packet = packet_factory_->create_packet(OP_LEAVE_DS);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    int return_code = nameserver->meta_manager()->leave_ds(blade_packet);
    EXPECT_EQ(BLADE_SUCCESS, return_code);
    //bool alive = (meta_manager->get_layout_manager().get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.20")))->is_alive_;
    bool alive = (nameserver->meta_manager()->get_layout_manager().get_server_map()[BladeNetUtil::string_to_addr("10.10.72.20")])->is_alive_;
    EXPECT_FALSE(alive);
    EXPECT_EQ(0, nameserver->meta_manager()->get_layout_manager().get_racks_map()[1].size());
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new LeaveDsPacket();
    packet->set_ds_id(BladeNetUtil::string_to_addr("10.10.72.200"));
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_LEAVE_DS);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    return_code = nameserver->meta_manager()->leave_ds(blade_packet);
    EXPECT_EQ(BLADE_ERROR, return_code);
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
}

TEST_F(TestMetaManager, testCreate)
{
    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    CreatePacket *packet = new CreatePacket(2, "file1", 0, 3);
    packet->pack();
    BladePacket *blade_packet = packet_factory_->create_packet(OP_CREATE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    BladePacket *reply_packet = NULL;
    nameserver->meta_manager()->meta_blade_create(blade_packet, reply_packet);
    CreateReplyPacket *create_reply = static_cast<CreateReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, create_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new CreatePacket(2, "file1/file2", 0, 3);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_CREATE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_create(blade_packet, reply_packet);
    CreateReplyPacket * create_packet = static_cast<CreateReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_INVALID_DIR_NAME, create_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new CreatePacket(2, "dir/file3", 0, 3);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_CREATE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_create(blade_packet, reply_packet);
    create_packet = static_cast<CreateReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_INVALID_DIR_NAME, create_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new CreatePacket(2, "file1", 0, 3);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_CREATE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_create(blade_packet, reply_packet);
    create_packet = static_cast<CreateReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_FILE_EXIST, create_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    MkdirPacket *mkdir_packet = new MkdirPacket(2, "test234");
    mkdir_packet->pack();
    blade_packet = packet_factory_->create_packet(OP_MKDIR);
    blade_packet->get_net_data()->set_read_data(mkdir_packet->content(), mkdir_packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_mkdir(blade_packet, reply_packet);
    MkdirReplyPacket *mkdir_reply = static_cast<MkdirReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, mkdir_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(mkdir_packet)
        delete mkdir_packet;
    mkdir_packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
    packet = new CreatePacket(2, "test234", 0, 3);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_CREATE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_create(blade_packet, reply_packet);
    create_packet = static_cast<CreateReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_NOT_FILE, create_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
}

TEST_F(TestMetaManager, testAddBlock)
{
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.1"));
    register_ds(1, BladeNetUtil::string_to_addr("10.10.72.2"));
    //register_ds(1, BladeNetUtil::string_to_addr("10.10.72.3"));
    register_ds(2, BladeNetUtil::string_to_addr("10.10.72.4"));
    //register_ds(2, BladeNetUtil::string_to_addr("10.10.72.5"));
    //register_ds(2, BladeNetUtil::string_to_addr("10.10.72.6"));
    //register_ds(3, BladeNetUtil::string_to_addr("10.10.72.7"));
    //register_ds(3, BladeNetUtil::string_to_addr("10.10.72.8"));
    //register_ds(3, BladeNetUtil::string_to_addr("10.10.72.9"));

    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    CreatePacket *creat_packet = new CreatePacket(2, "file5", 0, 3);
    creat_packet->pack();
    BladePacket *blade_packet = packet_factory_->create_packet(OP_CREATE);
    blade_packet->get_net_data()->set_read_data(creat_packet->content(), creat_packet->length());
    blade_packet->unpack();
    BladePacket *reply_packet = NULL;
    nameserver->meta_manager()->meta_blade_create(blade_packet, reply_packet);
    CreateReplyPacket *create_reply = static_cast<CreateReplyPacket *>(reply_packet);
    ASSERT_EQ(RET_SUCCESS, create_reply->get_ret_code());
    int64_t file_id = create_reply->get_file_id();
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(creat_packet)
        delete creat_packet;
    creat_packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    AddBlockPacket * packet = new AddBlockPacket(file_id, 0);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_ADDBLOCK);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    //SocketId id(1234567);
    blade_packet->endpoint_ = new AmFrame::StreamEndPoint();
    nameserver->meta_manager()->meta_blade_add_block(blade_packet, reply_packet);
    AddBlockReplyPacket *add_block_reply = static_cast<AddBlockReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, add_block_reply->get_ret_code());
    EXPECT_EQ(3, add_block_reply->get_dataserver_num());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new AddBlockPacket(1234, 0);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_ADDBLOCK);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    blade_packet->endpoint_ = new AmFrame::StreamEndPoint();
    nameserver->meta_manager()->meta_blade_add_block(blade_packet, reply_packet);
    add_block_reply = static_cast<AddBlockReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_INVALID_PARAMS, add_block_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
    
    packet = new AddBlockPacket(file_id, 0);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_ADDBLOCK);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();  
    blade_packet->endpoint_ = new AmFrame::StreamEndPoint();
    nameserver->meta_manager()->meta_blade_add_block(blade_packet, reply_packet);
    add_block_reply = static_cast<AddBlockReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_LAST_BLOCK_NO_COMPLETE, add_block_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

}

TEST_F(TestMetaManager, testComplete)
{
    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    CompletePacket * packet = new CompletePacket(5, 3, BLADE_BLOCK_SIZE+1);
    packet->pack();
    BladePacket *blade_packet = packet_factory_->create_packet(OP_COMPLETE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    BladePacket *reply_packet = NULL;
    nameserver->meta_manager()->meta_blade_complete(blade_packet, reply_packet);
    CompleteReplyPacket *complete_reply = static_cast<CompleteReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_INVALID_PARAMS, complete_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new CompletePacket(5, 3, 0);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_COMPLETE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_complete(blade_packet, reply_packet);
    complete_reply = static_cast<CompleteReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_INVALID_PARAMS, complete_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new CompletePacket(10, 3, BLADE_BLOCK_SIZE);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_COMPLETE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_complete(blade_packet, reply_packet);
    complete_reply = static_cast<CompleteReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_FILE_NOT_EXIST, complete_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new CompletePacket(5, 3, BLADE_BLOCK_SIZE);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_COMPLETE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_complete(blade_packet, reply_packet);
    complete_reply = static_cast<CompleteReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, complete_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new CompletePacket(5, 3, BLADE_BLOCK_SIZE);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_COMPLETE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_complete(blade_packet, reply_packet);
    complete_reply = static_cast<CompleteReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_BLOCK_NO_LEASE, complete_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    AddBlockPacket * add_block = new AddBlockPacket(5, 0);
    add_block->pack();
    blade_packet = packet_factory_->create_packet(OP_ADDBLOCK);
    blade_packet->get_net_data()->set_read_data(add_block->content(), add_block->length());
    blade_packet->unpack();
    blade_packet->endpoint_ = new AmFrame::StreamEndPoint();
    nameserver->meta_manager()->meta_blade_add_block(blade_packet, reply_packet);
    AddBlockReplyPacket * add_block_reply = static_cast<AddBlockReplyPacket *>(reply_packet);
    ASSERT_EQ(RET_SUCCESS, add_block_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(add_block)
        delete add_block;
    add_block = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new CompletePacket(5, 4, BLADE_BLOCK_SIZE-1);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_COMPLETE);   
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_complete(blade_packet, reply_packet);
    complete_reply = static_cast<CompleteReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, complete_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    add_block = new AddBlockPacket(5, 0);
    add_block->pack();
    blade_packet = packet_factory_->create_packet(OP_ADDBLOCK);
    blade_packet->get_net_data()->set_read_data(add_block->content(), add_block->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_add_block(blade_packet, reply_packet);
    add_block_reply = static_cast<AddBlockReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_FILE_COMPLETED, add_block_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(add_block)
        delete add_block;
    add_block = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

}

TEST_F(TestMetaManager, testGetBlockInfo)
{
    set<uint64_t> ds_list;
    int return_code = nameserver->meta_manager()->get_block_info(4, ds_list);
    ASSERT_EQ(BLADE_SUCCESS, return_code);
    EXPECT_EQ(3, ds_list.size());
}

TEST_F(TestMetaManager, testGetBlockLocation)
{
    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    GetBlockLocationsPacket * packet = new GetBlockLocationsPacket(2, "file5", 0, 2*BLADE_BLOCK_SIZE - 1);
    packet->pack();
    BladePacket *blade_packet = packet_factory_->create_packet(OP_GETBLOCKLOCATIONS);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    blade_packet->endpoint_ = new AmFrame::StreamEndPoint();
    BladePacket *reply_packet = NULL;
    nameserver->meta_manager()->meta_blade_get_block_locations(blade_packet, reply_packet);
    GetBlockLocationsReplyPacket *get_block_location = static_cast<GetBlockLocationsReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, get_block_location->get_ret_code());
    EXPECT_EQ(5, get_block_location->get_file_id());
    EXPECT_EQ(2*BLADE_BLOCK_SIZE-1, get_block_location->get_file_length());
    EXPECT_EQ(2, get_block_location->get_located_block_num());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
    
    packet = new GetBlockLocationsPacket(2, "file5", -1, 2*BLADE_BLOCK_SIZE - 1);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_GETBLOCKLOCATIONS);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_get_block_locations(blade_packet, reply_packet);
    get_block_location = static_cast<GetBlockLocationsReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_INVALID_PARAMS, get_block_location->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
    
    packet = new GetBlockLocationsPacket(2, "file5", 0, -1);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_GETBLOCKLOCATIONS);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_get_block_locations(blade_packet, reply_packet);
    get_block_location = static_cast<GetBlockLocationsReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_INVALID_PARAMS, get_block_location->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new GetBlockLocationsPacket(2, "", 0, 2*BLADE_BLOCK_SIZE - 1);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_GETBLOCKLOCATIONS);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_get_block_locations(blade_packet, reply_packet);
    get_block_location = static_cast<GetBlockLocationsReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_INVALID_PARAMS, get_block_location->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new GetBlockLocationsPacket(2, "add", 0, 2*BLADE_BLOCK_SIZE - 1);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_GETBLOCKLOCATIONS);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_get_block_locations(blade_packet, reply_packet);
    get_block_location = static_cast<GetBlockLocationsReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_LOOKUP_FILE_ERROR, get_block_location->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
    
    packet = new GetBlockLocationsPacket(2, "file5", 3*BLADE_BLOCK_SIZE, 2*BLADE_BLOCK_SIZE);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_GETBLOCKLOCATIONS);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_get_block_locations(blade_packet, reply_packet);
    get_block_location = static_cast<GetBlockLocationsReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_INVALID_OFFSET, get_block_location->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;    
}

TEST_F(TestMetaManager, testAbandonBlock)
{
    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    AddBlockPacket *add_block = new AddBlockPacket(3, 0);
    add_block->pack();
    BladePacket *blade_packet = packet_factory_->create_packet(OP_ADDBLOCK);
    blade_packet->get_net_data()->set_read_data(add_block->content(), add_block->length());
    blade_packet->unpack();
    BladePacket *reply_packet = NULL;
    blade_packet->endpoint_ = new AmFrame::StreamEndPoint();
    nameserver->meta_manager()->meta_blade_add_block(blade_packet, reply_packet);
    AddBlockReplyPacket *add_block_reply = static_cast<AddBlockReplyPacket *>(reply_packet);
    ASSERT_EQ(RET_SUCCESS, add_block_reply->get_ret_code());
    int64_t block_id = add_block_reply->get_located_block().block_id();
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(add_block)
        delete add_block;
    add_block = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    AbandonBlockPacket * packet = new AbandonBlockPacket(3, block_id);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_ABANDONBLOCK);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_abandon_block(blade_packet, reply_packet);
    AbandonBlockReplyPacket *abandon_reply = static_cast<AbandonBlockReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, abandon_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new AbandonBlockPacket(6,1234656);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_ABANDONBLOCK);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_abandon_block(blade_packet, reply_packet);
    abandon_reply = static_cast<AbandonBlockReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_BLOCK_NO_LEASE, abandon_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
}

TEST_F(TestMetaManager, testReportBadBlock)
{
    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    set<int64_t> bad_blocks;
    bad_blocks.insert(3);
    BadBlockReportPacket * packet = new BadBlockReportPacket(BladeNetUtil::string_to_addr("10.10.72.100"), bad_blocks);
    packet->pack();
    BladePacket *blade_packet = packet_factory_->create_packet(OP_BAD_BLOCK_REPORT);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    int32_t return_code = nameserver->meta_manager()->report_bad_block(blade_packet);
    EXPECT_EQ(BLADE_NEED_REGISTER, return_code);
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new BadBlockReportPacket(BladeNetUtil::string_to_addr("10.10.72.20"), bad_blocks);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_BAD_BLOCK_REPORT);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    return_code = nameserver->meta_manager()->report_bad_block(blade_packet);
    EXPECT_EQ(BLADE_NEED_REGISTER, return_code);
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    bad_blocks.clear();
    bad_blocks.insert(2000);
    packet = new BadBlockReportPacket(BladeNetUtil::string_to_addr("10.10.72.1"), bad_blocks);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_BAD_BLOCK_REPORT);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    return_code = nameserver->meta_manager()->report_bad_block(blade_packet);
    ASSERT_EQ(BLADE_SUCCESS, return_code);
    EXPECT_EQ(1, nameserver->meta_manager()->get_layout_manager().get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.1"))->blocks_to_remove_.size());
    EXPECT_EQ(2, nameserver->meta_manager()->get_layout_manager().get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.1"))->blocks_hold_.size());
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    bad_blocks.clear();
    bad_blocks.insert(3);
    packet = new BadBlockReportPacket(BladeNetUtil::string_to_addr("10.10.72.1"), bad_blocks);
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_BAD_BLOCK_REPORT);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    return_code = nameserver->meta_manager()->report_bad_block(blade_packet);
    ASSERT_EQ(BLADE_SUCCESS, return_code);
    EXPECT_EQ(1, nameserver->meta_manager()->get_layout_manager().get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.1"))->blocks_to_remove_.size());
    EXPECT_EQ(1, nameserver->meta_manager()->get_layout_manager().get_dataserver_info(BladeNetUtil::string_to_addr("10.10.72.1"))->blocks_hold_.size());
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
}

TEST_F(TestMetaManager, testReportBlocks)
{
    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    set<BlockInfo *> report_blocks;
    BlockInfo *block = new BlockInfo(3, 1);
    report_blocks.insert(block);
    BlockReportPacket *packet = new BlockReportPacket(BladeNetUtil::string_to_addr("10.10.72.123"), report_blocks);
    packet->pack();
    BladePacket *blade_packet = packet_factory_->create_packet(OP_BLOCK_REPORT);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    int return_code = nameserver->meta_manager()->report_blocks(blade_packet);
    EXPECT_EQ(BLADE_NEED_REGISTER, return_code);
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new BlockReportPacket(BladeNetUtil::string_to_addr("10.10.72.20"), report_blocks);
    packet->pack();
    packet->report_blocks_info().clear();
    blade_packet = packet_factory_->create_packet(OP_BLOCK_REPORT);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    return_code = nameserver->meta_manager()->report_blocks(blade_packet);
    EXPECT_EQ(BLADE_NEED_REGISTER, return_code);
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    BlockInfo * block1 = new BlockInfo(4, 1);
    BlockInfo * block2 = new BlockInfo(5, 1);
    //report_blocks.clear();
    //report_blocks.insert(new BlockInfo(3,1));
    //report_blocks.insert(new BlockInfo(4,1));
    report_blocks.insert(block1);
    report_blocks.insert(block2);
    packet = new BlockReportPacket(BladeNetUtil::string_to_addr("10.10.72.2"), report_blocks);
    packet->pack();
    packet->report_blocks_info().clear();
    blade_packet = packet_factory_->create_packet(OP_BLOCK_REPORT);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    return_code = nameserver->meta_manager()->report_blocks(blade_packet);
    EXPECT_EQ(BLADE_SUCCESS, return_code);
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL; 

    report_blocks.clear();
    //report_blocks.insert(block);
    report_blocks.insert(block1);
    packet = new BlockReportPacket(BladeNetUtil::string_to_addr("10.10.72.2"), report_blocks);
    packet->pack();
    packet->report_blocks_info().clear();
    blade_packet = packet_factory_->create_packet(OP_BLOCK_REPORT);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    return_code = nameserver->meta_manager()->report_blocks(blade_packet);
    EXPECT_EQ(BLADE_SUCCESS, return_code);
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
}

TEST_F(TestMetaManager, testMkdir)
{
    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    MkdirPacket * packet = new MkdirPacket(2, "dir1");
    packet->pack();
    BladePacket *blade_packet = packet_factory_->create_packet(OP_MKDIR);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    BladePacket *reply_packet = NULL;
    nameserver->meta_manager()->meta_blade_mkdir(blade_packet, reply_packet);
    MkdirReplyPacket *mkdir_packet = static_cast<MkdirReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, mkdir_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new MkdirPacket(2, "dir2/dir3");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_MKDIR);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_mkdir(blade_packet, reply_packet);
    mkdir_packet = static_cast<MkdirReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_INVALID_DIR_NAME, mkdir_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new MkdirPacket(2, "file1/dir4");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_MKDIR);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_mkdir(blade_packet, reply_packet);  
    mkdir_packet = static_cast<MkdirReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_INVALID_DIR_NAME, mkdir_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)                      
        delete packet;              
    packet = NULL;              
    if(blade_packet)                
        delete blade_packet;        
    blade_packet = NULL;

    packet = new MkdirPacket(2, "");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_MKDIR);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();  
    nameserver->meta_manager()->meta_blade_mkdir(blade_packet, reply_packet);  
    mkdir_packet = static_cast<MkdirReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_INVALID_PARAMS, mkdir_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)                      
        delete packet;              
    packet = NULL;              
    if(blade_packet)                
        delete blade_packet;        
    blade_packet = NULL;

    CreatePacket *creat_packet = new CreatePacket(2, "test123", 0, 3);
    creat_packet->pack();
    blade_packet = packet_factory_->create_packet(OP_CREATE);
    blade_packet->get_net_data()->set_read_data(creat_packet->content(), creat_packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_create(blade_packet, reply_packet);
    CreateReplyPacket *create_reply = static_cast<CreateReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, create_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(creat_packet)
        delete creat_packet;
    creat_packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
    packet = new MkdirPacket(2, "test123");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_MKDIR);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();  
    nameserver->meta_manager()->meta_blade_mkdir(blade_packet, reply_packet); 
    mkdir_packet = static_cast<MkdirReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_DIR_EXIST, mkdir_packet->get_ret_code());
    if(reply_packet) 
        delete reply_packet; 
    reply_packet = NULL; 
    if(packet)
        delete packet;
    packet = NULL;  
    if(blade_packet) 
        delete blade_packet;
    blade_packet = NULL;  
}

TEST_F(TestMetaManager, testIsValidPath)
{
    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    IsValidPathPacket * packet = new IsValidPathPacket(2, "dir1");
    packet->pack();
    BladePacket *blade_packet = packet_factory_->create_packet(OP_ISVALIDPATH);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    BladePacket *reply_packet = NULL;
    nameserver->meta_manager()->meta_blade_is_valid_path(blade_packet, reply_packet);
    IsValidPathReplyPacket *is_valid_path_packet = static_cast<IsValidPathReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, is_valid_path_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new IsValidPathPacket(2, "dir0");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_ISVALIDPATH);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length()); 
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_is_valid_path(blade_packet, reply_packet); 
    is_valid_path_packet = static_cast<IsValidPathReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_DIR_NOT_EXIST, is_valid_path_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new IsValidPathPacket(2, "file1");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_ISVALIDPATH);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length()); 
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_is_valid_path(blade_packet, reply_packet); 
    is_valid_path_packet = static_cast<IsValidPathReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_NOT_DIR, is_valid_path_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    reply_packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
}

TEST_F(TestMetaManager, testGetFileInfo)
{
    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    GetFileInfoPacket * packet = new GetFileInfoPacket(2, "file1");
    packet->pack();
    BladePacket *blade_packet = packet_factory_->create_packet(OP_GETFILEINFO);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    BladePacket *reply_packet = NULL;
    nameserver->meta_manager()->meta_blade_get_file_info(blade_packet, reply_packet);
    GetFileInfoReplyPacket *get_file_info_packet = static_cast<GetFileInfoReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, get_file_info_packet->get_ret_code());
    EXPECT_EQ(BLADE_FILE, get_file_info_packet->get_file_info().get_file_type());
    EXPECT_EQ(3, get_file_info_packet->get_file_info().get_num_replicas());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete packet;
    blade_packet = NULL;

    packet = new GetFileInfoPacket(2, "dir1");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_GETFILEINFO);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_get_file_info(blade_packet, reply_packet);
    get_file_info_packet = static_cast<GetFileInfoReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, get_file_info_packet->get_ret_code());
    EXPECT_EQ(BLADE_DIR, get_file_info_packet->get_file_info().get_file_type());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new GetFileInfoPacket(2, "abcc");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_GETFILEINFO);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_get_file_info(blade_packet, reply_packet);
    get_file_info_packet = static_cast<GetFileInfoReplyPacket *>(reply_packet);  
    EXPECT_EQ(RET_FILE_NOT_EXIST, get_file_info_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
}

TEST_F(TestMetaManager, testGetListing)
{
    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    GetListingPacket * packet = new GetListingPacket(2,"");
    packet->pack();
    BladePacket * blade_packet = packet_factory_->create_packet(OP_GETLISTING);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    BladePacket *reply_packet = NULL;
    nameserver->meta_manager()->meta_blade_get_listing(blade_packet,reply_packet);
    GetListingReplyPacket * get_listing_packet = static_cast<GetListingReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, get_listing_packet->get_ret_code());
    EXPECT_EQ(6, get_listing_packet->get_file_num());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new GetListingPacket(2, "adb");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_GETLISTING);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_get_listing(blade_packet,reply_packet);
    get_listing_packet = static_cast<GetListingReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_FILE_NOT_EXIST, get_listing_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new GetListingPacket(2, "file1");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_GETLISTING);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_get_listing(blade_packet,reply_packet);
    get_listing_packet = static_cast<GetListingReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_NOT_DIR, get_listing_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new GetListingPacket(2, "dir1");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_GETLISTING);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_get_listing(blade_packet,reply_packet);
    get_listing_packet = static_cast<GetListingReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, get_listing_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;
}

TEST_F(TestMetaManager, testDeleteFile)
{
    BladePacketFactory * packet_factory_ = new BladePacketFactory();
    DeleteFilePacket * packet = new DeleteFilePacket(2, "file1", "/file1");
    packet->pack();
    BladePacket * blade_packet = packet_factory_->create_packet(OP_DELETEFILE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    BladePacket * reply_packet = NULL;
    nameserver->meta_manager()->meta_blade_delete_file(blade_packet, reply_packet);
    DeleteFileReplyPacket *delete_file_packet = static_cast<DeleteFileReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, delete_file_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    MkdirPacket * mkdir_packet = new MkdirPacket(2, "dir1/dir10");
    mkdir_packet->pack();
    blade_packet = packet_factory_->create_packet(OP_MKDIR);
    blade_packet->get_net_data()->set_read_data(mkdir_packet->content(), mkdir_packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_mkdir(blade_packet, reply_packet);
    MkdirReplyPacket *mkdir_reply = static_cast<MkdirReplyPacket *>(reply_packet);
    ASSERT_EQ(RET_SUCCESS, mkdir_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(mkdir_packet)
        delete mkdir_packet;
    mkdir_packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new DeleteFilePacket(2, "dir1", "/dir1");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_DELETEFILE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_delete_file(blade_packet, reply_packet);
    delete_file_packet = static_cast<DeleteFileReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_DIR_NOT_EMPTY, delete_file_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new DeleteFilePacket(2, "dir1/dir10", "/dir1/dir10");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_DELETEFILE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_delete_file(blade_packet, reply_packet);
    delete_file_packet = static_cast<DeleteFileReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_SUCCESS, delete_file_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new DeleteFilePacket(2, "des", "/des");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_DELETEFILE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_delete_file(blade_packet, reply_packet);
    delete_file_packet = static_cast<DeleteFileReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_FILE_NOT_EXIST, delete_file_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    CreatePacket * creat_packet = new CreatePacket(2, "file99", 0, 3);
    creat_packet->pack();
    blade_packet = packet_factory_->create_packet(OP_CREATE);
    blade_packet->get_net_data()->set_read_data(creat_packet->content(), creat_packet->length());
    blade_packet->unpack(); 
    nameserver->meta_manager()->meta_blade_create(blade_packet, reply_packet);
    CreateReplyPacket *create_reply = static_cast<CreateReplyPacket *>(reply_packet);
    ASSERT_EQ(RET_SUCCESS, create_reply->get_ret_code());
    int64_t file_id = create_reply->get_file_id();
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(creat_packet)
        delete creat_packet;
    creat_packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    AddBlockPacket * add_block = new AddBlockPacket(file_id, 0);
    add_block->pack();
    blade_packet = packet_factory_->create_packet(OP_ADDBLOCK);
    blade_packet->get_net_data()->set_read_data(add_block->content(), add_block->length());
    blade_packet->unpack();
    blade_packet->endpoint_ = new AmFrame::StreamEndPoint();
    nameserver->meta_manager()->meta_blade_add_block(blade_packet, reply_packet);
    AddBlockReplyPacket * add_block_reply = static_cast<AddBlockReplyPacket *>(reply_packet);
    ASSERT_EQ(RET_SUCCESS, add_block_reply->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(add_block)
        delete add_block;
    add_block = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

    packet = new DeleteFilePacket(file_id, "", "/file99");
    packet->pack();
    blade_packet = packet_factory_->create_packet(OP_DELETEFILE);
    blade_packet->get_net_data()->set_read_data(packet->content(), packet->length());
    blade_packet->unpack();
    nameserver->meta_manager()->meta_blade_delete_file(blade_packet, reply_packet);
    delete_file_packet = static_cast<DeleteFileReplyPacket *>(reply_packet);
    EXPECT_EQ(RET_LAST_BLOCK_NO_COMPLETE, delete_file_packet->get_ret_code());
    if(reply_packet)
        delete reply_packet;
    reply_packet = NULL;
    if(packet)
        delete packet;
    packet = NULL;
    if(blade_packet)
        delete blade_packet;
    blade_packet = NULL;

}

}
}
