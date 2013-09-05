#include <stdio.h>
#include <assert.h>
#include <stdint.h>
#include <unistd.h>

#include "log.h"
#include "mempool.h"
#include "singleton.h"
#include "socket_context.h"
#include "blade_crc.h"
#include "blade_net_util.h"
#include "blade_common_data.h"
#include "bladestore_ops.h"
#include "read_block_packet.h"
#include "write_pipeline_packet.h"
#include "write_packet_packet.h"
#include "heartbeat_packet.h"
#include "register_packet.h"
#include "bad_block_report_packet.h"
#include "block_report_packet.h"
#include "block_received_packet.h"
#include "block_to_get_length_packet.h"
#include "replicate_block_packet.h"

#include "fsdataset.h"
#include "fs_interface.h"
#include "dataserver_conf.h"
#include "dataserver_impl.h"
#include "dataserver_heartbeat.h"
#include "dataserver_net_handler.h"
#include "dataserver_net_server.h"
#include "dataserver_response.h"
#include "dataserver_stat_manager.h"
#include "task_process.h"

namespace bladestore
{
namespace dataserver
{
using namespace bladestore::message;
//--------------------------- client task processor--------------------------//
ClientTaskProcessor::ClientTaskProcessor(DataServerImpl *impl)
{
    ds_impl_ = impl; 
    ds_id_ = (Singleton<DataServerConfig>::Instance()).ds_id();
}

void ClientTaskProcessor::Run(pandora::CThread *thread, void *arg) 
{
    ClientTask *task = NULL;
    while (ds_impl_->is_running()) 
    {
        LOGV(LL_INFO, "client:%ld start pop", thread->GetThreadId());
        bool ret = ds_impl_->PopClientQueue(task);
        if (NULL == task) 
        {
            LOGV(LL_DEBUG, "afer pop task = NULL");
            break;
        }
        if (false == ret) 
        {
            LOGV(LL_ERROR, "pop client queue error");
        } 
        else 
        {
            LOGV(LL_INFO, "client:%ld pop client success", thread->GetThreadId());
            int32_t ret_code = ProcessTask(task);
            LOGV(LL_DEBUG, "thread:%ld processTask ret_code:%d, operation:%d",
                 thread->GetThreadId(), ret_code, task->packet()->get_operation());
        }
        if (NULL != task) 
        {
            delete task;
            task = NULL;
        }
    }
}

// invoke related method to handle each kind of packet
int32_t ClientTaskProcessor::ProcessTask(ClientTask *task)
{
    if (NULL == task) 
    {
        LOGV(LL_DEBUG, "task = NULL");
        return BLADE_ERROR;
    }
    if (NULL == (task->packet())) 
    {
        LOGV(LL_DEBUG, "packet = NULL");
        return BLADE_ERROR;
    }

    int16_t operation = task->packet()->get_operation();
    int32_t ret = BLADE_SUCCESS;
    LOGV(LL_DEBUG, "start process client task %d", operation);
    switch (operation) 
    {
        //处理读请求
        case OP_READBLOCK:
        {
            //XXX for monitor
            AtomicInc64(&Singleton<DataServerStatManager>::
                        Instance().read_packet_times_);
            ret = ProcessRead(task);
            //XXX for monitor
            if (BLADE_SUCCESS != ret) 
            {
                AtomicInc64(&Singleton<DataServerStatManager>::
                        Instance().read_packet_error_times_);
            }
            break;
        }
        //处理建立流水线请求
        case OP_WRITEPIPELINE:
            ret = ProcessWritePipeline(task);
            break;
        //处理流水线回复
        case OP_WRITEPIPELINE_REPLY:
            ret = ProcessWritePipelineReply(task);
            break;
        //处理写请求
        case OP_WRITEPACKET:
        {
            //XXX for monitor
            AtomicInc64(&Singleton<DataServerStatManager>::Instance().write_packet_times_);
            ret = ProcessWritePacket(task);
            //XXX for monitor
            if (BLADE_SUCCESS != ret) 
            {
                AtomicInc64(&Singleton<DataServerStatManager>::Instance().write_packet_error_times_);
            }
            break;
        }
        //处理写回复请求
        case OP_WRITEPACKET_REPLY:
            ret = ProcessWritePacketReply(task);
            break;
        //处理数据块复制请求
        case OP_REPLICATEBLOCK:
        {
            //XXX for monitor
            AtomicInc64(&Singleton<DataServerStatManager>::Instance().replicate_packet_times_);
            ret = ProcessReplicateBlock(task);
            //XXX for monitor
            if (BLADE_SUCCESS != ret) 
            {
                AtomicInc64(&Singleton<DataServerStatManager>::Instance().replicate_packet_error_times_);
            }
            break;
        }
        default :
            LOGV(LL_ERROR, "operation invalid:%d", operation);
            ret = BLADE_ERROR;
            break;
    }
    return ret;
}

//process Read request
int32_t ClientTaskProcessor::ProcessRead(ClientTask *task)
{
    LOGV(LL_DEBUG, "come into read");
    ReadBlockPacket *p = static_cast<ReadBlockPacket *>(task->packet());
    if (NULL == p) 
    {
        LOGV(LL_ERROR, "read packet static_cast error");
        return BLADE_ERROR;
    }
    int32_t ret = BLADE_SUCCESS;
    uint32_t channel_id = p->channel_id();
    int64_t block_id = p->block_id();
    int32_t block_version = p->block_version();
    int64_t block_offset = p->block_offset();
    int64_t request_data_length = p->request_data_length();
    int64_t sequence = p->sequence();

    int64_t start_offset;
    int64_t end_offset;
    int64_t checksum_offset;

    Block  block_local = Block(block_id, 0, block_version);
    //get block file info such as fd ,block length
    BlockStatusPtr status = ds_impl_->fs_interface()->PrepareRead(block_local);
    int16_t ret_code = status->ret_code();
    // PrepareRead return error code, send it to client 
    if (RET_SUCCESS != ret_code) 
    {
        //向ns上报并且删除本地block。
        if (RET_BLOCK_OR_META_FILE_NOT_EXIST == ret_code) 
        {
            BadBlockReport(block_local);
        }
        LOGV(LL_ERROR, "read status retcode:%d,blk_id:%ld;blk_version:%ld,"
             "blk_offset:%ld;sequence:%ld,request_len:%ld", ret_code, block_id,
             block_version, block_offset, sequence, request_data_length);

        ReadBlockReplyPacket *reply_packet = new ReadBlockReplyPacket(ret_code,
                                    block_id, block_offset, sequence, 0, NULL); 
        if (NULL == reply_packet) 
        {
            LOGV(LL_ERROR, "new read reply packet error");
            return BLADE_ERROR;
        }

        reply_packet->set_channel_id(channel_id);
        ret = p->Reply(reply_packet);
        return ret;
    } 
    else 
    {
        //check whether the read request is valid 
        LOGV(LL_DEBUG, "read block_id:%ld;block_version:%d,block_offset:%ld;"
             "request_len:%ld,sequence:%ld,status->block_length:%ld", block_id,
             block_version, block_offset, request_data_length,
             status->block_length());

        if (0 > block_offset || block_offset > status->block_length() ||
            ((block_offset + request_data_length) > status->block_length())||
            0 >= request_data_length || 
            BLADE_MAX_PACKET_DATA_LENGTH < request_data_length) 
        {

            LOGV(LL_ERROR, "read request length invalid");
            ret_code = RET_READ_REQUEST_INVALID;    
            //TODO does here need to send packet to client     
            ReadBlockReplyPacket * reply_packet = new ReadBlockReplyPacket(
                         ret_code, block_id, block_offset, sequence, 0, NULL);
            reply_packet->set_channel_id(channel_id);

            ret = p->Reply(reply_packet);
            if (BLADE_SUCCESS != ret) 
            {
                LOGV(LL_ERROR, "send read reply packet error");
                return BLADE_ERROR;
            } 
            else 
            {
                LOGV(LL_INFO, "send read reply packet success,ret_code:%d",
                     ret_code);
                return BLADE_SUCCESS;
            }
        }

        start_offset = (block_offset - block_offset % BLADE_CHUNK_SIZE);
        end_offset = block_offset + request_data_length;
        if (0 != end_offset % BLADE_CHUNK_SIZE)
            end_offset += ( BLADE_CHUNK_SIZE - end_offset % BLADE_CHUNK_SIZE );

        if (end_offset > status->block_length())
            end_offset = status->block_length();

        checksum_offset = start_offset / BLADE_CHUNK_SIZE * BLADE_CHECKSUM_SIZE;

        int64_t len_data = end_offset -start_offset;
        LOGV(LL_DEBUG, "blk_len:%ld;blk_id:%ld;start_offset:%ld;end_offset:%ld;"
             "checksum_offset:%ld,sequece:%ld", status->block_length(), block_id,
             start_offset, end_offset, checksum_offset, sequence);

        char * reply_data = static_cast<char *>(MemPoolAlloc(len_data));
        if (NULL == reply_data ) 
        {
            LOGV(LL_ERROR, "mempool alloc failure");
            return BLADE_ERROR;
        }

        char * checksum = status->checksum_buf() + checksum_offset;
        ssize_t bytes_read = pread(status->blockfile_fd(), reply_data, len_data,
                                   start_offset);
        //XXX for monitor
        AtomicAdd64(&Singleton<DataServerStatManager>::Instance().read_bytes_, 
                (int64_t)bytes_read);

        if (len_data != bytes_read) 
        {
            LOGV(LL_ERROR, "read mistake,bytes_read:%d",bytes_read);
            MemPoolFree(reply_data);
            return BLADE_ERROR;
        }

        //check the crc checksum
        bool crc_ret = Func::crc_check(reply_data, checksum, len_data);
        if (false == crc_ret) 
        {
            LOGV(LL_ERROR, "crc block_id:%ld;block_offset:%ld,sequece:%ld",
                 block_id, block_offset, sequence);
            MemPoolFree(reply_data);
            //  report bad blocks; 
            Block b = Block(block_id, 0, block_version);
            BadBlockReport(b);
            // reply to client 
            ret_code = RET_READ_CRC_CHECK_ERROR; 
            ReadBlockReplyPacket * reply_packet = new ReadBlockReplyPacket(
                                    ret_code, block_id, block_offset, sequence,
                                    request_data_length, NULL); 
            reply_packet->set_channel_id(channel_id);

            ret_code = p->Reply(reply_packet);
            if (BLADE_SUCCESS != ret_code) 
            {
                LOGV(LL_ERROR, "send read reply packet error");
                return BLADE_ERROR;
            }
            else 
            {
                LOGV(LL_INFO, "send read reply packet success");

            }
            LOGV(LL_INFO, "crc error.blk_id:%ld,offset:%ld,data_length:%ld",
                 block_id, block_offset, request_data_length);
            return BLADE_SUCCESS;
        }
        //delete send_data final data  readblockpacket destructor will delete this 
        char * send_data = static_cast<char *>(MemPoolAlloc(
                                               request_data_length));

        memcpy(send_data, reply_data + block_offset % BLADE_CHUNK_SIZE, 
                                                  request_data_length);
        ret_code = RET_SUCCESS;
    
        ReadBlockReplyPacket * reply_packet = new ReadBlockReplyPacket(ret_code,
                                              block_id, block_offset, sequence,
                                              request_data_length, send_data); 
        reply_packet->set_channel_id(channel_id);

        ret_code = p->Reply(reply_packet);
        if (BLADE_SUCCESS != ret_code) 
        {
            LOGV(LL_ERROR, "send read reply packet error");
            ret = BLADE_ERROR;
        } 
        else 
        {
            LOGV(LL_INFO, "send read reply packet success。\
                    chennel_id:%u,FD:%d,block_id:%ld,\
                    offset:%ld,sequence:%ld,data_length:%ld,thread:%ld", 
                    channel_id, p->endpoint_.GetFd(),block_id, block_offset, 
                    sequence, request_data_length, pthread_self());
        }

        LOGV(LL_DEBUG, "read done。block_id:%ld, offset:%ld,sequence:%ld,"
             "data_length:%ld", block_id, block_offset, sequence,
             request_data_length);
        MemPoolFree(reply_data);
        return ret;
    }
}

int32_t ClientTaskProcessor::ProcessWritePipeline(ClientTask *task)
{
    LOGV(LL_DEBUG, "come into writepipleline");
    WritePipelinePacket *p = static_cast<WritePipelinePacket *>(task->packet());
    if (NULL == p) 
    {
        LOGV(LL_ERROR, "write pipeline static_cast error");
        return BLADE_ERROR;
    }

    int32_t ret = BLADE_SUCCESS;
    uint32_t channel_id = p->channel_id();
    int64_t block_id = p->block_id();
    int32_t block_version = p->block_version();
    int64_t file_id = p->file_id();
    int8_t is_safe_write = p->is_safe_write();
    int8_t target_num = p->target_num();
    vector<uint64_t> ds_ids = p->dataserver_ids();

    BlockStatusPtr status; 

    vector<uint64_t>::iterator iter_ds = ds_ids.begin();
    uint64_t local_id = ds_impl_->ds_heartbeat_manager()->ds_id();
    if (ds_ids[0] != local_id) 
    {
        LOGV(LL_ERROR,"dsid not match,mine:%ld,dest:%ld", local_id, ds_ids[0]);
        status = new BlockStatus();
        status->set_ret_code(RET_PIPELINE_DATASERVER_ID_NOT_MATCH);
        ret = WritePipelineLastServer(p, status);
        return ret;
    }

    Block block = Block(block_id, 0, block_version, file_id);
    status = ds_impl_->fs_interface()->PrepareWrite(block, is_safe_write);

    status->set_src_id(BladeNetUtil::GetPeerID(p->endpoint_.GetFd()));

    LOGV(LL_DEBUG, "block_id:%ld, block_version:%d,file_id:%d,target_num:%d;"
         "status:%d;is_safe_write:%d ", block_id, block_version, file_id,
         target_num, status->ret_code(), is_safe_write);
    if (1 == target_num) 
    {
        LOGV(LL_DEBUG,"jump into  last  dataserver");
        ret = WritePipelineLastServer(p, status);
        return ret;
    }

    if (RET_SUCCESS == status->ret_code()) 
    {
        vector<uint64_t>::iterator iter_ds = ds_ids.begin();
        ds_ids.erase(iter_ds);
        uint64_t next_ds_id = ds_ids[0];
        //connect to next dataserver and 
        DataServerStreamSocketHandler * stream_handler = ds_impl_->net_server()
                                                     ->ConnectToDS(next_ds_id); 
        if (NULL == stream_handler)
            return BLADE_ERROR;
        //save the endpoint  to next ds
        status->set_end_point(&(stream_handler->end_point()));
        
        status->set_des_id(next_ds_id);

        WritePipelinePacket * packet_to_next = new WritePipelinePacket(file_id,
                                 block_id, block_version, is_safe_write, ds_ids);
        if (NULL == packet_to_next) 
        {
            LOGV(LL_ERROR,"new write pipeline packet error");
            return BLADE_ERROR;
        }

        ret = packet_to_next->Pack();
        if (BLADE_SUCCESS != ret) 
        {
            LOGV(LL_ERROR, "write reply p pack error"); 
            delete packet_to_next;
            return BLADE_ERROR;
        }
        //construct client data
        WriteResponse * client_data = new WriteResponse(p->get_operation(), 
                                          p->endpoint_, channel_id, p->peer_id()); 
        if (NULL == client_data) 
        {
            LOGV(LL_ERROR,"new client_data err");
            delete packet_to_next;
            return BLADE_ERROR;
        }

        int8_t target_num_local = target_num - 1;

        ret = Singleton<AmFrame>::Instance().SendPacket(stream_handler->
                        end_point(),packet_to_next, true, (void *)client_data,
                        target_num_local*DS_WRITE_PIPELINE_TIME_OUT);
        if (0 != ret) 
        {
            delete client_data;
            if (NULL != packet_to_next)
                delete packet_to_next;
            LOGV(LL_ERROR, "error write pipeline to next ds ");
            return BLADE_ERROR;
        }
        else 
        {
            LOGV(LL_INFO, "sucess to next ds.blk_id:%ld;blk_version:%d;"
                 "ret_code:%d;", block_id, block_version, status->ret_code());
            return BLADE_SUCCESS;
        }
    }
    else 
    {
        LOGV(LL_ERROR, "preparewrite error return code:%d", status->ret_code());
        ret = WritePipelineLastServer(p, status);
    }
        return ret;
}

int32_t ClientTaskProcessor::ProcessWritePacket(ClientTask *task)
{
    //TODO  check request valid or not
    LOGV(LL_DEBUG, " come into writepacket");
    WritePacketPacket *p = static_cast<WritePacketPacket *>(task->packet());
    if (NULL == p) 
    {
        LOGV(LL_ERROR, "write packet static_cast error");
        return BLADE_ERROR;
    }
    int32_t  ret = BLADE_SUCCESS;

    uint32_t channel_id = p->channel_id();
    int64_t  block_id = p->block_id();
    int32_t  block_version = p->block_version();
    int64_t  block_offset = p->block_offset();
    int64_t  packet_seq = p->sequence();
    int8_t   target_num = p->target_num();
    int64_t  data_length = p->data_length();
    int64_t  checksum_length = p->checksum_length();
    char *   data = p->data(); //actual data
    char *   checksum = p->checksum(); 
    //1.1check the data_length match checksum_length or not
    
    if (0 != data_length) 
    {
        int64_t checksum_temp = ((block_offset + data_length + BLADE_CHUNK_SIZE - 1)/
                                   BLADE_CHUNK_SIZE) * BLADE_CHECKSUM_SIZE;
        int64_t checksum_length_actual = checksum_temp - (block_offset/
                                        BLADE_CHUNK_SIZE * BLADE_CHECKSUM_SIZE);
        if (checksum_length != checksum_length_actual ) 
        {
             LOGV(LL_ERROR, "checksum_length not match data_len.blk_id:%ld.seq:%d."
                  "block_offset:%ld.data_len:%ld;checksum_len:%ld.ch_id:%d,"
                  "target_num:%d.", block_id, packet_seq, block_offset, data_length,
                  checksum_length, channel_id, target_num);
             return BLADE_ERROR;
        }
    }
    if (0 > block_offset || (BLADE_BLOCK_SIZE) < block_offset ||
        (0 > data_length) || BLADE_BLOCK_SIZE < (block_offset + data_length)) 
    {
        LOGV(LL_ERROR, "blk_offset invalid.blk_id:%ld.seq:%d.block_offset:%ld"
            "data_len:%ld;checksum_len:%ld.ch_id:%d,target_num:%d", block_id,
            packet_seq, block_offset, data_length, checksum_length,
            channel_id, target_num);
        return BLADE_ERROR;
    }
    
    //1.2 获取本地对应block文件的相应信息
    Block block = Block(block_id, 0, block_version);
    BlockStatusPtr status = ds_impl_->fs_interface()->GetBlockStatus(block);
    if ( RET_SUCCESS != status->ret_code()) 
    {
         LOGV(LL_ERROR, "status ret code:%d blk_id:%ld.seq:%d.block_offset:%ld."
              "data_len:%ld;checksum_len:%ld.ch_id:%d,target_num:%d.", status->ret_code(),
              block_id, packet_seq, block_offset, data_length, checksum_length, 
              channel_id, target_num);
        return BLADE_ERROR;
    }
    //TODO  is there need to check request source??   
 //   uint64_t src_id = BladeNetUtil::GetPeerID(p->endpoint_.GetFd());
 //   LOGV(LL_DEBUG, "blk_id:%ld,status->block_id():%ld,block_offset:%ld,"
 //        "pipeline packet received from %s,this write packet from%s", 
 //        block_id, status->block_id(), block_offset, 
 //        BladeNetUtil::addr_to_string(status->src_id()).c_str(),
 //        BladeNetUtil::addr_to_string(src_id).c_str());
 //   if (src_id != status->src_id()) {
 //       LOGV(LL_ERROR, "src_id not match blk_id:%ld,status->block_id():%ld,"
 //            "block_offset:%ld,pipeline packet received from %s,this write packet from%s", 
 //            block_id, status->block_id(), block_offset, 
 //            BladeNetUtil::addr_to_string(status->src_id()).c_str(),
 //            BladeNetUtil::addr_to_string(src_id).c_str());
 //       return BLADE_ERROR;
 //   }
    int8_t is_safe_write = status->mode();

    LOGV(LL_DEBUG, "into blk_id:%ld. seq:%d.block_offset:%ld.data_len:%ld;"
         "checksum_len:%ld.ch_id:%d,target_num:%d.status_mode:%d", block_id,
         packet_seq, block_offset, data_length, checksum_length, channel_id,
         target_num, is_safe_write);

    int16_t ret_code = status->ret_code();

    //当前是流水线最后一个节点或者本地文件操作失败
    if (1 == target_num || (RET_SUCCESS != ret_code)) 
    {
        ret = WritePacketLastServer(p, status);
        // here status returned by getblockstatus()  must be validi。
        // so fs_interface will delete status  you don't need to do this。
        // if ((status)&&(RET_SUCCESS != ret_code))
        // delete status;
        return ret;
    }
    //2.将请求包发到下一个节点 如果超时则需要做相应的处理
    WritePacketPacket * packet_to_next = new WritePacketPacket(block_id,
            block_version, block_offset, packet_seq, target_num-1, data_length,
            checksum_length, data, checksum);
    if (NULL == packet_to_next) 
    {
        LOGV(LL_ERROR, "new write_packet error.blk_id_:%ld.", block_id);
        return BLADE_ERROR;
    }
    ret = packet_to_next->Init();
    if (BLADE_SUCCESS != ret) 
    {
        LOGV(LL_ERROR, "init write packet error,blk_id_:%ld.", block_id);
        return BLADE_ERROR;
    }
    //2.1 构建client_data,保存向上一节点发送的endpoint等信息
    WriteResponse * client_data = new WriteResponse(p->get_operation(),
                                      p->endpoint_, channel_id, p->peer_id()); 
    if (NULL == client_data) 
    {
        delete packet_to_next;
        LOGV(LL_ERROR, "new client_data error");
        return BLADE_ERROR;
    }

    ret = packet_to_next->Pack();
    if (BLADE_SUCCESS != ret) 
    {
        LOGV(LL_ERROR, "write reply p pack error");
        delete packet_to_next;
        delete client_data;
        return BLADE_ERROR;
    }
    //2.2将写请求包发往下一个dataserver
    if ((0 != status->des_id()) && 
        status->des_id() == BladeNetUtil::GetPeerID(status->end_point().GetFd())) 
    {
        ret = Singleton<AmFrame>::Instance().SendPacket(status->end_point(),
                                        packet_to_next, true, (void *)client_data,
                                        (target_num-1)*DS_WRITE_TIME_OUT);
        if (0 != ret)
        {
            LOGV(LL_ERROR, "send write to next error:%d", ret);
            delete client_data;
            delete packet_to_next;
            return BLADE_ERROR;
        }
        else
        {
            LOGV(LL_INFO, "success send write packet to next;blk_id:%ld;"
                "blk_version:%d;blk_seq:%ld;", block_id, block_version, packet_seq);
        }
    }
    else 
    {
        LOGV(LL_ERROR, "des_id not match error");
        delete packet_to_next;
        delete client_data;
        return BLADE_ERROR;
    }
    /*write the data into local file
     * not the last dataserver  do not need check crc
    */
    //3.将数据写进本地文件
    int32_t checksum_offset = block_offset / BLADE_CHUNK_SIZE * 
                              BLADE_CHECKSUM_SIZE;
    if (0 != data_length) 
    {    
        // not the last packet
        int32_t start_partial_chunk_size = (BLADE_CHUNK_SIZE - block_offset%
                                         BLADE_CHUNK_SIZE)%BLADE_CHUNK_SIZE;
        if (start_partial_chunk_size > data_length)
            start_partial_chunk_size = data_length;
        if (0 != start_partial_chunk_size ) 
        {
            //update checksum_
            Func::crc_update((status->checksum_buf()), checksum_offset, data, 0,
                             start_partial_chunk_size);
            LOGV(LL_DEBUG, "updata crc,blk_id:%ld,checksum_offset:%d,start:%d,"
                 "blk_offset:%ld,data_len:%ld,cksum_len:%d", block_id,
                 checksum_offset, start_partial_chunk_size, block_offset,
                 data_length, checksum_length);
            memcpy((status->checksum_buf() + checksum_offset + BLADE_CHECKSUM_SIZE),
                   (checksum + BLADE_CHECKSUM_SIZE),
                   (checksum_length-BLADE_CHECKSUM_SIZE));

        }
        else 
        {
            memcpy(status->checksum_buf() + checksum_offset, checksum,
                   checksum_length);
        }
        if (kSafeWriteMode == is_safe_write) 
        { 
            ret = pwrite(status->metafile_fd(), status->checksum_buf() + 
                         checksum_offset , checksum_length, checksum_offset);
            //XXX for monitor
            AtomicAdd64(&Singleton<DataServerStatManager>::Instance().write_bytes_, (int64_t)ret);

            if (ret != checksum_length) 
            {
                LOGV(LL_ERROR, "write meta file error. blk_id:%d", block_id);

                client_data->set_error(1);
                int32_t response_count = client_data->AddResponseCount() ;
                LOGV(LL_DEBUG, "a response_count:%d,blk_id:%ld,seq:%ld", response_count,
                    block_id, packet_seq);
                if (2 == response_count) 
                {
                    delete client_data;
                    LOGV(LL_DEBUG, "delete client dataa response_count:%d,blk_id:%ld,seq:%d",
                         response_count, block_id, packet_seq);
                }

                return BLADE_ERROR; 
            }
            LOGV(LL_DEBUG, "write metafile ok,blk_id:%ld,blk_seq:%ld,blk_offset:"
                 "%ld,checksum_offset:%d,st_part:%d,block_length:%ld,"
                 "checksum_len:%ld,", block_id, packet_seq, block_offset, 
                 checksum_offset, start_partial_chunk_size, data_length,
                 checksum_length);
        }

        ret = pwrite(status->blockfile_fd(), data, data_length, block_offset);
        //XXX for monitor
        AtomicAdd64(&Singleton<DataServerStatManager>::Instance().write_bytes_, (int64_t)ret);

        if (ret != data_length) 
        {
            LOGV(LL_ERROR, "write block file error.");

            client_data->set_error(1);
            int32_t response_count = client_data->AddResponseCount() ;
            LOGV(LL_DEBUG, "a response_count:%d,blk_id:%ld,seq:%ld", response_count,
                block_id, packet_seq);
            if (2 == response_count) 
            {
                delete client_data;
                LOGV(LL_DEBUG, "delete client dataa response_count:%d,blk_id:%ld,seq:%d",
                     response_count, block_id, packet_seq);
            }

            return BLADE_ERROR; 
        }
        //this used for safewrite; safewrite writes one packet afer one packet 
        status->set_block_length(block_offset + data_length);
        LOGV(LL_DEBUG, "write block file ok,blk_id:%ld,blk_seq:%ld,"
            "blk_len_sum:%ld", block_id, packet_seq, status->block_length());
        ret = BLADE_SUCCESS;

    } 
    else 
    {
        //the last packet 写block的最后一个packet
        if (kWriteCompleteMode != status->mode()) 
        {
            //update block file length
            status->set_block_length(block_offset + data_length);
            LOGV(LL_INFO, "to complete write,status->block_length():%d",
                 status->block_length());

            ds_impl_->fs_interface()->Complete(block, is_safe_write);
            if (BLADE_SUCCESS != ret) 
            {
                LOGV(LL_ERROR, "complete write error;blk_id:%ld.seq:%d;",
                     block_id,packet_seq);

                client_data->set_error(1);
                int32_t response_count = client_data->AddResponseCount() ;
                LOGV(LL_DEBUG, "a response_count:%d,blk_id:%ld,seq:%ld", response_count,
                block_id, packet_seq);
                if (2 == response_count) 
                {
                    delete client_data;
                }
                return BLADE_ERROR;
            }
        } else {
            LOGV(LL_INFO, "have already complete write;");
        }
    }

    //FIXME  remove  for test to test reply recieved before write success
    //sleep(3);

    //check if recieved write reply packet from next DS,if so send reply to pre DS
    int32_t response_count = client_data->AddResponseCount() ;
    LOGV(LL_DEBUG, "a response_count:%d,blk_id:%ld,seq:%ld", response_count,
         block_id, packet_seq);

    if (2 == response_count) 
    {
        if (NULL != client_data) 
        {
            delete client_data;
        }
        //如果error被置为1，这种情况只是在超时的时候发生
        if (client_data->error())
        {
            LOGV(LL_DEBUG, "client_data->error()=1,blk_id:%ld,seq:%ld",
                 block_id, packet_seq);
            return BLADE_SUCCESS; 
        }
        WritePacketReplyPacket * reply_packet = new WritePacketReplyPacket(block_id,
                                                    packet_seq, status->ret_code());
        if (NULL == reply_packet) 
        {
            LOGV(LL_ERROR, "new write reply p packet error");
            return BLADE_ERROR;
        }

        reply_packet->set_channel_id(channel_id);
        ret = reply_packet->Pack();
        if (BLADE_SUCCESS != ret) 
        {
            delete reply_packet;
            LOGV(LL_ERROR, "write reply p pack errot"); 
            return BLADE_ERROR;
        }
        //FIXME for test use
        //sleep(60);
        if ((0 != p->peer_id()) && 
            p->peer_id() == BladeNetUtil::GetPeerID(p->endpoint_.GetFd())) 
        {
            ret = Singleton<AmFrame>::Instance().SendPacket(p->endpoint_, reply_packet);
            if (0 != ret) 
            {
                LOGV(LL_ERROR, "send write reply packet error:%d", ret);
                delete reply_packet;
                return BLADE_ERROR;
            }
            else 
            {
                LOGV(LL_INFO, "To pre DS/client blokk_id:%ld;seq_num:%ld;ret:%d;",
                     block_id, packet_seq, status->ret_code());
                return BLADE_SUCCESS;
            }
        }
        else 
        {
            LOGV(LL_ERROR, "peer_id not match error:%d", ret);
            delete reply_packet;
            return BLADE_ERROR;
        }
    }
    return ret;
}

int32_t ClientTaskProcessor::ProcessWritePipelineReply(ClientTask *task)
{

    WritePipelineReplyPacket * p = static_cast<WritePipelineReplyPacket *>
                                              (task->packet());
    WriteResponse * client_data = task->write_response();
    if (NULL == p) 
    {
        LOGV(LL_ERROR, "pipeline reply static_cast error");
        if (client_data) 
        {
            delete client_data;
        }  
        return BLADE_ERROR;
    }

    if (NULL == client_data) 
    {
        LOGV(LL_ERROR, "pipeline reply write response == NULL");
        return BLADE_ERROR;
    }

    int32_t ret = BLADE_SUCCESS;
    int64_t block_id = p->block_id();

    WritePipelineReplyPacket* reply_packet = new WritePipelineReplyPacket(
                            p->block_id(), p->block_version(), p->ret_code());
    if (NULL == reply_packet) 
    {
        LOGV(LL_ERROR, "new writereplypacket mistake");
        delete client_data;
        ret = BLADE_ERROR;
    } 
    else 
    {
        reply_packet->set_channel_id(client_data->channel_id());
        ret = reply_packet->Pack();
        if (BLADE_SUCCESS != ret) 
        {
            LOGV(LL_ERROR, "write pipeline reply p pack error"); 
            delete reply_packet;
            delete client_data;
            return BLADE_ERROR;
        }

        if ((0 != client_data->peer_id()) && 
            client_data->peer_id() == BladeNetUtil::GetPeerID(client_data->end_point().GetFd())) 
        {
            ret = Singleton<AmFrame>::Instance().SendPacket((client_data->end_point()),
                                                           reply_packet);
            if (0 != ret)
            {
                LOGV(LL_ERROR, "sendpacket mistake:%d ", ret);
                delete client_data;
                delete reply_packet;
                ret = BLADE_ERROR;
            }
            else
            {
                LOGV(LL_INFO, "reply to pre ds :blk_id:%ld;ret_code:%d;", block_id,
                     p->ret_code());
                delete client_data;
                ret = BLADE_SUCCESS;
            }
        }
        else 
        {
                LOGV(LL_ERROR, "peer_id not match client data  mistake:%d ", ret);
                delete client_data;
                delete reply_packet;
                ret = BLADE_ERROR;
        }
    }
    return ret;
}

int32_t ClientTaskProcessor::ProcessWritePacketReply(ClientTask *task)
{
    WritePacketReplyPacket * p = static_cast<WritePacketReplyPacket *>
                                            (task->packet());
    WriteResponse * client_data = task->write_response();
    // when p == NULL ,you have to delete client_data
    if (NULL == p) 
    {
        LOGV(LL_ERROR, " write reply static_cast error");
        if (NULL != client_data) 
        {
            delete client_data;
        }  
        return BLADE_ERROR;
    }

    int32_t ret = BLADE_SUCCESS;
    int64_t block_id = p->block_id();
    int64_t packet_seq = p->sequence();
    int16_t ret_code = p->ret_code();
    LOGV(LL_DEBUG, "into write reply blk_id:%ld, p_seq:%ld, ret_code:%d,",
         block_id, packet_seq, ret_code);

    if (NULL == client_data) 
    {
        LOGV(LL_ERROR, "write reply write response == NULL");
        return BLADE_ERROR;
    }

    LOGV(LL_INFO, "next dataserver return ret_code:%d", ret_code);
    
    //check if local write is done,if so send reply to pre DS
    int32_t response_count = client_data->AddResponseCount() ;
    LOGV(LL_DEBUG, " A response_count:%d,blk_id:%ld,seq:%ld", response_count,
         block_id, packet_seq);

    if (2 == response_count) 
    {
        //TODO for test use
        //sleep(10);
        //如果error被置为1，这种情况只是在超时的时候发生
        if (client_data->error())
        {
            LOGV(LL_DEBUG, "client_data->error()=1,blk_id:%ld,seq:%ld",
                 block_id, packet_seq);
            if (NULL != client_data)
                delete client_data;
            return BLADE_SUCCESS; 
        }

        WritePacketReplyPacket * reply_packet = new WritePacketReplyPacket(block_id,
                                                    packet_seq, ret_code);
        if (NULL == reply_packet)
        {
            LOGV(LL_ERROR, "new write reply p packet error");
            if (NULL != client_data)
                delete client_data;
            return BLADE_ERROR;
        }

        reply_packet->set_channel_id(client_data->channel_id());
        ret = reply_packet->Pack();
        if (BLADE_SUCCESS != ret) 
        {
            delete reply_packet;
            if (NULL != client_data)
                delete client_data;
            LOGV(LL_ERROR, "write reply p pack errot"); 
            return BLADE_ERROR;
        }

        if ((0 != client_data->peer_id()) && 
            client_data->peer_id() == BladeNetUtil::GetPeerID(client_data->end_point().GetFd())) 
        {

            ret = Singleton<AmFrame>::Instance().SendPacket(client_data->end_point(), reply_packet);
            if (NULL != client_data)
                delete client_data;
            if (0 != ret) 
            {
                LOGV(LL_ERROR, "send write reply packet error%d", ret);
                delete reply_packet;
                return BLADE_ERROR;
            }
            else 
            {
                LOGV(LL_INFO, "To pre DS/client block_id:%ld;seq_num:%ld;ret:%d;",
                     block_id, packet_seq, ret);
                return BLADE_SUCCESS;
            }
        }
        else 
        {
            if (NULL != client_data) 
            {
                delete client_data;
            }
            LOGV(LL_ERROR, "peer_id not match  error%d", ret);
                delete reply_packet;
                return BLADE_ERROR;
        }
    }
    return ret;
}

int32_t ClientTaskProcessor::ProcessReplicateBlock(ClientTask *task)
{
    LOGV(LL_DEBUG, "replicate block");
    ReplicateBlockPacket *p = static_cast<ReplicateBlockPacket *>
                              (task->packet());
    if (NULL == p) 
    {
        LOGV(LL_ERROR, "replicate packet static_cast err");
        return BLADE_ERROR;
    }
    int32_t  ret = BLADE_SUCCESS;

    uint32_t channel_id = p->channel_id();
    int64_t  block_id = p->block_id();
    int64_t  block_length = p->block_length();
    int32_t  block_version = p->block_version();
    int64_t  file_id = p->file_id();
    int64_t  block_offset = p->block_offset();
    int64_t  packet_seq = p->sequence();
    int64_t  data_length = p->data_length();
    int64_t  checksum_length = p->checksum_length();
    char *   data = p->data();
    char *   checksum = p->checksum(); 
    
    //1.0check length match or not 
    if (0 != data_length) 
    {
        int64_t checksum_temp = ((block_offset + data_length + BLADE_CHUNK_SIZE - 1)/
                                   BLADE_CHUNK_SIZE) * BLADE_CHECKSUM_SIZE;
        int64_t checksum_length_actual = checksum_temp - (block_offset/
                                        BLADE_CHUNK_SIZE * BLADE_CHECKSUM_SIZE);
        LOGV(LL_DEBUG, "replicate block .blk_id:%ldseq:%ld.blk_offset:%ld."
             "data_len:%ld;checksum_len:%ld.channel_id:%d", block_id, packet_seq,
             block_offset, data_length, checksum_length, channel_id);
        if (checksum_length != checksum_length_actual ) 
        {
             LOGV(LL_ERROR, "checksum_length not match data_len.blk_id:%ldseq:%ld."
                  "blk_offset:%ld.data_len:%ld;checksum_len:%ld.ch_id:%d", block_id,
               packet_seq, block_offset, data_length, checksum_length, channel_id);
             return BLADE_ERROR;
        }
    }
    //2.0 prepare for replicate:create file and open file 
    Block block = Block(block_id, block_length, block_version, file_id);
    BlockStatusPtr status; 
    status = ds_impl_->fs_interface()->PrepareReplicate(block);
    if (0 == status) 
    {
        LOGV(LL_ERROR, "get replicate status error");
        return BLADE_ERROR;
    }
    // if status->ret_code() != RET_SUCCESS ,you have to delete status yourself
    if (RET_SUCCESS != status->ret_code()) 
    {
        LOGV(LL_ERROR, "replicate prepare error retcode:%d",
             status->ret_code());
        //delete status;
        return BLADE_ERROR;
    }

    //如果所接收的复制block为长度为0。
    if (0 == block_length) 
    {
        ret = ds_impl_->fs_interface()->Complete(block, kReplicateMode);
        if (BLADE_SUCCESS != ret) 
        {
            LOGV(LL_ERROR, "complete replicate error;blk_id:%ld.seq:%d;",
                 block_id, packet_seq);
            return BLADE_ERROR;
        } 
        else 
        {
            LOGV(LL_INFO, "complete replicate ok;blk_id:%ld.seq:%ld;",
                 block_id,packet_seq);
        }
        BlockInfo block_info(block_id, block_version);
        bool ret_flag = ds_impl_->ds_heartbeat_manager()->DSReceivedBlockReport(block_info);
        if (true == ret_flag) 
        {
            LOGV(LL_INFO, "blockreceive report ok;blk_id:%ld,blk_version:%d",
                 block_id, block_version);
        }
        else 
        {
            LOGV(LL_ERROR, "blockreceive report error;blk_id:%ld,blk_versi:%d",
                 block_id, block_version);
            ret = BLADE_ERROR;
        }
        return ret;
    }

    //2.1 write data to local file
    int32_t checksum_offset = block_offset / BLADE_CHUNK_SIZE * BLADE_CHECKSUM_SIZE;
    LOGV(LL_DEBUG, "before write to local,block_offset:%ld,data_len:%ld,"
         "checksum_offset:%d, checksum_len:%ld,", block_offset, data_length,
         checksum_offset, checksum_length);
    memcpy((status->checksum_buf() + checksum_offset), checksum, checksum_length);
    ssize_t bytes_write = pwrite(status->blockfile_fd(), data, data_length,
                                 block_offset);
    //XXX for monitor
    AtomicAdd64(&Singleton<DataServerStatManager>::Instance().write_bytes_, (int64_t)bytes_write);

    if (bytes_write != data_length) 
    {
        LOGV(LL_ERROR, "write block file error.");
        return BLADE_ERROR; 
    }
    status->AddBlockLength(data_length);
    LOGV(LL_DEBUG, "write block file ok,blk_id:%ld,blk_seq:%ld,blk_len_sum:%d",
         block_id, packet_seq, status->block_length());
    //2.2 send reply packet
    ReplicateBlockReplyPacket * reply_packet = new ReplicateBlockReplyPacket(
                                status->ret_code(), block_id, packet_seq);
    if (NULL == reply_packet) 
    {
        LOGV(LL_ERROR, "new replicate reply p packet error");
        return BLADE_ERROR;
    }

    reply_packet->set_channel_id(channel_id);
    ret = reply_packet->Pack();
    if (BLADE_SUCCESS != ret) 
    {
        delete reply_packet;
        LOGV(LL_ERROR, "replicate reply packet pack error"); 
        return BLADE_ERROR;
    }

    if ((0 != p->peer_id()) && 
        p->peer_id() == BladeNetUtil::GetPeerID(p->endpoint_.GetFd())) 
    {

        ret = Singleton<AmFrame>::Instance().SendPacket(p->endpoint_,
                                                        reply_packet);
        if (0 != ret) 
        {
            LOGV(LL_ERROR, "send replicate reply packe error%d", ret);
            delete reply_packet;
            return BLADE_ERROR;
        } 
        else 
        {
            LOGV(LL_INFO, "ok to DS block_id:%ld;seq:%d;ret:%d;", block_id, 
                 packet_seq, status->ret_code());
        }
    }
    else 
    {
        LOGV(LL_ERROR, "peer_id not match error%d", ret);
        delete reply_packet;
        return BLADE_ERROR;

    }
    if (status->block_length() == block_length) 
    {
        ret = ds_impl_->fs_interface()->Complete(block, kReplicateMode);
        if (BLADE_SUCCESS != ret) 
        {
            LOGV(LL_ERROR, "complete replicate error;blk_id:%ld.seq:%d;",
                 block_id, packet_seq);
            return BLADE_ERROR;
        }
        else 
        {
            LOGV(LL_INFO, "complete replicate ok;blk_id:%ld.seq:%ld;",
                 block_id,packet_seq);
        }
        BlockInfo block_info(block_id, block_version);
        bool ret_flag = ds_impl_->ds_heartbeat_manager()->DSReceivedBlockReport(block_info);
        if (true == ret_flag) 
        {
            LOGV(LL_INFO, "blockreceive report ok;blk_id:%ld,blk_version:%d",
                 block_id, block_version);
        }
        else 
        {
            LOGV(LL_ERROR, "blockreceive report error;blk_id:%ld,blk_versi:%d",
                 block_id, block_version);
        }
    }
    return BLADE_SUCCESS;
}
//this function doesn't need to delete status
int32_t ClientTaskProcessor::WritePipelineLastServer(BladePacket * packet, 
                                                     BlockStatusPtr status_para)
{
    WritePipelinePacket *p = static_cast<WritePipelinePacket *>(packet); 
    BlockStatusPtr status = status_para;
    int32_t ret = BLADE_SUCCESS;
    uint32_t channel_id = p->channel_id();
    int64_t block_id = p->block_id();
    int32_t block_version = p->block_version();

    LOGV(LL_DEBUG, "last DS write pipeline;blk_id:%ld", block_id); 
    WritePipelineReplyPacket * reply_packet = new WritePipelineReplyPacket(
                                block_id, block_version, status->ret_code());
    if (NULL == reply_packet) 
    {
        LOGV(LL_ERROR, "last ds new reply_packet == NULL");
        return BLADE_ERROR;
    }
    reply_packet->set_channel_id(channel_id);
    ret = reply_packet->Pack();
    if (BLADE_SUCCESS != ret) 
    {
        delete reply_packet;
        LOGV(LL_ERROR, "pipeline reply packet  pack error");
        return BLADE_ERROR;
    }
    //TODO for test
    //sleep(20);
    if ((0 != p->peer_id()) && 
        p->peer_id() == BladeNetUtil::GetPeerID(p->endpoint_.GetFd())) 
    {

        ret = Singleton<AmFrame>::Instance().SendPacket(p->endpoint_, reply_packet);
        if (0 != ret) 
        {
            LOGV(LL_ERROR, "last DS did not send pipeline p To pre DS");
            delete reply_packet;
            ret = BLADE_ERROR;
        } 
        else 
        {
            LOGV(LL_INFO, "Last DS To pre DS reply blk_id:%ld;blk_v:%d;ret_code:%d;",
                 block_id,block_version, status->ret_code());
            ret = BLADE_SUCCESS;
        }
    }
    else 
    {
        LOGV(LL_ERROR, "peer_id not match");
        delete reply_packet;
        ret = BLADE_ERROR;

    }
    return ret;
}

int32_t ClientTaskProcessor::WritePacketLastServer(BladePacket * packet,
                                                   BlockStatusPtr status_para)
{
    WritePacketPacket *p = static_cast<WritePacketPacket *>(packet); 
    BlockStatusPtr status = status_para;
    if (0 == p || 0 == status) 
    {
        LOGV(LL_ERROR, "packet or status == NULL");
        return BLADE_ERROR;
    }

    int32_t ret = BLADE_SUCCESS;
    uint32_t channel_id = p->channel_id();
    int64_t block_id = p->block_id();
    int32_t block_version = p->block_version();
    int64_t block_offset = p->block_offset();
    int64_t packet_seq = p->sequence();
    int64_t data_length = p->data_length();
    int64_t checksum_length = p->checksum_length();
    char * data = p->data(); //actual data
    char * checksum = p->checksum(); 

    int8_t is_safe_write = status->mode();

    LOGV(LL_DEBUG, "Last DS write packet.blk_id:%ld;seq:%ld;blk_offset_:%ld;"
       "data_len:%ld,checksum_length:%ld,status_mode:%d", block_id, packet_seq,
         block_offset, data_length, checksum_length, is_safe_write);

    Block block = Block(block_id, 0, block_version);
    int32_t ret_code = status->ret_code();
    if (RET_SUCCESS == ret_code) 
    {
        // crc checksum check and write the file into local block files
         //then send reply_packet to pre dataserver or client 
        // not the last packet
        if ( 0 != data_length) 
        {     
            // partial chunk size 
            int32_t start_partial_chunk_size = (BLADE_CHUNK_SIZE - block_offset%
                                          BLADE_CHUNK_SIZE)%BLADE_CHUNK_SIZE;
            //data_length is very small 
            if (start_partial_chunk_size > data_length)
                start_partial_chunk_size = data_length;
            // crc_check
            bool ret_crc = Func::crc_check(data, checksum, data_length, 
                                           start_partial_chunk_size);    
            LOGV(LL_DEBUG, "check info:data_len:%ld,start%d,block_offset:%d",
                 data_length, start_partial_chunk_size, block_offset);
            if (false == ret_crc) 
            {
                LOGV(LL_ERROR, "crc check error.start_part:%d,blk_id:%ld,"
                     "block_offset:%ld", start_partial_chunk_size, block_id,
                     block_offset);
                return BLADE_ERROR;;
            } 
            else 
            {
                int32_t checksum_offset = block_offset/BLADE_CHUNK_SIZE*
                                          BLADE_CHECKSUM_SIZE;
                LOGV(LL_DEBUG, "crc check ok.start_part:%d, check_offset:%d"
                     "blk_id:%ld,block_offset:%ld", start_partial_chunk_size,
                     checksum_offset, block_id, block_offset);
                //save checksum  
                if (0 != start_partial_chunk_size) 
                {
                    //use partial chunk to update checksum 
                    Func::crc_update(status->checksum_buf(), checksum_offset, data,
                                     0, start_partial_chunk_size);
                    memcpy((status->checksum_buf() + checksum_offset + 
                           BLADE_CHECKSUM_SIZE), (checksum + BLADE_CHECKSUM_SIZE),
                           (checksum_length -  BLADE_CHECKSUM_SIZE));
                } 
                else 
                {
                    memcpy((status->checksum_buf() + checksum_offset), checksum, 
                           checksum_length);
                }
                //write block data to local file
                if (kSafeWriteMode == is_safe_write) 
                {
                    ret = pwrite(status->metafile_fd(), status->checksum_buf() + 
                         checksum_offset , checksum_length, checksum_offset);
                    //XXX for monitor
                    AtomicAdd64(&Singleton<DataServerStatManager>::Instance().write_bytes_, (int64_t)ret);
                
                    if (ret != checksum_length) 
                    {
                        LOGV(LL_ERROR, "write meta file error.");
                        return BLADE_ERROR; 
                    }
                    LOGV(LL_DEBUG, "write metafile ok,blk_id:%ld,blk_seq:%d,"
                            "block_offset:%d,checksum_offset:%d,st_part:%d,"
                            "block_length:%d,checksum_len:%d,", block_id, packet_seq,
                            block_offset, checksum_offset, start_partial_chunk_size,
                            data_length, checksum_length);
                }   
                ret = pwrite(status->blockfile_fd(), data, data_length,
                             block_offset);
                //XXX for monitor
                AtomicAdd64(&Singleton<DataServerStatManager>::Instance().write_bytes_, (int64_t)ret);

                if (ret != data_length) 
                {
                    LOGV(LL_ERROR, "write_packet data err.blk_id_:%ld;p_seq:%d;"
                         "blk_offset:%d;data_length:%d.", block_id, packet_seq,
                         block_offset, data_length);
                    return BLADE_ERROR; 
                }
                
                status->set_block_length(block_offset + data_length);
                LOGV(LL_DEBUG, "write block file ok,blk_id:%ld,blk_seq:%d,"
                    "blk_len:%d", block_id, packet_seq, status->block_length());
                ret = BLADE_SUCCESS;
            }
        } 
        else 
        {
            if (kWriteCompleteMode != status->mode()) 
            {
                status->set_block_length(block_offset + data_length);
                //block.set_num_bytes(block_offset + data_length);
                LOGV(LL_INFO, "to complete write,status->block_length():%d",
                    status->block_length());

                ret = ds_impl_->fs_interface()->Complete(block, is_safe_write);
                if (BLADE_SUCCESS != ret) 
                {
                    LOGV(LL_ERROR, "complete write error;blk_id:%ld.seq:%d;",
                         block_id, packet_seq);
                    return BLADE_ERROR;
                }
                LOGV(LL_INFO, "complete write blk_id:%ld,seq:%d,blk_offset:%ld",
                     block_id, block_offset, packet_seq);
            } 
            else 
            {
                LOGV(LL_INFO, "has alread complete write blk_id:%ld,seq:%d,"
                     "blk_offset:%ld", block_id, block_offset, packet_seq);
            }
        }   
        //FIXME  send reply_packet to pre 
        //sleep(10);
        WritePacketReplyPacket * reply_packet = new WritePacketReplyPacket(
                                              block_id, packet_seq, ret_code);
        if (NULL == reply_packet) 
        {
            LOGV(LL_ERROR, "new write reply_p err;blk_id:%ld.packet_seq:%d.",
                 block_id, packet_seq);
            return BLADE_ERROR;
        }
        reply_packet->set_channel_id(channel_id);
        ret = reply_packet->Pack();
        if (BLADE_SUCCESS != ret) 
        {
            delete reply_packet;
            LOGV(LL_ERROR, "write reply p pack errot"); 
            return BLADE_ERROR;
        }

        if ((0 != p->peer_id()) && 
            p->peer_id() == BladeNetUtil::GetPeerID(p->endpoint_.GetFd())) 
        {

            ret = Singleton<AmFrame>::Instance().SendPacket(p->endpoint_,
                                                             reply_packet);
            if (0 != ret) 
            {
                LOGV(LL_ERROR, "send write reply to pre DS");
                delete reply_packet;
                ret = BLADE_ERROR;
            }
            else 
            {
                ret = BLADE_SUCCESS;
                LOGV(LL_INFO, "ok send write reply to Pre DS.blk_id%ld;seq:%d;"
                     "ret_code:%d;.\n", block_id, packet_seq, ret_code);
            }
        }
        else 
        {
            LOGV(LL_ERROR, "peer_id not match!!");
            delete reply_packet;
            ret = BLADE_ERROR;
        }
    }
    return ret;
}


int32_t ClientTaskProcessor::BadBlockReport(set<int64_t>& bad_blocks)
{
    LOGV(LL_DEBUG, "come into  badblockreport"); 
    int32_t ret = BLADE_SUCCESS;
    BadBlockReportPacket * p = new BadBlockReportPacket(ds_id_, bad_blocks);
    if (NULL == p) 
    {
        LOGV(LL_ERROR, "new badblockreport packet error"); 
        return BLADE_ERROR;
    }
    ret = p->Pack();
    if (BLADE_SUCCESS != ret) 
    {
        delete p;
        LOGV(LL_ERROR, "write reply p pack error");
        return BLADE_ERROR;
    }

    uint64_t ns_id = (Singleton<DataServerConfig>::Instance()).ns_id();
    int64_t  socket_fd = ds_impl_->net_server()->stream_handler()->end_point().GetFd();
    uint64_t ns_id_use = BladeNetUtil::GetPeerID(socket_fd);
    if (ns_id_use != ns_id) 
    {
        LOGV(LL_ERROR, "streamhandler with NS  changed, report error.");
        delete p;
        return BLADE_ERROR;
    }
    ret = Singleton<AmFrame>::Instance().SendPacket(ds_impl_->
                              net_server()->stream_handler()->end_point(), p); 
    if (0 != ret) 
    {
        LOGV(LL_ERROR, "BadBlockReport Send packet err:%d", ret);
        delete p;
        return BLADE_ERROR;
    }
    else 
    {
        LOGV(LL_INFO, "BadBlockReport Send packet success");
        return BLADE_SUCCESS;
    }
}

int32_t ClientTaskProcessor::BadBlockReport(Block &b, bool remove_flag)
{
    LOGV(LL_DEBUG, "dataserver report bad block,blk_id:%ld", b.block_id()); 
    int32_t ret = BLADE_SUCCESS;
    //local remove files and map items
    if (remove_flag)
        ds_impl_->dataset()->RemoveBlock(b.block_id());
    set<int64_t> bad_blocks_id;
    bad_blocks_id.insert(b.block_id());
    ret = BadBlockReport(bad_blocks_id);
    return ret;
}

//------------------------nameserver task processor------------------------//

NSTaskProcessor::NSTaskProcessor(DataServerImpl * impl)
{
    ds_impl_ = impl;
    ds_id_ = (Singleton<DataServerConfig>::Instance()).ds_id();
    rack_id_ = (Singleton<DataServerConfig>::Instance()).rack_id();
    LOGV(LL_DEBUG, "ds_id: %ld, rack_id:%d", ds_id_,rack_id_);
}

void NSTaskProcessor::Run(pandora::CThread * thread, void * arg) 
{
    NSTask * task;
    while (ds_impl_->is_running()) 
    {
        LOGV(LL_INFO, "start ns  pop");
        bool ret = ds_impl_->PopNameServerQueue(task);
        if (!task) 
        {
            LOGV(LL_ERROR, "after pop task = NULL");
            break;
        }
        if (false == ret) 
        {
            LOGV(LL_ERROR, "pop ns queue error");
        } 
        else 
        {
            LOGV(LL_INFO, "pop ns queue success");
            int32_t ret_code = ProcessTask(task);
            LOGV(LL_DEBUG, "processTask:%d, operation:%d", ret_code,
                 task->packet()->operation());
        }
        if (NULL != task) 
        {
            delete task;
            task = NULL;
        }
    }
}

int32_t NSTaskProcessor::ProcessTask(NSTask * task)
{
    if (NULL == task) 
    {
        LOGV(LL_ERROR, "task is null ");
        return BLADE_ERROR;
    }
    int16_t operation = task->packet()->get_operation();
    int32_t ret = BLADE_SUCCESS;
    LOGV(LL_DEBUG, "st process NS task %d", operation);
    switch (operation) 
    {
        case OP_HEARTBEAT_REPLY:
            ret = ProcessHeartbeatReply(task);
            break;
        case OP_BLOCK_REPORT_REPLY:
            ret = ProcessBlockReportReply(task);
            break;
        case OP_BLOCK_TO_GET_LENGTH:
            ret = ProcessBlockToGetLength(task);
            break;
        case OP_REGISTER_REPLY:
            ret = ProcessRegisterReply(task);
            break;
        case OP_REPLICATEBLOCK_REPLY:
            ret = ProcessReplicateBlockReply(task);
            break;
        default:
            LOGV(LL_ERROR, "operation invalid");
            break;
    }

    return ret;
}

int32_t NSTaskProcessor::ProcessHeartbeatReply(NSTask *task)
{ 
    int32_t ret = BLADE_SUCCESS;
    LOGV(LL_DEBUG, " In heartbeat_reply");
    HeartbeatReplyPacket * reply =  static_cast<HeartbeatReplyPacket *>(task->packet());
    if (NULL == reply) 
    {
        LOGV(LL_ERROR, "Heartbeat reply static_cast error");
        return BLADE_ERROR;
    }
    switch (reply->ret_code()) 
    {
        case RET_NEED_REGISTER:
            LOGV(LL_INFO, "Heartbeat reply to register");
            while (!(ds_impl_->ds_heartbeat_manager()->DSRegister())) 
            {
                LOGV(LL_ERROR, "ds_register return error");
            }
            break;
        case RET_NEED_OP:
            LOGV(LL_INFO, "Heartbeat reply to need op");
            if (!reply->blocks_to_remove().empty()) 
            {
                //XXX for test
                //sleep(10);
                LOGV(LL_INFO, "Heartbeat reply to need remove %d blocks", 
                     reply->blocks_to_remove().size());
                RemoveBlocks(reply);
            }
            if (!reply->blocks_to_replicate().empty()) 
            {
                //TODO block replicate  this need to be  moved out from heart beat
                LOGV(LL_INFO, "Heartbeat reply to need replicate %d blocks", 
                     reply->blocks_to_replicate().size());
                bool flag = ds_impl_->PushSyncQueue(reply->blocks_to_replicate());
                if (flag)
                    LOGV(LL_INFO, "into sync queue success");
                else
                    LOGV(LL_ERROR, "into sync queue error");
                LOGV(LL_DEBUG, "sync_queue_size: %d", ds_impl_->SyncQueueSize());

                LOGV(LL_DEBUG, "done HB reply need transferblock");
            }
            break;
        default:
            LOGV(LL_ERROR, "heartbeat with unexpected error");
            ret = BLADE_ERROR;
    }
    return ret;
}

int32_t NSTaskProcessor::ProcessBlockReportReply(NSTask *task)
{
    LOGV(LL_DEBUG, " In block report_reply");
    //FIXME  for test
    //BlockToGetLengthPacket *p = new BlockToGetLengthPacket(3,2);
    //NSTask *temp_task = new NSTask(p,NULL);
    //ds_impl_->PushNameServerQueue(temp_task);
    BlockReportReplyPacket * reply = static_cast<BlockReportReplyPacket *>(task->packet());
    if (NULL == reply) 
    {
        LOGV(LL_ERROR, "BlockReport static_cast error");
        return BLADE_ERROR;
    }
    int32_t ret = BLADE_SUCCESS;
    switch (reply->ret_code()) 
    {
        case RET_SUCCESS: 
        {
            LOGV(LL_INFO, "BlockReport with RET_SUCCESS");
            struct tm *time_now;
            time_t lt;
            lt = time(NULL);
            time_now = localtime(&lt);
            srand((unsigned)time(NULL));
            int64_t time_next = ((ds_impl_->config()->block_report_time_hour()
                        - time_now->tm_hour + 24) % 24) * 3600000000 +
                        ((ds_impl_->config()->block_report_time_minite()
                        - time_now->tm_min + 60) % 60) * 60000000 +
                        //加上一个随机数避免所有DS在同一时间进行block report
                        rand() % 120 * 60000000;
            if (0 == time_next)
                time_next = LAST_REPORT_INTERVAL;
            ds_impl_->ds_heartbeat_manager()->set_last_report(TimeUtil::GetTime() + time_next);
            LOGV(LL_DEBUG, "last report time updated");
            break;
        }
        case RET_NEED_REGISTER:
            LOGV(LL_INFO, "BlockReport with RET_NEED_REGISTER");
            ret = BLADE_ERROR;
            break;
        default:
            LOGV(LL_ERROR, "BlockReport with unexpected error");
            ret = BLADE_ERROR;
    }
    //XXX for test
    //map<BlockInfo, set<uint64_t> > replicate;
    //BlockInfo block = BlockInfo(5,1);
    //uint64_t tmp_dsid = 38798474349066;
    //set<uint64_t> tmp_ds;
    //tmp_ds.insert(tmp_dsid);
    //replicate.insert(make_pair(block, tmp_ds));
    //ds_impl_->PushSyncQueue(replicate);
    return ret;
}

int32_t NSTaskProcessor::ProcessBlockToGetLength(NSTask *task)   
{
    LOGV(LL_DEBUG, " In block to get length");
    BlockToGetLengthPacket *p = static_cast<BlockToGetLengthPacket *>(task->packet());
    if (NULL == p) 
    {
        LOGV(LL_ERROR, "get_length packet static_cast error");
        return BLADE_ERROR;
    }

    int64_t block_id = p->block_id();
    int32_t block_version = p->block_version();
    int64_t block_length = 0;
    int16_t ret_code = RET_SUCCESS; 

    LOGV(LL_DEBUG, "get_length,blk_id:%ld,blk_version:%d,", block_id,
         block_version);
    Block  b=Block(block_id, 0, block_version);
    block_length = ds_impl_->dataset()->GetBlockLength(b);
    
    if (0 > block_length)
        ret_code = RET_GET_BLOCK_LENGTH_BLOCK_NOT_EXIST;

    LOGV(LL_INFO, "ok get_length,ret_code:%d,ds_id:%d, blk_id:%ld,bversion:%d,"
         "blk_length:%d", ret_code, ds_id_, block_id, block_version,
         block_length);
    BlockToGetLengthReplyPacket * reply_packet = new
            BlockToGetLengthReplyPacket(ret_code, ds_id_, block_id,
                                        block_version, block_length); 
    if (NULL == reply_packet) 
    {
        LOGV(LL_ERROR, "new get length reply packet error");
        return BLADE_ERROR;
    }
    int ret = p->Reply(reply_packet);
    if (BLADE_SUCCESS == ret)
        LOGV(LL_INFO, "ok get_length,blk_id:%ld,bversion:%d,blk_length:%d",
             block_id, block_version, block_length);
    else
        LOGV(LL_ERROR, "error get_length,bid:%ld,bversion:%d,blk_length:%d",
             block_id, block_version, block_length);
    return ret;
}

int32_t NSTaskProcessor::ProcessRegisterReply(NSTask * task)
{
    int32_t ret = BLADE_SUCCESS;
    LOGV(LL_DEBUG, " In register_reply");

    SyncResponse * client_data = task->sync_response();
    if (NULL == client_data) 
    {
        LOGV(LL_ERROR, "client data is NULL");
        return BLADE_ERROR;
    }
    RegisterReplyPacket * reply = static_cast<RegisterReplyPacket *>(task->packet());
    if (NULL == reply) 
    {
        LOGV(LL_ERROR, "RegisterReplyPacket static_cast error");
        client_data->cond().Lock();
        client_data->set_wait(false);
        client_data->cond().Unlock();
        client_data->cond().Signal();
        return BLADE_ERROR;
    } 
    switch (reply->ret_code()) 
    {
        case RET_SUCCESS: 
        {
            LOGV(LL_INFO, "register with RET_SUCCESS");
            client_data->cond().Lock();
            client_data->set_wait(false);
            client_data->set_response_ok(true);
            client_data->cond().Unlock();
            client_data->cond().Signal();
            LOGV(LL_DEBUG, "RegisterReplyPacket signal success");

            set<BlockInfo *> blockset_info;
            ds_impl_->dataset()->GetBlockReport(blockset_info);

            if (!(ds_impl_->ds_heartbeat_manager()->DSBlockReport(blockset_info))) 
            {
                LOGV(LL_ERROR, "ds block report error");
                ret = BLADE_ERROR;
            }
            break;
        }
        case RET_REGISTER_INVALID:
            LOGV(LL_ERROR, "register with RET_REGISTER_INVALID");
            ret = BLADE_ERROR;
        default:
            client_data->cond().Lock();

            client_data->set_wait(false);
            client_data->cond().Unlock();
            client_data->cond().Signal();
            LOGV(LL_DEBUG, "RegisterReplyPacket signal error");
            ret = BLADE_ERROR;
    }
    return ret;
}

int32_t NSTaskProcessor::ProcessReplicateBlockReply(NSTask * task)
{
    int32_t ret = BLADE_SUCCESS;
    LOGV(LL_DEBUG, " In replicateblock_reply");

    SyncResponse * client_data = task->sync_response();
    //this check  may not be necessary
    if (NULL == client_data) 
    {
        LOGV(LL_ERROR, "client data is NULL");
        return BLADE_ERROR;
    }
    ReplicateBlockReplyPacket * reply = static_cast<ReplicateBlockReplyPacket *>
                                                   (task->packet());
    if (NULL == reply) 
    {
        LOGV(LL_ERROR, "ReplicateblockReplyPacket static_cast error");
        client_data->cond().Lock();
        int32_t response_count = client_data->AddResponseCount();
        int32_t base_count = client_data->base_count();
        if (response_count == base_count) 
        {
            client_data->set_wait(false);
            client_data->cond().Unlock();
            client_data->cond().Signal();
            LOGV(LL_DEBUG, "ReplicateblockReplyPacket signal success");
        } 
        else 
        {
            client_data->cond().Unlock();
        }
        return BLADE_ERROR;
    } 
    LOGV(LL_DEBUG, "Replicateblockreply blk_id:%ld,packet_seq:%d",
         reply->block_id(), reply->sequence());

    client_data->cond().Lock();
    int32_t response_count = client_data->AddResponseCount();
    int32_t base_count = client_data->base_count();
    if (response_count == base_count) 
    {
        client_data->set_wait(false);
        client_data->cond().Unlock();
        client_data->cond().Signal();
        LOGV(LL_DEBUG, "ReplicateblockReplyPacket signal success");
    } 
    else 
    {
        client_data->cond().Unlock();
    }
    LOGV(LL_INFO, "Replicateblockreply proccessed");
    return ret;
}

int32_t NSTaskProcessor::RemoveBlocks(BladePacket *packet)
{
    HeartbeatReplyPacket * reply =  static_cast<HeartbeatReplyPacket *>(packet) ;
    if (NULL == reply) 
    {
        LOGV(LL_ERROR, "Heartbeat reply static_cast error");
        return BLADE_ERROR;
    }
    LOGV(LL_DEBUG, "Heartbeat reply to remove blocks");
    ds_impl_->dataset()->RemoveBlocks(reply->blocks_to_remove());

    return BLADE_SUCCESS;    
}

//SyncTaskProcessor
SyncTaskProcessor::SyncTaskProcessor(DataServerImpl * impl)
{
    ds_impl_ = impl;
    ds_id_ = (Singleton<DataServerConfig>::Instance()).ds_id();
    rack_id_ = (Singleton<DataServerConfig>::Instance()).rack_id();
    LOGV(LL_DEBUG, "ds_id: %ld, rack_id:%d", ds_id_,rack_id_);
}

void SyncTaskProcessor::Run(pandora::CThread * thread, void * arg) 
{
    map<BlockInfo, set<uint64_t> > task;
    task.clear();
    LOGV(LL_INFO, "after first clear ,map.size:%d", task.size());
    while (ds_impl_->is_running()) 
    {
        LOGV(LL_INFO, "Start Sync pop");
        bool ret = ds_impl_->PopSyncQueue(task);
        if (false == ret) 
        {
            LOGV(LL_ERROR, "pop sync queue error");
        } 
        else 
        {
            LOGV(LL_INFO, "pop sync queue success,map.size:%d", task.size());
            int32_t ret_code = TransferBlocks(task);
            LOGV(LL_INFO, "after transfer ret_code%d", ret_code);
        }
        task.clear();
        LOGV(LL_INFO, "after clear ,map.size:%d", task.size());
    }
}

int32_t SyncTaskProcessor::TransferBlockHandleError(char * data,
                                    DataServerStreamSocketHandler * stream_handler)
{
    int32_t ret = BLADE_SUCCESS;
    if (NULL != data) 
    {
        MemPoolFree(data);
        data = NULL;
    }
    if (NULL != stream_handler) 
    {
        int32_t  ret_code = Singleton<AmFrame>::Instance().CloseEndPoint(
                                                stream_handler->end_point());
        if (0 == ret_code)
            LOGV(LL_INFO, "close end point ok");
        else 
        {
            LOGV(LL_ERROR, "close end point error");
            ret = BLADE_ERROR;
        }
    }
    return ret;
}

int32_t SyncTaskProcessor::TransferBlock(const BlockInfo & b, uint64_t ds_id)
{
    LOGV(LL_DEBUG, "come into transferblock,blk_id:%ld", b.block_id());
    //1.check this request is valid:block exist here && block length is small than local
    int64_t file_id = ds_impl_->fs_interface()->BlockIdToFileId(b.block_id());
    if (-1 == file_id) 
    {
        LOGV(LL_DEBUG, "transferblock,blk_id:%ld, getfileid failed", b.block_id());
        Block block_local(b.block_id(), 0, b.block_version()); 
        ds_impl_->client_processor()->BadBlockReport(block_local, false);
        return BLADE_ERROR;
    }

    Block block(b.block_id(), 0, b.block_version(), file_id); 
    BlockStatusPtr status = ds_impl_->fs_interface()->PrepareRead(block);

    int16_t ret_code = status->ret_code();
    LOGV(LL_DEBUG, "prepareread ,ret_code:%d", ret_code);
    switch (ret_code)
    {
        case RET_SUCCESS :
        {
            LOGV(LL_INFO, "prepareread success,ret_code:%d", ret_code);
            break;
        }
        case RET_BLOCK_OR_META_FILE_NOT_EXIST :
        {
            LOGV(LL_ERROR, "prepareread error,ret_code:%d", ret_code);
            ds_impl_->client_processor()->BadBlockReport(block);
            return BLADE_ERROR;
        }
        case RET_BLOCK_NOT_EXIST_IN_DS :
        {
            ds_impl_->client_processor()->BadBlockReport(block, false);
        }
        default :
        {
            LOGV(LL_ERROR, "prepareread error,ret_code:%d", ret_code);
            return BLADE_ERROR;
        }
    }

    LOGV(LL_DEBUG, "status->block_length:%d block_id:%d", status->block_length(),
         b.block_id());
    block.set_num_bytes(status->block_length());

    int64_t block_id = block.block_id();
    int64_t block_length = block.num_bytes();
    int32_t block_version = block.block_version();

    char * data = static_cast<char *>(MemPoolAlloc(
                                      BLADE_MAX_PACKET_DATA_LENGTH + 1));
    if (NULL == data) 
    {
        LOGV(LL_ERROR, "malloc memory error", ret_code);
        return BLADE_ERROR;
    }

    //2.connect to target ds
    DataServerStreamSocketHandler * stream_handler = ds_impl_->net_server()
                                                    ->ConnectToDS(ds_id);
    if (NULL == stream_handler) 
    {
        LOGV(LL_ERROR, "connect to ds error,ds:%ld", ds_id);
        TransferBlockHandleError(data, NULL);
        return BLADE_ERROR;
    }
    LOGV(LL_DEBUG, "connect to ds success,ds:%ld", ds_id);
    
    uint64_t peer_id = ds_id; 
 
    //如果所复制的文件长度为0，直接发个空包。不需要回复
    if (0 == block_length) 
    {
        ReplicateBlockPacket * packet = new ReplicateBlockPacket( block_id,
                block_length, block_version, file_id, 0, 1, 0, 0, NULL, NULL);
        if (NULL == packet) 
        {
            LOGV(LL_ERROR, "new replicate p, blk_id:%ld; packet_seq:%d",
                block_id, 1);
                TransferBlockHandleError(data, stream_handler);
                return BLADE_ERROR;
        }   
        int32_t ret = packet->Pack();
        if (BLADE_SUCCESS != ret) 
        {
            LOGV(LL_ERROR, "write reply p pack error"); 
            delete packet;
            TransferBlockHandleError(data, stream_handler);
            return BLADE_ERROR;
        }
        //send the data out
        LOGV(LL_DEBUG, "before send packet to ds blk_id:%ld,blk_length:%d.",
            block_id, block_length);
        if ((0 != peer_id) && peer_id == BladeNetUtil::GetPeerID(
                                            stream_handler->end_point().GetFd())) 
        {

            ret = Singleton<AmFrame>::Instance().SendPacket(
                            stream_handler->end_point(), packet);
            if (0 != ret) 
            {
                LOGV(LL_ERROR, "send packet to ds error ");
                delete packet;
                TransferBlockHandleError(data, stream_handler);
                return BLADE_ERROR;
            }
            else 
            {
                LOGV(LL_INFO, "sucess to  ds.blk_id:%ld;", block_id);
            }
        }
        else 
        {
            LOGV(LL_ERROR, "peer_id not match  error ");
            delete packet;
            TransferBlockHandleError(data, stream_handler);
            return BLADE_ERROR;
        }
        TransferBlockHandleError(data, stream_handler);
        return BLADE_SUCCESS;
    }

    //TODO close endpoint while error
    //所复制的block长度不为0
    int32_t packet_seq = 1;
    ssize_t bytes_total_send = 0; 
    int64_t bytes_sum = block_length; 
    int64_t bytes_left = bytes_sum;
    int32_t i;

    SyncResponse * client_data;
    while (bytes_total_send < bytes_sum) 
    {
        //3.1 construct client_data
        int32_t base_count = 0;
        if (bytes_left > BLADE_MAX_PACKET_DATA_LENGTH*DS_REPLICATE_SYNC_UNIT) 
        {
            base_count = DS_REPLICATE_SYNC_UNIT;
        } 
        else 
        {
            base_count = (bytes_left + BLADE_MAX_PACKET_DATA_LENGTH - 1)/
                                            BLADE_MAX_PACKET_DATA_LENGTH;
        }
        client_data = new SyncResponse(OP_REPLICATEBLOCK, base_count); 
        if (NULL == client_data) 
        {
            LOGV(LL_ERROR, "new client_data error");
            TransferBlockHandleError(data, stream_handler);
            return BLADE_ERROR;
        }
        LOGV(LL_DEBUG, "init, blk_id:%ld, packet_seq:%d,bytes_total_send:%d,"
             "bytes_sum:%d,bytes_left:%d,base_count:%d", block_id, packet_seq,
             bytes_total_send, bytes_sum, bytes_left, base_count);

        for (i = 0; i < base_count; i++) 
        {
            //3.2 read data out from local
            int64_t len_data = (bytes_left >= BLADE_MAX_PACKET_DATA_LENGTH) ?
                               BLADE_MAX_PACKET_DATA_LENGTH : bytes_left;
            ssize_t bytes_read = pread(status->blockfile_fd(), data, len_data,
                                       bytes_total_send);
            //XXX for monitor
            AtomicAdd64(&Singleton<DataServerStatManager>::Instance().read_bytes_, (int64_t)bytes_read);

            if (len_data != bytes_read) 
            {
                LOGV(LL_ERROR, "read mistake");
                client_data->set_error(1);
                client_data->set_base_count(i);
                break;
            }
            //3.3 checksum the data
            bool crc_ret = Func::crc_check(data, status->checksum_buf() +
                           bytes_total_send/BLADE_CHUNK_SIZE*BLADE_CHECKSUM_SIZE ,
                           len_data );
            if (false == crc_ret) 
            {
                LOGV(LL_ERROR, "crc blk_id:%ld;packet_seq:%d", block_id, packet_seq);
                client_data->set_error(1);
                client_data->set_base_count(i);
                ds_impl_->client_processor()->BadBlockReport(block);
                break;
            }

            // 3.5 construct packet
            // because here offset is n*BLADE_CHUNK_SIZE, so there is no necessary to 
            // consider on checksum_length like writepacket
            int64_t checksum_length = (len_data + BLADE_CHUNK_SIZE - 1)/BLADE_CHUNK_SIZE*
                                      BLADE_CHECKSUM_SIZE;
            char * checksum = status->checksum_buf() +
                         bytes_total_send/BLADE_CHUNK_SIZE*BLADE_CHECKSUM_SIZE;
            ReplicateBlockPacket * packet = new ReplicateBlockPacket( block_id,
                    block_length, block_version, file_id, bytes_total_send,
                    packet_seq, len_data, checksum_length, data, checksum);
            if (NULL == packet) 
            {
                LOGV(LL_ERROR, "new replicate p, blk_id:%ld; packet_seq:%d",
                     block_id, packet_seq);
                client_data->set_error(1);
                client_data->set_base_count(i);
                break;
            }   
            int32_t ret = packet->Pack();
            if (BLADE_SUCCESS != ret) 
            {
                LOGV(LL_ERROR, "write reply p pack error"); 
                delete packet;
                client_data->set_error(1);
                client_data->set_base_count(i);
                break;
            }

            // 3.6 send the data out
            LOGV(LL_DEBUG, "before send packet to ds blk_id:%ld,blk_length:%d,"
                "packet_seq:%d,blk_offset:%d,data_len:%d,checksum_len:%d",
                block_id, block_length, packet_seq, bytes_total_send, len_data,
                checksum_length);


            if ((0!= peer_id) && peer_id == BladeNetUtil::GetPeerID(
                                            stream_handler->end_point().GetFd())) 
            {
                ret = Singleton<AmFrame>::Instance().SendPacket(
                                        stream_handler->end_point(), packet, true,
                                        (void *)client_data, DS_REPLICATE_TIME_OUT);
                if (0 != ret) 
                {
                    LOGV(LL_ERROR, "send packet to ds error ");
                    delete packet;
                    client_data->set_error(1);
                    client_data->set_base_count(i);
                    break;
                }
                else 
                {
                    LOGV(LL_INFO, "sucess to  ds.blk_id:%ld;packet_seq%d;",
                         block_id, packet_seq);
                }
            }
            else 
            {
                LOGV(LL_ERROR, "send packet to ds error ");
                delete packet;
                client_data->set_error(1);
                client_data->set_base_count(i);
                break;
            }

            packet_seq += 1;
            bytes_total_send += len_data;
            bytes_left -= len_data;
            LOGV(LL_DEBUG, "AfterSendPacket to ds blk_id:%ld,next_packet_seq:%d"
                 ",bytes_total_send:%d,bytes_left:%d", block_id, packet_seq,
                 bytes_total_send, bytes_left);
            if (0 >= bytes_left)
                break;
        }//end "for loop"

        //wait for sync
        if (client_data->base_count() != client_data->response_count()) 
        {
            client_data->cond().Lock();
            int wait_times = 1;//if client_data static_cast error  this variable prevent this functiong from stucking
            while (true == client_data->wait()) 
            {
                LOGV(LL_DEBUG, "before wait packet_seq:%d;", packet_seq-1);
                //here need limit wait time
                client_data->cond().Wait(1000);
                if (10 < (++wait_times))
                {
                    LOGV(LL_DEBUG, "wait_times:%d;", wait_times);
                    break;
                }
                LOGV(LL_DEBUG, "after wait packet_seq:%d;", packet_seq-1);
            }
            client_data->cond().Unlock();
            LOGV(LL_DEBUG, "after wait:response_count:%d, base_count:%d,"
                 "next packet_seq:%d", client_data->response_count(),
                 client_data->base_count(), packet_seq);
        }
        if (client_data->error()) 
        {
            LOGV(LL_DEBUG, "error to  ds.packet_seq:%d;", packet_seq - 1);
            TransferBlockHandleError(data, stream_handler);
            delete client_data;
            client_data = NULL;
            return BLADE_ERROR;
        } 
        else 
        {
            LOGV(LL_DEBUG, "sucess to  ds.packet_seq:%d;", packet_seq - 1);
            delete client_data;
            client_data = NULL;
        }
    }//end while
    TransferBlockHandleError(data, stream_handler);
    LOGV(LL_DEBUG, "transferblock:%ld end", block_id);
    return BLADE_SUCCESS;
}

int32_t SyncTaskProcessor::TransferBlocks(map<BlockInfo,
                                        set<uint64_t> > & blocks_to_replicate)
{
    LOGV(LL_DEBUG, "into replicate blocks");
    map<BlockInfo, set<uint64_t> >::iterator iter_map = 
                                             blocks_to_replicate.begin();
    map<BlockInfo, set<uint64_t> >::iterator iter_map_end = 
                                             blocks_to_replicate.end();
    set<uint64_t>::iterator iter_set;
    set<uint64_t>::iterator iter_set_end;
    int32_t ret = BLADE_SUCCESS;
    int32_t i =blocks_to_replicate.size();
    for (;iter_map != iter_map_end; iter_map++) 
    {
        const BlockInfo & block_info = iter_map->first;
        set<uint64_t> & set_temp = iter_map->second;
        iter_set = set_temp.begin();
        iter_set_end = set_temp.end();
        for (;iter_set != iter_set_end; iter_set++) 
        {
            LOGV(LL_DEBUG, "i:%d replicate blk:%ld to ds:%s", i,
                 block_info.block_id(), 
                 (BladeNetUtil::AddrToString(*iter_set)).c_str());
            //XXX for monitor
            AtomicInc64(&Singleton<DataServerStatManager>::Instance().transfer_block_times_);

            if ( BLADE_ERROR == TransferBlock(block_info, *iter_set)) 
            {
               ret = BLADE_ERROR;
                //XXX for monitor
                AtomicInc64(&Singleton<DataServerStatManager>::Instance().transfer_block_error_times_);
            }
            LOGV(LL_DEBUG, "i:%d done replicate blk:%ld to ds:%s,ret:%d", i,
                 block_info.block_id(), 
                 (BladeNetUtil::AddrToString(*iter_set)).c_str(), ret);
        }
        
    }
    return ret;
}

}//end of namespace dataserver
}//end of namespace bladestore
