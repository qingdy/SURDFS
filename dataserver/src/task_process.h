/* Sohu R&D 2012
 *
 * FileDescription: impliment of read and write  and replicate ...
 *
 * Author   : @landyliu
 * Version  :
 * Date     : 2012-06-01
 *
 */

#ifndef BLADESTORE_TASK_PROCESS_H
#define BLADESTORE_TASK_PROCESS_H 1

#include <set>
#include <map>
#include "blade_common_define.h"
#include "blade_packet.h"
#include "cthread.h"
#include "handle.h"

namespace bladestore
{
namespace dataserver
{
using std::set;
using std::map;
using namespace pandora;
using namespace bladestore::common;
using namespace bladestore::message;


class NSTask;
class ClientTask;
class SyncResponse;
class BlockStatus;
class DataServerImpl;
class bladestore::common::Block;
class DataServerStreamSocketHandler;

typedef pandora::Handle<BlockStatus> BlockStatusPtr;
///////////////////////////////////////////////////////////////////////////////
// task processor for client queue
class ClientTaskProcessor : public Runnable
{
public:
    explicit ClientTaskProcessor(DataServerImpl *impl);
    ~ClientTaskProcessor(){}

    void Run(CThread *thread, void *arg);
    int32_t ProcessTask(ClientTask *task);

    int32_t BadBlockReport(Block &b, bool remove_flag = true);//if there is no such block, remove_flag =  false; if block crc error  remove_flag = true;

private:
    ClientTaskProcessor(); //disallow

    int32_t ProcessRead(ClientTask                 *task);
    int32_t ProcessWritePipeline(ClientTask        *task);
    int32_t ProcessWritePacket(ClientTask          *task);
    int32_t ProcessReplicateBlock(ClientTask       *task);

    int32_t ProcessWritePipelineReply(ClientTask   *task);
    int32_t ProcessWritePacketReply(ClientTask     *task);
    //assist function
    int32_t WritePipelineLastServer(BladePacket * packet, BlockStatusPtr status_para);
    int32_t WritePacketLastServer(BladePacket * packet, BlockStatusPtr  status_para);

    int32_t BadBlockReport(set<int64_t>& bad_blocks);

    DataServerImpl  * ds_impl_; 
    uint64_t ds_id_;
    
    DISALLOW_COPY_AND_ASSIGN(ClientTaskProcessor);
};


//----------------------task processor for nameserver queue----------------------//
class NSTaskProcessor : public pandora::Runnable
{
public:
    explicit NSTaskProcessor(DataServerImpl *impl);
    ~NSTaskProcessor(){}
    
    void Run(pandora::CThread *thread, void *arg);
    int32_t ProcessTask(NSTask *task);

private:
    NSTaskProcessor(); //disallow
    
    int32_t ProcessHeartbeatReply(NSTask *task);   
    int32_t ProcessBlockReportReply(NSTask *task);   
    int32_t ProcessBlockToGetLength(NSTask *task); 
    //these two need sync
    int32_t ProcessRegisterReply(NSTask *task);   
    int32_t ProcessReplicateBlockReply(NSTask * task);
    
    int32_t RemoveBlocks(BladePacket *packet);

    DataServerImpl  * ds_impl_;
    int32_t  rack_id_;
    uint64_t ds_id_;

    DISALLOW_COPY_AND_ASSIGN(NSTaskProcessor);
};

//--------------------sync processor: espacially dealing with the sync situation----------------//
class SyncTaskProcessor : public pandora::Runnable
{
public:
    explicit SyncTaskProcessor(DataServerImpl *impl);
    ~SyncTaskProcessor(){}
    
    void Run(pandora::CThread *thread, void *arg);

private:
    SyncTaskProcessor(); //disallow

    int32_t TransferBlocks(map<BlockInfo, set<uint64_t> > &blocks_to_replicate);//map: blockinfo -> destinate dataservers;
    //for replicate block
    int32_t TransferBlock(const BlockInfo &  b, uint64_t ds_id);
    int32_t TransferBlockHandleError(char * data,
                                    DataServerStreamSocketHandler * stream_handler);//mempoolfree data and close the stream_handler
    
    DataServerImpl  * ds_impl_;
    int32_t  rack_id_;
    uint64_t ds_id_;

    DISALLOW_COPY_AND_ASSIGN(SyncTaskProcessor);
};

} //end of namespace 
}

#endif
