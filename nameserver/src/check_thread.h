/*
 *version : 1.0
 *author  : chen yunyun, funing
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_NAMESERVER_CHECK_THREAD_H
#define BLADESTORE_NAMESERVER_CHECK_THREAD_H

#include "cthread.h"
#include "block_replicate.h"
#include "block_redundant.h"
#include "block_scanner_manager.h"
#include "block_received_packet.h"

#ifdef GTEST
#define private public
#define protected public
#endif

using namespace pandora;
using namespace bladestore::message;

namespace bladestore
{
namespace nameserver
{

class NameServerImpl;
class MetaManager;

//check the status of DS and block
class CheckThread : public Runnable
{
public:
	CheckThread(NameServerImpl * nameserver_impl, MetaManager & meta_manager);
	~CheckThread();

	//实现Runnable接口,内部逻辑主要是check_ds, check_block等线程
	void Run(CThread * thread, void * args);
	int do_check_ds();
	int do_check_block();	

	int block_received(BlockReceivedPacket * block_received_packet);

private:
	int check_ds(int64_t );	
	bool check_task_interval(time_t now_time, time_t& last_check_time, time_t interval);

private:
	MetaManager & meta_manager_;

	//管理block副本不足时的复制
	ReplicateLauncher replicate_launcher_;

	//管理block副本过多时的删除
	BlockRedundantLauncher redundant_launcher_;	

	//模块化统一管理类
	BlockScannerManager scanner_manager_;	

	NameServerImpl * nameserver_impl_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif 
