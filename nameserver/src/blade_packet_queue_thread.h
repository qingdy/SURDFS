/*
 *version : 1.0
 *author  : chenyunyun
 *date    : 2012-3-26
 */
#ifndef BLADESTORE_NAMESERVER_PACKETQUEUE_THREAD_H
#define BLADESTORE_NAMESERVER_PACKETQUEUE_THREAD_H

#include <time.h>
#include "cthread.h"
#include "task_queue.h"
#include "thread_cond.h"
#include "blade_packet.h"
#include "blade_common_define.h"

using namespace bladestore::message;
using namespace bladestore::common;

namespace bladestore
{
namespace nameserver
{

class IPacketQueueHandler
{
public:
	IPacketQueueHandler()
	{
	
	}

	virtual ~IPacketQueueHandler()
	{

	}

	virtual bool handle_packet_queue(BladePacket * packet, void * args)=0;
};

class BladePacketQueueThread : public Runnable
{
public:
	BladePacketQueueThread();
	virtual ~BladePacketQueueThread();
	void set_thread_parameter(const int thread_count, IPacketQueueHandler *handler, void * args, int max_queue_length = 10000);	
	void set_thread_count(const int count = 1);
	void Start();
	void Stop(bool wait_finish = false);
	void Wait();
	void Clear();

	bool Push(BladePacket * packet, bool block = false);
	void Run(CThread * thread, void * args);

    TaskQueue<BladePacket *> & get_queue()
    {
        return queue_;
    }

private:
	TaskQueue<BladePacket *> queue_;
	IPacketQueueHandler * handler_;
	CThread * thread_;	
	void * args_;
	int thread_count_;
	bool stop_;
	bool waiting_;
	bool wait_finish_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
