#include "blade_packet_queue_thread.h"
#include "log.h"

using namespace pandora;
namespace bladestore
{
namespace nameserver
{

BladePacketQueueThread::BladePacketQueueThread()
{
	stop_ = false;
	waiting_ = false;
	thread_ = NULL;
}

BladePacketQueueThread::~BladePacketQueueThread()
{
	if (NULL != thread_)
	{
		delete []thread_;
	}
}

void BladePacketQueueThread::set_thread_parameter(const int thread_count, IPacketQueueHandler * handler, void * args, int max_queue_length)
{
	set_thread_count(thread_count);
	handler_ = handler;
	args_ = args;
	queue_.Init(max_queue_length);
}

void BladePacketQueueThread::set_thread_count(const int thread_count)
{
	if (NULL != thread_)
	{
		LOGV(MSG_ERROR, "thread has already been setted");
		return ;
	}
	thread_count_ = thread_count;
}

void BladePacketQueueThread::Start()
{
	if (NULL != thread_ || thread_count_ < 1)
	{
		LOGV(MSG_ERROR, "%s", "parameter invalid");
		return ;
	}
	thread_ = new CThread[thread_count_];
	for (int i = 0; i < thread_count_; i++)
	{
		thread_[i].Start(this, (void *)(args_));	
	}
}

void BladePacketQueueThread::Wait()
{
	if (NULL != thread_)
	{
		for (int i = 0; i < thread_count_; i++)
		{
			thread_[i].Join();
		}
	}
}

void BladePacketQueueThread::Stop(bool wait_finish)
{
	wait_finish_ = wait_finish;
	stop_ = true;
}

bool BladePacketQueueThread::Push(BladePacket * packet, bool block)
{
	if (false != stop_ || NULL == thread_)
	{
		return true;
	}

	if (false != block)
	{
		queue_.WaitTillPush(packet);
	}
	else
	{
		queue_.WaitTimePush(packet, 1000);
	}
	return true;
}


void BladePacketQueueThread::Run(CThread * thread, void * args)
{
	(void)(thread);
	(void)(args);
	BladePacket * packet = NULL;
	bool ret = false;

	while (false == stop_)
	{
		ret = queue_.WaitTillPop(packet);
		if (false != ret && NULL != handler_)
		{
			handler_->handle_packet_queue(packet, args);
		}
	}
	
	while (false == queue_.IsEmpty())
	{
		//这里不能用WaitTillPop
		ret = queue_.WaitTimePop(packet, 1000);
		if (false != ret && NULL != handler_ && false != wait_finish_)
		{
			handler_->handle_packet_queue(packet, args);
		}
	}
}

void BladePacketQueueThread::Clear()
{
	stop_ = false;
	delete [] thread_;
	thread_ = NULL;
}

}//end of namespace nameserver
}//end of namespace bladestore
