/*
 *version : 0.1
 *author  : chenyunyun
 *date    : 2012-3-29
 *
 */
#ifndef BLADESTORE_NAMESERVER_PROACTOR_DATAPIPE_H
#define BLADESTORE_NAMESERVER_PROACTOR_DATAPIPE_H
#include <time.h>

#include "cthread.h"
#include "thread_cond.h"
#include "blade_common_define.h"

using namespace pandora;
using namespace bladestore::common;

namespace bladestore
{
namespace nameserver
{

//Executor需要实现execute方法
template <typename Container, typename Executor>
class ProactorDataPipe : public Runnable
{
public:
	typedef typename Container::value_type value_type;
public:
	ProactorDataPipe();
	ProactorDataPipe(Executor * excutor, int thread_count, void * args);
	
	virtual ~ProactorDataPipe()
	{
		Stop();
		if (thread_ != NULL)
		{
			delete []thread_;
		}
	}

	void set_thread_parameter(Executor * excutor, int thread_count, void * args);	
	void set_thread_count(int thread_count = 1);
	
	void Stop(bool wait_for_finish = false);
	bool Push(const value_type & data, const int32_t max_queue_length = 1000, bool blocking = false);
	
	void Run(CThread * thread, void * args);
	void Start();

public:
	bool excuting_;

private:
	bool stop_;
	bool wait_for_finish_;
	int thread_count_;
	time_t wait_time_;
	void * args_;
	Container container_;
	Executor * excutor_;
	CThread * thread_;
	CThreadCond cond_;
};

template<typename Container, typename Excutor>
ProactorDataPipe<Container, Excutor>::ProactorDataPipe() : excuting_(false), stop_(false), wait_for_finish_(false), args_(NULL), excutor_(NULL), thread_(NULL)
{

}

template<typename Container, typename Excutor>
ProactorDataPipe<Container, Excutor>::ProactorDataPipe(Excutor * excutor, int thread_count, void * args):excuting_(false), wait_for_finish_(false), args_(args), excutor_(excutor), thread_(NULL)
{
	set_thread_count(thread_count);
}

template<typename Container, typename Excutor>
void ProactorDataPipe<Container, Excutor>::set_thread_parameter(Excutor * excutor, int thread_count, void * args)
{
	excutor_ = excutor;
	set_thread_count(thread_count);
	args_ = args;
	thread_ = NULL;
}

template<typename Container, typename Excutor>
void ProactorDataPipe<Container, Excutor>::set_thread_count(int thread_count)
{
	if (NULL != thread_)
	{
		LOGV(LL_ERROR, "thread has been already setted");
		return ;
	}
	thread_count_ = thread_count;
}

template<typename Container, typename Excutor>
void ProactorDataPipe<Container, Excutor>::Start()
{
	if (NULL != thread_ || thread_count_ < 1)
	{
		LOGV(LL_ERROR, "parameter invalid");
		return;
	}
	thread_ = new CThread[thread_count_];
	for (int i =0; i < thread_count_; i++)
	{
		thread_[i].Start(this, (void *)(args_));
	}
}

template<typename Container, typename Excutor>
void ProactorDataPipe<Container, Excutor>::Stop(bool wait_for_finish)
{
	cond_.Lock();
	stop_ = true;
	wait_for_finish_ = wait_for_finish;
	cond_.Broadcast();
	cond_.Unlock();
}

template<typename Container, typename Excutor>
bool ProactorDataPipe<Container, Excutor>::Push(const value_type & data, const int32_t max_queue_depth, bool blocking)
{
	if (stop_)
	{
		return false;
	}
	cond_.Lock();
	if (max_queue_depth > 0)
	{
		while(false == stop_ && static_cast<int>(container_.size()) >= max_queue_depth && blocking)
		{
			cond_.Wait();
		}
		if (stop_)
		{
			cond_.Unlock();
			return false;
		}
		if (static_cast<int>(container_.size()) > max_queue_depth && !blocking)
		{
			cond_.Unlock();
			return false;
		}
	}
	container_.push_back(data);
	cond_.Unlock();	
	cond_.Signal();
	return true;
}

template<typename Container, typename Excutor>
void ProactorDataPipe<Container, Excutor>::Run(CThread * thread, void * args)
{
	//int32_t total_count = 0;
	while(!stop_)
	{
		cond_.Lock();
		while(!stop_ && container_.size() == 0)
		{
			cond_.Wait();
		}
		if (stop_)
		{
			cond_.Unlock();
			break;
		}
		value_type data = container_.front();
		container_.pop_front();
		excuting_ = true;
		cond_.Unlock();
		cond_.Signal();

		if (excutor_)
		{
			excutor_->Execute(data, args);
		}
		
		cond_.Lock();
		excuting_ = false;
		cond_.Unlock();
	}

	if (wait_for_finish_)
	{
		cond_.Lock();
		if (container_.size() > 0)
		{
			value_type data = container_.front();
			container_.pop_front();
			cond_.Unlock();
			
			if (excutor_)
			{
				excutor_->Execute(data, args);
			}
			cond_.Lock();
		}
		cond_.Unlock();
	}
	else
	{
		//do nothing now, maybe do something later
	}
}

}//end of namespace nameserver
}//end of namespace bladestore
#endif
