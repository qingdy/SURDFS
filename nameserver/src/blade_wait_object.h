/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-4-28
 *
 */

#ifndef BLADESTORE_NAMESERVER_WAIT_OBJECT_H
#define BLADESTORE_NAMESERVER_WAIT_OBJECT_H

#include <ext/hash_map>
#include <set>
#include <vector>

#include "blade_packet.h"
#include "thread_cond.h"
#include "thread_mutex.h"
#include "log.h"

using namespace std;
using namespace pandora;
using namespace bladestore::message;

namespace bladestore
{
namespace nameserver
{

class WaitObject 
{
public:
	friend class WaitObjectManager;

public:
	WaitObject()
	{
		Init();
	}

	~WaitObject()
	{
	}

	bool wait_done(int count, int timeout = 2000)
	{
		cond.Lock();
		while (done_count<count) 
		{
			if (cond.Wait(timeout) == false) 
			{
				cond.Unlock();
				return false;
			}
		}
		cond.Unlock();
		return true;
	}

	bool insert_packet(BladePacket* packet)
	{
		if (NULL != packet)
		{
			CThreadGuard guard(&mutex_);

			if (NULL != resp_list) 
			{
				resp_list->push_back(packet);
			}
			else if (NULL != resp) 
			{
				resp_list = new vector<BladePacket * >();
				resp_list->push_back(resp);
				resp = NULL;
				resp_list->push_back(packet);
			}
			else 
			{
				resp = packet;
			}
		}
		Done();
		return true;
	}

	BladePacket *get_packet(int index=0)
	{
		CThreadGuard guard(&mutex_);
		if (NULL != resp_list) 
		{
			if((size_t)index < resp_list->size())
			{
				return resp_list->at(index);
			}
			return NULL;
		}
		else 
		{
			return resp;
		}
	}

	int get_packet_count()
	{
		CThreadGuard guard(&mutex_);
		if (NULL != resp_list) 
		{
			return resp_list->size();
		}
		if (NULL != resp) 
		{
			return 1;
		}
		return 0;
	}

	int  get_id() 
	{
		return id;
	}

	void set_no_free()
	{
		resp = NULL;
		resp_list = NULL;
	}

private:
	int id;

	void Init()
	{
		done_count = 0;
		id = 0;
		resp = NULL;
		resp_list = NULL;
	}

	void Done() 
	{
		cond.Lock();
		done_count ++;
		cond.Signal();
		cond.Unlock();
	}

	CThreadMutex mutex_;

	BladePacket *resp;
	vector<BladePacket *> *resp_list;
	CThreadCond cond;
	int done_count;
};

class WaitObjectManager 
{
public:
	WaitObjectManager()
	{
		wait_object_seq_id_ = 1;
	}

	~WaitObjectManager()
	{
		CThreadGuard guard(&mutex_);
		__gnu_cxx::hash_map<int, WaitObject*>::iterator it;
		for (it=wait_object_map.begin(); it!=wait_object_map.end(); ++it) 
		{
			delete it->second;
		}
	}

	WaitObject* create_wait_object()
	{
		WaitObject *cwo = new WaitObject();
		add_new_wait_object(cwo);
		return cwo;
	}


	void destroy_wait_object(WaitObject *cwo)
	{
		CThreadGuard guard(&mutex_);
		wait_object_map.erase(cwo->id);
		delete cwo;
	}

	void wakeup_wait_object(int id, BladePacket *packet)
	{
		CThreadGuard guard(&mutex_);
		__gnu_cxx::hash_map<int, WaitObject*>::iterator it;
		it = wait_object_map.find(id);
		if (it != wait_object_map.end()) 
		{
			if (NULL == packet) 
			{
				LOGV(LL_ERROR, "[%d] packet is null.", id);
			}

			if (!it->second->insert_packet(packet))
			{
				LOGV(LL_DEBUG, "[pCode=%d] packet had been ignored", packet->get_operation());
					delete packet;
			}
		}
		else 
		{
			if (packet != NULL) 
			{
				delete packet;
			}
			LOGV(LL_ERROR, "[%d] waitobj not found.", id);
		}
	}

private:
	void add_new_wait_object(WaitObject *cwo)
	{
		CThreadGuard guard(&mutex_);
		wait_object_seq_id_ ++;
		if (wait_object_seq_id_ == 0) 
			wait_object_seq_id_ ++;
		cwo->id = wait_object_seq_id_;
		wait_object_map[cwo->id] = cwo;
	}

	__gnu_cxx::hash_map<int, WaitObject*> wait_object_map;
	int wait_object_seq_id_;
	CThreadMutex mutex_;
};

}//end of namespace nameserver
}//end of namespace bladestore

#endif
