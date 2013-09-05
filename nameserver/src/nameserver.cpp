#include "nameserver.h"

namespace bladestore
{
namespace nameserver
{

NameServer::NameServer()
{
	nameserver_impl_ = NULL;
}

NameServer::~NameServer()
{
	if (NULL != nameserver_impl_)
	{
		delete nameserver_impl_;
	}
	nameserver_impl_ = NULL;
}

int NameServer::Init()
{
	int ret = BLADE_SUCCESS;
	nameserver_impl_ = new NameServerImpl();
	if (NULL == nameserver_impl_)
		ret = BLADE_ERROR;
	if (BLADE_SUCCESS == ret)
	{
		ret = nameserver_impl_->Initialize();
	}
	return ret;
}

int NameServer::Start()
{
	int ret = BLADE_SUCCESS;
	ret = nameserver_impl_->Start();
	return ret;
}

int NameServer::Wait()
{
	int ret = BLADE_SUCCESS;
	ret = nameserver_impl_->Wait();
	return ret;
}

int NameServer::Stop()
{
	int ret = BLADE_SUCCESS;
	ret = nameserver_impl_->Stop();
	return ret;
}

}//end of namespace nameserver
}//end of namespace bladestore
