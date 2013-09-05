/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_NAMESERVER_NAMESERVER_H
#define BLADESTORE_NAMESERVER_NAMESERVER_H

#include "nameserver_impl.h"
#include "blade_common_define.h"

using namespace bladestore::common;

namespace bladestore
{
namespace nameserver
{

class NameServer
{
public:
	NameServer();
	~NameServer();
    
	int Init();
	int Start();
	int Wait();
	int Stop();

private:
	DISALLOW_COPY_AND_ASSIGN(NameServer);
	NameServerImpl * nameserver_impl_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
