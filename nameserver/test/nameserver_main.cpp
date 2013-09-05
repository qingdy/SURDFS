#include "nameserver.h"

using namespace bladestore::nameserver;
using namespace bladestore::common;
using namespace bladestore::message;
using namespace pandora;
using namespace bladestore::ha;
using namespace bladestore::btree;

int main ()
{
    NameServer * nameserver = new NameServer();

    nameserver->init();
    nameserver->start();

    while(true) 
	{
		sleep(10);
    }

    return 0;
}
