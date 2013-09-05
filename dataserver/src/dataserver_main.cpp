#include "stdio.h"
#include "unistd.h"
#include <cassert>
#include "stdbool.h"
#include "log.h"
#include "dataserver.h"

using namespace bladestore::dataserver;

int main(int argc, char **argv)
{
#define STOP_FILE "stop.flag"
    //check the stop flag file exist
    int res = access(STOP_FILE, F_OK);
    if (0 == res) 
        remove(STOP_FILE);

    DataServer*  dataserver = new  DataServer();
    assert(dataserver);
    bool ret = dataserver->Init();
    assert(ret);
    ret = dataserver->Start();
    assert(ret);

    LOGV(LL_INFO, "dataserver start!!\n");

    //wait for stop cmd
    while (1) 
    {
        res = access(STOP_FILE, F_OK);
        if (0 == res) 
        {
            break;
        } 
        else 
            sleep(5);
    }
    ret = dataserver->Stop();
    if (false == ret) 
    {
        fprintf(stderr, "stop dataserver failed");
    }
    //wait the thread to join
    sleep(10);

    return 0;
}
