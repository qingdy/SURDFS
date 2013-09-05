/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-8
 *
 */

#include <fcntl.h>
#include <sys/resource.h>

#include "log.h"
#include "blade_common_define.h"

using namespace pandora;

namespace bladestore
{
namespace common
{

int blade_daemon()
{
    int  fd;

    switch (fork()) 
	{
    case -1:
        LOGV(LL_ERROR, "fork() failed");
        return BLADE_ERROR;

    case 0:
        break;

    default:
        exit(0);
    }

    if (setsid() == -1) 
	{
        LOGV(LL_ERROR, "setsid() failed");
        return BLADE_ERROR;
    }

    umask(0);
	
    fd = open("/dev/null", O_RDWR);
    if (fd == -1) 
	{
        LOGV(LL_ERROR, "open(\"/dev/null\") failed");
        return BLADE_ERROR;
    }

    if (dup2(fd, STDIN_FILENO) == -1) 
	{
        LOGV(LL_ERROR, "dup2(STDIN) failed");
        return BLADE_ERROR;
    }

    if (dup2(fd, STDOUT_FILENO) == -1) 
	{
        LOGV(LL_ERROR, "dup2(STDOUT) failed");
        return BLADE_ERROR;
    }

#if 0
    if (dup2(fd, STDERR_FILENO) == -1) 
	{
        LOGV(LL_ERROR, "dup2(STDERR) failed");
        return BLADE_ERROR;
    }
#endif

    if (fd > STDERR_FILENO) 
	{
        if (close(fd) == -1) 
		{
            LOGV(LL_ERROR, "close() failed");
            return BLADE_ERROR;
        }
    }

    return BLADE_SUCCESS;
}

}//end of namespace common
}//end of namespace bladestore
