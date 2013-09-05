#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <stdio.h>
#include <time.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include "time_util.h"
#include "log.h"
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "blade_file_info.h"
#include "client.h"

#ifdef PATH_MAX
static int pathmax = PATH_MAX;
#else
static int pathmax = 0;
#endif

#define SUSV3 200112L

static long posix_version = 0;

#define PATH_MAX_GUESS 1024

using namespace pandora;
using namespace bladestore::client;
using namespace bladestore::common;
using namespace bladestore::message;

static char *fullpath;
string sourcepath;
string destpath;
Client *client = NULL;

char *path_alloc(int *sizep)
{
    char *ptr;
    int size;

    if (posix_version == 0)
        posix_version = sysconf(_SC_VERSION);
    if (pathmax == 0) {
        errno = 0;
        if ((pathmax = pathconf("/", _PC_PATH_MAX)) < 0) {
            if (errno == 0) {
                pathmax = PATH_MAX_GUESS;
            } else {
                printf("pathconf error for _PC_PATH_MAX.\n");
                return NULL;
            }
        } else {
            ++pathmax;
        }
    }
    if (posix_version < SUSV3)
        size = pathmax + 1;
    else
        size = pathmax;
    if ((ptr = (char *)malloc(size)) == NULL) {
        printf("malloc error for pathname.\n");
        return NULL;
    }

    if (sizep != NULL)
        *sizep = size;
    return (ptr);
}

int32_t readfile(void *buf, int32_t buffersize)
{
    char * tmp = fullpath + sourcepath.size(); 
    string filename = destpath + tmp;
    int fid = client->Open(fullpath, O_RDONLY);
    if (fid < 0) {
        LOGV(LL_ERROR, "Open file error.");
        return -1;
    }

    int localfid = open(filename.c_str(), O_RDWR | O_APPEND | O_CREAT | O_TRUNC);
    if (localfid < 0) {
        LOGV(LL_ERROR, "Open local file error.");
        return -1;
    }
    int64_t readlen;
    do {
        readlen = client->Read(fid, buf, buffersize);
        if (readlen < 0) {
            LOGV(LL_ERROR, "Read error.");
            return -1;
        } else if (readlen > 0) {
            LOGV(LL_INFO, "Write data: %ld", readlen);
            int64_t writelocallen = write(localfid, buf, readlen);
        } else {
            LOGV(LL_INFO, "Read no data");
        }
    } while (readlen > 0);

    close(localfid);
    client->Close(fid);
    return 0;
}

int dopath(void *buf, int32_t buffersize)
{
    int             ret;
    char            *ptr;
    FileInfo finfo = client->GetFileInfo(fullpath);
    if (0 == finfo.get_file_id()) {
        LOGV(LL_ERROR, "GetFileInfo error");
        return -1;
    }
    if (1 == finfo.get_file_type()) {
        int32_t readret = readfile(buf, buffersize);
        if (0 == readret) {
            LOGV(LL_INFO, "Read file success");
            return 0;
        } else {
            LOGV(LL_ERROR, "read error");
            return -1;
        }
    }

    char * tmp = fullpath + sourcepath.size(); 
    string dirname = destpath + tmp;

    int mkdirret = mkdir(dirname.c_str(), S_IRWXU);
    if (mkdirret != 0) {
        LOGV(LL_ERROR, "Makedirectory %s error.", dirname.c_str());
        return -1;
    }

    ptr = fullpath + strlen(fullpath);
    *ptr++ = '/';
    *ptr = 0;

vector<string> filenames = client->GetListing(fullpath);
    if (filenames.empty()) {
        LOGV(LL_DEBUG, "empty dir or error occured.");
    }

    for (vector<string>::iterator it = filenames.begin(); it != filenames.end(); ++it) {
        LOGV(LL_INFO, "the file name: %s .", (*it).c_str());

        strcpy(ptr, (*it).c_str());

        if ((ret = dopath(buf, buffersize)) != 0)
            break;
    }
    ptr[-1] = 0;

    return ret;
}

int main (int argc, char **argv)
{
    if (argc != 3) {
        printf("%s sourcepath destpath (0 for write, 1 for safewrite)\n", argv[0]);
        return -1;
    }

    client = new Client();
    int initret = client->Init();
    if (BLADE_ERROR == initret)
        abort();

    int32_t     buffersize = 1048600;
    char        *buf = (char *)malloc(buffersize);

    int         len;
    fullpath = path_alloc(&len);

    sourcepath = argv[1];
    destpath = argv[2];

    strncpy(fullpath, sourcepath.c_str(), len);
    fullpath[len - 1] = 0;
    LOGV(LL_INFO, "fullpath: %s, sourcepath: %s, destpath: %s", fullpath, sourcepath.c_str(), destpath.c_str());
    dopath(buf, buffersize);

    free(buf);
    delete client;
}
