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

int32_t writefiles(void *buf, int8_t issafewrite, int32_t buffersize)
{
    int64_t localfid = open(fullpath, O_RDONLY);

    // Create a file
    char * tmp = fullpath + sourcepath.size(); 
    string filename = destpath + tmp;
    int64_t fid = client->Create(filename, issafewrite, 3);
    LOGV(LL_INFO, "File created, filepath: %s, ID: %ld.", filename.c_str(), fid);
    if (fid < 0) {
        LOGV(LL_ERROR, "Create file error.");
        return -1;
    }
    
    int64_t alreadyread = 0;
    int64_t readlocallen = 0;
    do {
        readlocallen = read(localfid, buf, buffersize);
        if (readlocallen < 0) {
            LOGV(LL_ERROR, "read local file error.");
            return -1;
        } else if (readlocallen == 0) {
            break;
        }

        int64_t writelen = client->Write(fid, buf, readlocallen);
        if (writelen < 0) {
            LOGV(LL_ERROR, "error in write");
            return -1;
        }
        alreadyread += writelen;
    } while (readlocallen > 0);
    
    client->Close(fid);
    close(localfid);
    return 0;
}

int dopath(void *buf, int8_t issafewrite, int32_t buffersize)
{
    struct stat     statbuf;
    struct dirent   *dirp;
    DIR             *dp;
    int             ret;
    char            *ptr;

    if (lstat(fullpath, &statbuf) < 0)
        return -1;
    if (S_ISDIR(statbuf.st_mode) == 0) {
        int writeret = writefiles(buf, issafewrite, buffersize);
        return writeret;
    }

    char * tmp = fullpath + sourcepath.size(); 
    string dirname = destpath + tmp;

    int mkdirret = client->MakeDirectory(dirname);
    if (mkdirret != BLADE_SUCCESS && mkdirret != -RET_DIR_EXIST) {
        LOGV(LL_ERROR, "Makedirectory %s error.", dirname.c_str());
        return -1;
    }

    ptr = fullpath + strlen(fullpath);
    *ptr++ = '/';
    *ptr = 0;

    if ((dp = opendir(fullpath)) == NULL)
        return -1;

    while ((dirp = readdir(dp)) != NULL) {
        LOGV(LL_INFO, "the file name: %s .", dirp->d_name);
        if (strcmp(dirp->d_name, ".") == 0 || strcmp(dirp->d_name, "..") == 0)
            continue;

        strcpy(ptr, dirp->d_name);
        if ((ret = dopath(buf, issafewrite, buffersize)) != 0)
            break;
    }
    ptr[-1] = 0;

    if (closedir(dp) < 0)
        return -1;

    return ret;
}

int main (int argc, char **argv)
{
    if (argc != 4) {
        printf("%s issafewrite sourcepath destpath (0 for write, 1 for safewrite)\n", argv[0]);
        return -1;
    }

    client = new Client();
    int initret = client->Init();
    if (BLADE_ERROR == initret)
        abort();

    int32_t     issafewrite = atoi(argv[1]);
    int32_t     buffersize = 1048600;
    char        *buf = (char *)malloc(buffersize);

    int         len;
    fullpath = path_alloc(&len);

    sourcepath = argv[2];
    destpath = argv[3];

    //int mkdirret = client->MakeDirectory(destpath);
    //if (mkdirret != BLADE_SUCCESS && mkdirret != -RET_DIR_EXIST) {
    //    LOGV(LL_ERROR, "Makedirectory destpath %s error.", destpath.c_str());
    //    return -1;
    //}

    strncpy(fullpath, sourcepath.c_str(), len);
    fullpath[len - 1] = 0;
    LOGV(LL_INFO, "fullpath: %s, sourcepath: %s, destpath: %s", fullpath, sourcepath.c_str(), destpath.c_str());
    dopath(buf, issafewrite, buffersize);

    free(buf);
    delete client;
}
