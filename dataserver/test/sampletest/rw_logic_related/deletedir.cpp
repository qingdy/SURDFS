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
string deleteddir;
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

int dopath()
{
    int             ret;
    char            *ptr;
    FileInfo finfo = client->GetFileInfo(fullpath);
    if (0 == finfo.get_file_id()) {
        LOGV(LL_ERROR, "GetFileInfo error");
        return -1;
    }
    if (1 == finfo.get_file_type()) {
        int32_t deleteret = client->Delete(fullpath);
        if (0 == deleteret) {
            LOGV(LL_INFO, "Delete file success");
            return 0;
        } else {
            LOGV(LL_ERROR, "delete error, file: %s.", fullpath);
            return -1;
        }
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

        if ((ret = dopath()) != 0)
            break;
    }

    ptr[-1] = 0;

    int32_t deleteret = client->Delete(fullpath);
    if (0 == deleteret) {
        LOGV(LL_INFO, "Delete file success");
        return 0;
    } else {
        LOGV(LL_ERROR, "delete error, file: %s.", fullpath);
        return -1;
    }

    return ret;
}

int main (int argc, char **argv)
{
    if (argc != 2) {
        printf("%s deletedpath\n", argv[0]);
        return -1;
    }

    client = new Client();
    int initret = client->Init();
    if (BLADE_ERROR == initret)
        abort();

    int         len;
    fullpath = path_alloc(&len);

    deleteddir = argv[1];

    if (string(argv[1]) == "/") {
        vector<string> filelist = client->GetListing("/");
        for (vector<string>::iterator it = filelist.begin(); it != filelist.end(); ++it) {
            if ((*it) != "/") {
                deleteddir = "/" + (*it);
                strncpy(fullpath, deleteddir.c_str(), len);
                fullpath[len - 1] = 0;
                LOGV(LL_INFO, "fullpath: %s, deleteddir: %s", fullpath, deleteddir.c_str());
                dopath();
            }
               
        }

    } else {
        strncpy(fullpath, deleteddir.c_str(), len);
        fullpath[len - 1] = 0;
        LOGV(LL_INFO, "fullpath: %s, deleteddir: %s", fullpath, deleteddir.c_str());
        dopath();
    }

//    int32_t readret = readfile(client, buf, buffersize);
//    if (0 == readret) {
//        LOGV(LL_INFO, "Read file success");
//    } else {
//        LOGV(LL_ERROR, "read error");
//        //return -1;
//    }
//
//    // Desend to delete all files
//    deletefiles();
    //for (int32_t i = 0; i < 10; ++i) {
    //    vector<string> file_list = client->GetListing(dname[i]);
    //    for (vector<string>::iterator it = file_list.begin(); it != file_list.end(); ++it) {
    //        FileInfo finfo = client->GetFileInfo(dname[i] + "/" + *it);
    //        if (0 == finfo.get_file_id()) {
    //            LOGV(LL_ERROR, "GetFileInfo error");
    //            //return -1;
    //        }
    //        // a common file, not dir
    //        int32_t deleteret;
    //        if (1 == finfo.get_file_type()) {
    //            deleteret = client->Delete(dname[i] + "/" + *it);
    //            if (RET_SUCCESS != deleteret && deleteret != -RET_FILE_NOT_EXIST) {
    //                LOGV(LL_ERROR, "delete file error");
    //                // return -1;
    //            }
    //        }
    //    }
    //}
    delete client;
}
