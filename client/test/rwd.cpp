#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <net/if.h>
#include <net/if_arp.h>
#include <arpa/inet.h>
#include <time.h>
#include "time_util.h"
#include "log.h"
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "blade_file_info.h"
#include "int_to_string.h"
#include "client.h"

#define random(x) (rand()%x)

using namespace pandora;
using namespace bladestore::client;
using namespace bladestore::common;
using namespace bladestore::message;

string dname[] = {"/L1a", "/L1a/L2a", "/L1a/L2a/L3a", "/L1a/L2a/L3a/L4a",
                  "/L1a/L2a/L3a/L4a/L5a", "/L1b", "/L1b/L2b", "/L1b/L2b/L3b",
                  "/L1b/L2b/L3b/L4b", "/L1b/L2b/L3b/L4b/L5b"};
vector<string> filenames;

int32_t writefiles(Client *client, void *buf, int8_t issafewrite, int32_t buffersize, string& prefix)
{
    for (int32_t i = 0; i < 5; ++i) {
        int64_t filesize = random(1073741824);
        int64_t localfid = open("/data4/BladeStore/client/test/readfile", O_RDONLY);
        if (localfid < 0) {
            LOGV(LL_ERROR, "Open Localfile error.");
            return -1;
        }
    
        // Create a file
        int64_t fid;
        do {
            char current_time[20];
            string prefixissafewrite = "0";
            prefixissafewrite[0] += issafewrite;
            string filename = dname[random(10)] + "/" + prefixissafewrite + prefix + Int32ToString(i) +
                              TimeUtil::TimeToStr(time(NULL), current_time);
    
            LOGV(LL_INFO, "Creating file: %s .", filename.c_str());
            fid = client->Create(filename, issafewrite, 3);
            LOGV(LL_INFO, "File created, ID: %ld.", fid);
            if (fid < 0) {
                LOGV(LL_ERROR, "Create file error.");
                if (-RET_FILE_EXIST == fid)
                    continue;
                return -1;
            }
            filenames.push_back(filename);
        } while (-RET_FILE_EXIST == fid);
    
        int64_t alreadyread = 0;
        do {
            int32_t newsize = buffersize;
            if (alreadyread + buffersize >= filesize)
                newsize = filesize - alreadyread;
            int64_t readlocallen = read(localfid, buf, newsize);
            if (readlocallen <= 0) {
                LOGV(LL_ERROR, "read local file error.");
                return -1;
            }
            int64_t writelen = client->Write(fid, buf, readlocallen);
            if (writelen != readlocallen) {
                LOGV(LL_ERROR, "error in write");
                return -1;
            }
            alreadyread += writelen;
        } while (alreadyread < filesize);
    
        if (alreadyread != filesize) {
            LOGV(LL_ERROR, "alreadyread should equal to filesize when written complete");
            return -1;
        }
        int closeret = client->Close(fid);
        if (closeret < 0) {
            LOGV(LL_ERROR, "Close file error");
            return -1;
        }
        close(localfid);
    }
    return 0;
}

int32_t readfile(Client *client, void *buf, int32_t buffersize)
{
    for (int i = 0; i < 3; ++i) {
        int randnum = random(10);
        vector<string> file_list = client->GetListing(dname[randnum]);
        for (vector<string>::iterator it = file_list.begin(); it != file_list.end(); ++it) {
            FileInfo finfo = client->GetFileInfo(dname[randnum] + "/" + *it);
            if (0 == finfo.get_file_id()) {
                LOGV(LL_ERROR, "GetFileInfo error");
                return -1;
            }
            if (1 != finfo.get_file_type()) {
                continue;
            }
            int fid = client->Open(dname[randnum] + "/" + *it, O_RDONLY);
            if (fid < 0) {
                LOGV(LL_ERROR, "Open file error.");
                return -1;
            }

            int localfid = open("tempdata", O_RDWR | O_APPEND | O_CREAT | O_TRUNC);
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
                    write(localfid, buf, readlen);
                } else {
                    LOGV(LL_INFO, "Read no data");
                }
            } while (readlen > 0);

            close(localfid);
            int closeret = client->Close(fid);
            if (closeret == -1)
                return -1;
        }
    }
    return 0;
}

int GetHostInfo(string &local_ip)
{
    struct sockaddr_in *addr;
    struct sockaddr_in myip;
    struct ifreq ifr;
    int sock;
    LOGV(LL_INFO, "GetHostInfo1");
    if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
        return -1;
    }
    strcpy(ifr.ifr_name, "eth0");
    LOGV(LL_INFO, "GetHostInfo2");
    if (ioctl(sock, SIOCGIFADDR, &ifr) < 0) {
        return -1;
    }
    myip.sin_addr = ((struct sockaddr_in *)(&ifr.ifr_addr))->sin_addr;
    local_ip = inet_ntoa(myip.sin_addr);
    close(sock);
    return 0;
}

int main (int argc, char **argv)
{
    if (argc != 3) {
        printf("%s issafewrite buffersize (0 for write, 1 for safewrite)\n", argv[0]);
        return -1;
    }

    string localip;
    //if (GetHostInfo(localip)) {
    //    LOGV(LL_ERROR, "GetLocalIpERROR");
    //    return -1;
    //}
    //LOGV(LL_INFO, "Local IP: %s", localip.c_str());
    //string prefix = localip.substr(localip.rfind('.') + 1);
    string prefix;

    Client *client = new Client();
    int initret = client->Init();
    if (BLADE_ERROR == initret)
        abort();

    srand(time(NULL));
    int32_t issafewrite = atoi(argv[1]);
    int32_t buffersize = atoi(argv[2]);
    char *buf = (char *)malloc(buffersize);

    for (int i = 0; i < 10; ++i) {
        int ret = client->MakeDirectory(dname[i]);
        if (ret != BLADE_SUCCESS && ret != -RET_DIR_EXIST) {
            LOGV(LL_ERROR, "Makedirectory error.");
        }
    }

    while (1) {
        int32_t writeret = writefiles(client, buf, issafewrite, buffersize, prefix);
        if (0 == writeret) {
            LOGV(LL_INFO, "Write 20 files success");
        } else {
            LOGV(LL_ERROR, "Write error");
            return -1;
        }

        int32_t readret = readfile(client, buf, buffersize);
        if (0 == readret) {
            LOGV(LL_INFO, "Read file success");
        } else {
            LOGV(LL_ERROR, "read error");
            //return -1;
        }

        // Delete all files
        for (vector<string>::iterator iter = filenames.begin(); iter != filenames.end(); ++iter) {
            int32_t deleteret = client->Delete(*iter);
                    if (BLADE_SUCCESS != deleteret && deleteret != -RET_FILE_NOT_EXIST) {
                        LOGV(LL_ERROR, "delete file error");
                    //    return -1;
                    }
        }
        filenames.clear();
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
    }
    free(buf);
    delete client;
}
