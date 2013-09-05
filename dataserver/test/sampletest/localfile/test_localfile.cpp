#include <stdlib.h>
#include <pthread.h>
#include <iostream>
#include <string>
#include <map>
#include "log.h"
#include "block.h"
#include "fsdataset.h"
#include "fsdir.h"
#include "helper.h"
#include "block_meta_info.h"
#include "cthread.h"
using namespace std;
using namespace bladestore::dataserver;
using namespace bladestore::common;
using namespace pandora;

FSDataSet fsdataset_;
int32_t thread_num = 31;
int32_t page = 10;

class WriteBlockRunnable:public Runnable
{
public :
    void Run(CThread * thread, void * arg)
    {
        LOGV(LL_INFO,"start thread[%ld]", *(int64_t*)arg);
        WriteBlockFd *writeblockfd;
        int64_t block_id = *(int64_t*)arg;
        int64_t idx_start = (block_id-1)*page + 1;
        int64_t idx_end   =  block_id * page;
        int64_t idx;
        for(idx = idx_start;idx <= idx_end; idx++)
        {
            Block b(idx,0,1,50);
            LOGV(LL_INFO,"start to write block:%ld",idx);
            writeblockfd = fsdataset_.WriteBlockAndMetaFd(b);
            //fsdataset_.CloseFile(b,writeblockfd->block_fd_,writeblockfd->meta_fd_,kWriteCompleteMode);
            sleep(1);
        }
    }
};

class RemoveBlockRunnable:public Runnable
{
public :
    void Run(CThread * thread, void * arg)
    {
        LOGV(LL_INFO,"start thread[%ld]", *(int64_t*)arg);
        int64_t block_id = *(int64_t*)arg;
        int64_t idx_start = (block_id-1)*page + 1;
        int64_t idx_end   =  block_id * page;
        int64_t idx;
        set<int64_t> block_id_set;
        for(idx = idx_start;idx <= idx_end; idx++)
        {
            block_id_set.insert(idx);
            fsdataset_.RemoveBlocks(block_id_set);
            block_id_set.erase(idx);
            sleep(1);
        }
    }
};

int main()
{
    fsdataset_.Init("/opt/bladestore/fstest","/opt/bladestore/fstest_tmp");
    WriteBlockRunnable wr;
    CThread *write_thread[thread_num];
   
	int64_t i;

    int64_t block_id[thread_num];

    for(i = 1;i < thread_num;i++)
    {
        write_thread[i] = new CThread();
        block_id[i] = i;
        write_thread[i]->Start(&wr,&block_id[i]);
    }

    for(i = 1;i < thread_num;i++)
    {
        write_thread[i]->Join();
    }

    hash_map<int64_t,BlockMetaInfo> block_map;
    fsdataset_.GetBlockMap(block_map);

    LOGV(LL_INFO,"block map size:%d",block_map.size());

    CThread *read_thread[thread_num];
    RemoveBlockRunnable rr;
    for(i = 1;i < thread_num; i++)
    {
        read_thread[i] = new CThread();
        block_id[i] = i;
        read_thread[i]->Start(&rr,&block_id[i]);
    }

    for(i = 1;i < thread_num;i++)
    {
        read_thread[i]->Join();
    }
    
    fsdataset_.GetBlockMap(block_map);
    LOGV(LL_INFO,"block map size:%d",block_map.size());

    while(1)
    {
        sleep(1);
    }
    return 0;
}
