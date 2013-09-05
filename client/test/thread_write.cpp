#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdio.h>
#include "int_to_string.h"
#include "client.h"
#include "time_util.h"
#include "blade_common_define.h"

using namespace bladestore::client;
using bladestore::common::BLADE_ERROR;
using namespace pandora;

int32_t THREAD_NUM;
int32_t CREATE_NUM;

void *create(void *arg)
{
	int32_t threadnum = *((int32_t *)arg);
    Client client;
    int initret = client.Init();
    if (BLADE_ERROR == initret)
        abort();

	for(int32_t i = 0;i < CREATE_NUM;i++)
	{
		char current_time[20];
		int64_t fid;
		ssize_t readlocallen;
		int64_t writelen;
		int writelentotal=0;
		int32_t buffersize = 10485760;   //1M : 1 << 20 
		char *buf = (char *)malloc(buffersize);
		string src = Int32ToString(threadnum) + Int32ToString(i);
		int localfid = open("/opt/BladeStore/100Mfile", O_RDONLY);//本地文件

		fid = client.Create(src, 0, 3);
		if (fid < 0) 
		{  
		    printf("creat file error\n");
			pthread_exit(arg);
		} 
		else 
		{     
			do {
				readlocallen = read(localfid, (void *)buf, buffersize);
				if (readlocallen > 0) {
				    writelen = client.Write(fid, buf, readlocallen);
					if (readlocallen != writelen)
					{
						printf("############not all write:read:%ld,write:%ld######################\n",readlocallen,writelen);
						pthread_exit(arg);
					}
					if (writelen != readlocallen)
				    {
						printf("@@@@@@@@@@@@@@@@@@error in write@@@@@@@@@@@@@@@@@@@@@@@@@\n");
						pthread_exit(arg);
					} 
					writelentotal += writelen;
				} 
            } while (readlocallen > 0);

			client.Close((int64_t)fid);
			close(localfid);
			free(buf);
		}     
	}
	pthread_exit(arg);
}

int32_t main(int argc,char **argv)
{
    if (argc != 3)
    {
	    printf("./exefile threadnum createnum\n");
		return -1;
 	}
	THREAD_NUM = atoi(argv[1]);
	CREATE_NUM = atoi(argv[2]);
	pthread_t *thread_queue[THREAD_NUM];

    //record the beginning time
    int64_t begin = TimeUtil::GetTime();
    
	//start threads
	for(int32_t i = 0;i<THREAD_NUM;i++)
 	{ 
	    int32_t err = pthread_create(thread_queue[i],NULL,create,&i);
		if(err != 0)
			printf("cannot create thread %d: %s\n",i,strerror(err));
	}

	//join threads
    for(int32_t i = 0;i<THREAD_NUM;i++)
	{   
	    int32_t err = pthread_join(*thread_queue[i],NULL);
		if(err != 0)
			printf("cannot join with thread %d: %s\n",i,strerror(err));
 	} 

	//sleep
	while(1)
	{  
	    sleep(1);
	} 

	//recode the ending time
    int64_t end = TimeUtil::GetTime();

	//calculate 
    double used = (end -begin)/1000000.000000;
	printf("IOPS:  create -> %lf \n",CREATE_NUM * THREAD_NUM / used);
    return 0;
}


