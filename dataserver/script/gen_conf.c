#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <string.h>

typedef struct machineInfo {
    char ip[32];
    //int disk_no;
    //int port;
} MacInfo;

const int DISK_NUM = 12;
const short START_PORT = 12331;
const int MAX_MACHINE = 30;

void _trim(char *buf)
{
    if (!buf) return ;

    char *cur = buf;
    while (*cur != '\0') cur++;
    while (--cur > buf) {
        if (!isspace(*cur))
           break; 
    }
    if (cur > buf) {
        *(++cur) = '\0';
    }

    return ;
}

int main(int argc, char **argv)
{
    //open input file & output file
    FILE *fp_in;
    FILE *fp_out;
    fp_in = fopen("./ip_list.txt", "rb");
    if (!fp_in) {
        printf("open ip_list.txt failed, errmsg=%s\n", strerror(errno));
        exit(-1);
    }
    fp_out = fopen("./conf.txt", "w+");
    if (!fp_out) {
        printf("open conf.txt failed, errmsg=%s\n", strerror(errno));
        exit(-1);
    }

    MacInfo machines[MAX_MACHINE];
    int machine_num = 0;
    char buf[256];
    int idx = 0;
    while (fgets(buf, 256, fp_in) != NULL) {
        _trim(buf);
        printf("reading ip: %s\n", buf);
        strcpy(machines[idx].ip, buf);
        ++idx;
    }
    machine_num = idx;
    if ((machine_num % 3) != 0) {
        printf("machine number not multiple of 3, machine_num=%d\n", machine_num);
        exit(-1);
    }

    int group_idx = 0;
    int group_id = 1;
    int i;
    int j;
    int k;
    int selectIdx[3];

    for (i = 0; i < machine_num; i += 3) {
        //fill all of these three machines
        int port = START_PORT;
        for (j = 1; j <= DISK_NUM; ++j) {
            fprintf(fp_out, "##Group %d\n", group_idx);
            fprintf(fp_out, "GROUP[%d]=%d\n", group_idx, group_id);

            int tmp = j % 3; 
            for (k = 0; k < 3; ++k) {
                selectIdx[k] = tmp;
                tmp = (tmp + 1) % 3;
            }

            for (k = 0; k < 3; ++k) {
                fprintf(fp_out, "ip%d[%d]=%s\n", k, group_id, machines[i+selectIdx[k]].ip);
                fprintf(fp_out, "port%d[%d]=%d\n", k, group_id, port);
                fprintf(fp_out, "path%d[%d]=/data%d/dataservice_%d\n", k, group_id, j, group_id);
                fprintf(fp_out, "user%d[%d]=bladefs\n", k, group_id);
                fprintf(fp_out, "passwd%d[%d]=bladefs\n", k, group_id);
                fprintf(fp_out, "\n");
            }
            ++port;
            ++group_idx;
            ++group_id;
        }
    }

    printf("=======finish generating conf.txt==========\n");
    printf("total machine number: %d\n", machine_num);
    printf("total group number: %d\n", group_idx);

    fclose(fp_in);
    fclose(fp_out);
    return 0;
}
