#include <string>
#include <unistd.h>
#include <fcntl.h>
#include "stdio.h"
#include "iostream.h"

int main()
{
    char *src = NULL;
    char *des = new char[100];
    memcpy(des, src, 0);
    int fd = open("yyy", O_RDWR |O_CREAT | O_TRUNC);
    ssize_t write_bytes = pwrite(fd, src, 0,0); 
    cout << write_bytes << endl;
    delete[] des;

}
