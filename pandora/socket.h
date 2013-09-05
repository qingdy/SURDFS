/*
* Copyright (c) 2012£¬Sohu R&D
* All rights reserved.
* 
* File   name : socket.h
* Description : 
* 
* Version : 1.0
* Author  : binghan@sohu-inc.com
* Date    : 2012-01-04
*/

#ifndef PANDORA_SYSTEM_NET_SOCKET_H
#define PANDORA_SYSTEM_NET_SOCKET_H

#include <stdio.h>
#include <string.h>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include "thread_mutex.h"

namespace pandora
{
typedef int SOCKET;

class Socket
{
public:
    Socket();
    ~Socket();
    bool set_address(const char* address, const int port);
    bool set_address(struct sockaddr *host_addr);
    struct sockaddr_in address();
    std::string GetAddrAsStr();
    bool set_socket_handle(SOCKET handle);
    SOCKET socket_handle();
    bool CheckSocketHandle();

    bool Create(int af = AF_INET, int type = SOCK_STREAM, int protocol = 0);
    void Close();
    void Shutdown();
    int Write(const void* data, int len);
    int Read(void* data, int len);

    //attach a socket handle to this object
    void Attach(SOCKET handle);
    //detach socket handle of this object
    SOCKET Detach();

    bool SetCloexec(bool value);
    bool SetKeepAlive(bool on);
    bool SetReuseAddress(bool reuseaddr_enabled);
    bool SetLinger(bool do_linger, int seconds);
    bool SetBlocking(bool blocking_enabled);
    bool SetSendTimeout(const struct timeval& time_val);
    bool SetRecvTimeout(const struct timeval& time_val);
    bool SetSendBufferSize(size_t size);
    bool SetReceiveBufferSize(size_t size);
    bool SetTcpNoDelay(bool no_delay);

    int GetSoError();
    static int GetLastError() { return errno; };
protected:
    struct sockaddr_in  address_;
    SOCKET socket_handle_;
    static CThreadMutex mutex_;
};
}
#endif 
