#include <errno.h>

#include "blade_net_util.h"

using namespace std;
namespace bladestore
{
namespace common
{
uint32_t BladeNetUtil::GetLocalAddr(const char *dev_name)
{
    int             fd, intrface;
    struct ifreq    buf[16];
    struct ifconf   ifc;

    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) <= 0) 
	{
        return 0;
    }
  
    ifc.ifc_len = sizeof(buf);
    ifc.ifc_buf = (caddr_t) buf;
    if (ioctl(fd, SIOCGIFCONF, (char *) &ifc)) 
	{
        close(fd);
        return 0;
    }
    
    intrface = ifc.ifc_len / sizeof(struct ifreq);
    while (intrface-- > 0)
    {
        if(ioctl(fd,SIOCGIFFLAGS,(char *) &buf[intrface])) 
		{
            continue;
        }
        if(buf[intrface].ifr_flags&IFF_LOOPBACK) continue;
        if (!(buf[intrface].ifr_flags&IFF_UP)) continue;
        if (dev_name != NULL && strcmp(dev_name, buf[intrface].ifr_name)) continue;
        if (!(ioctl(fd, SIOCGIFADDR, (char *) &buf[intrface]))) 
		{
            close(fd);
            return ((struct sockaddr_in *) (&buf[intrface].ifr_addr))->sin_addr.s_addr;
        }
    }
    close(fd);
    return 0;
}

bool BladeNetUtil::IsLocalAddr(uint32_t ip, bool loopSkip)
{
    int             fd, intrface;
    struct ifreq    buf[16];
    struct ifconf   ifc;

    if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) <= 0) 
	{
        return false;
    }
  
    ifc.ifc_len = sizeof(buf);
    ifc.ifc_buf = (caddr_t) buf;
    if (ioctl(fd, SIOCGIFCONF, (char *) &ifc)) 
	{
        close(fd);
        return false;
    }
    
    intrface = ifc.ifc_len / sizeof(struct ifreq);
    while (intrface-- > 0)
    {
        if(ioctl(fd,SIOCGIFFLAGS,(char *) &buf[intrface])) 
		{
            continue;
        }
        if(loopSkip && buf[intrface].ifr_flags&IFF_LOOPBACK) continue;
        if (!(buf[intrface].ifr_flags&IFF_UP)) continue;
        if (ioctl(fd, SIOCGIFADDR, (char *) &buf[intrface])) 
		{
            continue;
        }
        if (((struct sockaddr_in *) (&buf[intrface].ifr_addr))->sin_addr.s_addr == ip) 
		{
            close(fd);
            return true;
        }
    }
    close(fd);
    return false;
}

uint32_t BladeNetUtil::GetAddr(const char *ip)
{
    if (ip == NULL) return 0;
    int x = inet_addr(ip);
    if (x == (int)INADDR_NONE) 
	{
        struct hostent *hp;
        if ((hp = gethostbyname(ip)) == NULL) 
		{
            return 0;
        }
        x = ((struct in_addr *)hp->h_addr)->s_addr;
    }
    return x;
}

string BladeNetUtil::AddrToString(uint64_t ipport)
{
    char str[32];
    uint32_t ip = (uint32_t)(ipport & 0xffffffff);
    int port = (int)((ipport >> 32 ) & 0xffff);
    unsigned char *bytes = (unsigned char *) &ip;
    if (port > 0) 
	{
        sprintf(str, "%d.%d.%d.%d:%d", bytes[0], bytes[1], bytes[2], bytes[3], port);
    } 
	else 
	{
        sprintf(str, "%d.%d.%d.%d", bytes[0], bytes[1], bytes[2], bytes[3]);
    }
    return str;
}

string BladeNetUtil::AddrToIP(uint64_t ipport, int *port)
{
    char str[32];
    uint32_t ip = (uint32_t)(ipport & 0xffffffff);
    *port = (int)((ipport >> 32 ) & 0xffff);
    unsigned char *bytes = (unsigned char *) &ip;
    sprintf(str, "%d.%d.%d.%d", bytes[0], bytes[1], bytes[2], bytes[3]);
    return str;
}

uint64_t BladeNetUtil::StrToAddr(const char *ip, int port)
{
    uint32_t nip = 0;
    const char *p = strchr(ip, ':');
    if (NULL != p && p>ip) 
	{
        int len = p-ip;
        if (len>64) len = 64;
        char tmp[128];
        strncpy(tmp, ip, len);
        tmp[len] = '\0';
        nip = GetAddr(tmp);
        port = atoi(p+1);
    } 
	else 
	{
        nip = GetAddr(ip);
    }
    if (nip == 0) 
	{
        return 0;
    }
    uint64_t ipport = port;
    ipport <<= 32;
    ipport |= nip;
    return ipport;
}

uint64_t BladeNetUtil::StringToAddr(string ip_port)
{
    char  ip[32];
    int port;
    sscanf(ip_port.c_str(), "%s:%d", ip, &port);
    return StrToAddr(ip,port);
}

uint64_t BladeNetUtil::IPToAddr(uint32_t ip, int port)
{
    uint64_t ipport = port;
    ipport <<= 32;
    ipport |= ip;
    return ipport;
}

uint64_t BladeNetUtil::GetPeerID(int64_t socket_fd) 
{
	if (socket_fd == -1) 
		return 0;

	struct sockaddr_in peer;
	socklen_t length = sizeof(peer);
	if (getpeername(socket_fd ,(struct sockaddr*)&peer, &length) == 0) 
	{
		return BladeNetUtil::IPToAddr(peer.sin_addr.s_addr, ntohs(peer.sin_port));
	}
	return 0;
}

int BladeNetUtil::GetSockError(int64_t socket_fd)
{
    if (socket_fd == -1) 
	{                                                                                                  
        return EINVAL;
    }

    int last_error = errno;
    int so_error = 0;
    socklen_t so_error_len = sizeof(so_error);
    if (getsockopt(socket_fd, SOL_SOCKET, SO_ERROR, (void *)(&so_error), &so_error_len) != 0) 
	{
        return last_error;
    } 
    if (so_error_len != sizeof(so_error))
	{
        return EINVAL;
	}

    return so_error;
}

}//end of namespace common
}//end of namespace bladestore
