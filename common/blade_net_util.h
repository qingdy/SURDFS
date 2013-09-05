/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-4-6
 *
 */

#ifndef BLADESTORE_COMMON_NET_UTIL_H
#define BLADESTORE_COMMON_NET_UTIL_H

#include <stdint.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <netdb.h>
#include <sys/time.h>
#include <net/if.h>
#include <inttypes.h>
#include <sys/types.h>
#include <string>

using std::string;

namespace bladestore
{
namespace common
{
struct ipaddr_less 
{
    bool operator()(const uint64_t a, const uint64_t b) const 
	{
        uint64_t a1 = ((a & 0xFF) << 24) | ((a & 0xFF00) << 8) | ((a & 0xFF0000) >> 8) | ((a & 0xFF000000) >> 24);
        a1 <<= 32; a1 |= ((a>>32) & 0xffff);
        uint64_t b1 = ((b & 0xFF) << 24) | ((b & 0xFF00) << 8) | ((b & 0xFF0000) >> 8) | ((b & 0xFF000000) >> 24);
        b1 <<= 32; b1 |= ((b>>32) & 0xffff);
        return (a1<b1);
    }
};
//网络相关工具，包括获取本机ip地址，将ip与字符串相互转换等
class BladeNetUtil 
{
public:
	static uint32_t GetLocalAddr(const char *dev_name);
    static bool IsLocalAddr(uint32_t ip, bool loopSkip = true);
    static uint32_t GetAddr(const char *ip);
    static std::string AddrToString(uint64_t ipport);
    static uint64_t StrToAddr(const char *ip, int port);
    static uint64_t IPToAddr(uint32_t ip, int port);
	static uint64_t GetPeerID(int64_t socket_fd);
	static int  GetSockError(int64_t socket_fd);
    static string AddrToIP(uint64_t ipport, int *port);
    static uint64_t StringToAddr(string ip_port);
};

}//end of namespace common
}//end of namespace bladestore

#endif
