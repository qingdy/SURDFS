/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef	BLADESTORE_HA_NS_LOG_WORKER_H 
#define BLADESTORE_HA_NS_LOG_WORKER_H 

#include "blade_log_entry.h"
#include "blade_packet_factory.h"
#include "blade_packet.h"

namespace bladestore
{
namespace nameserver
{
class MetaManager;
}
}

using namespace bladestore::message;
using namespace bladestore::nameserver;

namespace bladestore
{
namespace ha
{
class BladeNsLogManager;


//有2个作用
//1.master:用来向slave发送redo日志
//2.slave :用来replay master发送过来的日志
class BladeNsLogWorker
{
public:
	BladeNsLogWorker();
	~BladeNsLogWorker();

public:
	void SetLogManager(BladeNsLogManager * log_manager);
	void SetMetaManager(MetaManager * meta_manager);
	int FlushLog(const LogCommand cmd, const char* log_buffer, const int64_t& serialize_size);

	int Apply(LogCommand cmd, char* log_data, const int64_t& data_len);

	//保证nameserver_和log_manager_不为NULL,在做实际操作的时候需要首先调用这个函数进行检查
	bool IsEssentialInit();

    int BladeBadBlockReport(LogCommand cmd, char * data, int length);
    int BladeBlockReport(LogCommand cmd, char * data, int length);
	int BladeBlockReceived(LogCommand cmd, char * data, int length);
    int BladeRegister(LogCommand cmd, char * data, int length);
	int BladeCreate(LogCommand cmd, char * data, int length);
    int BladeMkdir(LogCommand cmd, char * data, int length);
    int BladeAddBlock(LogCommand cmd, char * data, int length);
    int BladeComplete(LogCommand cmd, char * data, int length);
    int BladeAbandonBlock(LogCommand cmd, char * data, int length);
    int BladeRename(LogCommand cmd, char * data, int length);
    int BladeDeleteFile(LogCommand cmd, char * data, int length);
    int BladeExpireLease(LogCommand cmd, char * data, int length);
    int BladeLeaveDs(LogCommand cmd, char * data, int length);

	int DoCheckPoint(const char* log_data, const int64_t& log_length);

public:
	void Exit();

private:
	BladeNsLogManager* log_manager_;
	MetaManager * meta_manager_;
	BladePacketFactory * packet_factory_;
};

}//end of namespace ha
}//end of namespace bladestore

#endif 

