/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_HA_NS_LOG_REPLAY_H
#define BLADESTORE_HA_NS_LOG_REPLAY_H

#include "blade_log_replay_runnable.h"
#include "blade_log_entry.h"
#include "blade_meta_manager.h"
#include "blade_packet_factory.h"

namespace bladestore
{
namespace ha
{

class BladeNsLogManager;

class BladeNsLogReplay : public BladeLogReplayRunnable
{
public:
	BladeNsLogReplay();
	~BladeNsLogReplay();

public:
	void SetLogManager(BladeNsLogManager * log_manager);
	int Replay(LogCommand cmd, uint64_t seq, char* log_data, const int64_t data_len);

private:
	BladeNsLogManager * blade_ns_log_manager_;
};

}//end of namespace ha
}//end of namespace bladestore

#endif

