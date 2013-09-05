/*
 *version  : 1.0
 *author   : chen yunyun
 *date     : 2012-5-10
 *
 */
#ifndef BLADESTORE_NAMESERVER_BTREE_ENTRY_H 
#define BLADESTORE_NAMESERVER_BTREE_ENTRY_H 

#include <string>
#include <deque>
#include <map>

#include "types.h"
#include "btree_meta.h"
#include "base.h"
#include "meta.h"

using namespace std;
using namespace bladestore::btree;

namespace bladestore
{
namespace nameserver
{

class BtreeCheckPoint; 

//对BTREE的checkpoint文件进行restore
class DiskEntry 
{
public:
	DiskEntry(BtreeCheckPoint * check_point);
	~DiskEntry();

public:
	typedef bool (DiskEntry::*parser)(deque <string> &c); 


	void add_parser(string k, parser f) 
	{ 
		table_[k] = f; 
	}

	//初始化处理函数
	void init_map();

	bool Parse(char *line);	

	void set_min_replicas(int16_t min_replicas)
	{
		min_replicas_ = min_replicas;
	}

private:
	bool pop_name(string &name, const string tag, deque <string> &c, bool ok);
	bool pop_path(string &path, const string tag, deque <string> &c, bool ok);
	bool pop_fid(int64_t &fid, const string tag, deque <string> &c, bool ok);
	bool pop_size(size_t &sz, const string tag, deque <string> &c, bool ok);
	bool pop_offset(off_t &o, const string tag, deque <string> &c, bool ok);
	bool pop_short(int16_t &n, const string tag, deque <string> &c, bool ok);
	bool pop_type(FileType &t, const string tag, deque <string> &c, bool ok);
	bool pop_time(struct timeval &tv, const string tag, deque <string> &c, bool ok);
	bool restore_chunk_version_inc(deque <string> &c);

	bool checkpoint_seq(deque <string> &c);
	bool checkpoint_fid(deque <string> &c);	
	bool checkpoint_chunkId(deque <string> &c);
	bool checkpoint_version(deque <string> &c);
	bool checkpoint_time(deque <string> &c);
	bool checkpoint_log(deque <string> &c);
	bool restore_chunkVersionInc(deque <string> &c);
	bool restore_dentry(deque <string> &c);
	bool restore_fattr(deque <string> &c);
	bool restore_chunkinfo(deque <string> &c);

private:
	typedef map <string, parser> parsetab;

	static const char SEPARATOR = '/';
	BtreeCheckPoint * check_point_;
	parsetab table_;

	int16_t min_replicas_;
};

}//end of namespace nameserver
}//end of namespace bladestore

#endif
