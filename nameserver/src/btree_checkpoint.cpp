#include <iostream>
#include <assert.h>
#include <ctime>
#include <csignal>

#include "btree_checkpoint.h"
#include "blade_common_define.h"
#include "btree_entry.h"
#include "btree_meta.h"
#include "base.h"
#include "meta.h"

using namespace bladestore::common;

namespace bladestore
{
namespace nameserver
{

BtreeCheckPoint::BtreeCheckPoint(MetaTree & meta_tree) : meta_tree_(meta_tree)
{ 
	disk_entry_map_ = new DiskEntry(this);
	init_entry_map();
} 

BtreeCheckPoint::~BtreeCheckPoint() 
{
	if (NULL != disk_entry_map_)
	{
		delete disk_entry_map_;
		disk_entry_map_ = NULL;
	}
}

int BtreeCheckPoint::write_leaves()
{
    BtreeReadHandle read_handle;
	MetaTree::MetaBtree * meta_btree = meta_tree_.get_meta_btree();
	int res;
    Meta * value = NULL;
    const Key fkey(BLADE_MIN, MetaTree::MINFID, MetaTree::MIN_CHUNK_OFFSET);
    const Key tkey(BLADE_MAX, MetaTree::MAXFID, MetaTree::MAX_CHUNK_OFFSET);
    meta_btree->get_read_handle(read_handle);
    meta_btree->set_key_range(read_handle, &fkey, 0, &tkey, 0);
	while(ERROR_CODE_OK == meta_btree->get_next(read_handle, value))
	{
		assert(NULL != value);
		res = value->checkpoint(offile_);	
		if (res != 0)
		{
			return 1;
		}
		value = NULL;
	}

	return 0;
}

int BtreeCheckPoint::do_cp(char * cpname)
{
	offile_.open(cpname);
	int status = offile_.fail() ? -EIO : 0;
	if (0 == status) 
	{
		offile_ << "version/" << VERSION << '\n';
		offile_ << "fid/" << fileID.getseed() << '\n';
		offile_ << "chunk_Id/" << chunkID.getseed() << '\n';
		offile_ << "chunk_version_inc/" << chunkVersionInc << '\n';
		time_t t = time(NULL);
		offile_ << "time/" << ctime(&t);
		status = write_leaves();
		offile_.close();
	}
	return status == 0 ? BLADE_SUCCESS : BLADE_ERROR;
}

bool BtreeCheckPoint::Rebuild(char * cpname, int16_t min_replicas)
{
	const int MAXLINE = 400;
	char line[MAXLINE];
	int lineno = 0;

	iffile_.open(cpname);
	bool is_ok = !iffile_.fail();

	assert(NULL != disk_entry_map_ );

	disk_entry_map_->set_min_replicas(min_replicas);

	while (is_ok && !iffile_.eof()) 
	{
		++lineno;
		iffile_.getline(line, MAXLINE);
		is_ok = disk_entry_map_->Parse(line);
		if (!is_ok)
		{
			std::cerr << "Error at line " << lineno << ": " << line << '\n';
		}
	}

	iffile_.close();
	return is_ok ? BLADE_SUCCESS : BLADE_ERROR;
}

void BtreeCheckPoint::init_entry_map()
{
	disk_entry_map_->init_map();
}

}//end of namespace nameserver
}//end of namespace bladestore
