#include <assert.h>

#include "btree_entry.h"
#include "btree_checkpoint.h"
#include "util.h"

namespace bladestore
{
namespace nameserver
{

DiskEntry::DiskEntry(BtreeCheckPoint * check_point) : min_replicas_(3)
{
	assert(NULL != check_point);
	check_point_ = check_point;	
}

DiskEntry::~DiskEntry()
{


}

bool DiskEntry::Parse(char *line)
{
	const string l(line);
	deque <string> component;

	if (l.empty())
	{
		return true;
	}

	Split(component, l, SEPARATOR);
	parsetab::iterator c = table_.find(component[0]);
	return (c != table_.end() && (this->*(c->second))(component));
}

/*!
 * \brief remove a file name from the front of the deque
 * \param[out]	name	the returned name
 * \param[in]	tag	the keyword that precedes the name
 * \param[in]	c	the deque of components from the entry
 * \param[in]	ok	if false, do nothing and return false
 * \return		true if parse was successful
 *
 * The ok parameter short-circuits parsing if an error occurs.
 * This lets us do a series of steps without checking until the
 * end.
 */
bool DiskEntry::pop_name(string &name, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 2 || c.front() != tag)
	{
		return false;
	}

	c.pop_front();
	name = c.front();
	c.pop_front();
	if (!name.empty())
	{
		return true;
	}

	/*
	 * Special hack: the initial entry for "/" shows up
	 * as two empty components ("///"); I should probably
	 * come up with a more elegant way to do this.
	 */
	if (c.empty() || !c.front().empty())
	{
		return false;
	}

	c.pop_front();
	name = "/";
	return true;
}

/*
 * brief remove a path name from the front of the deque
 * @param[out]	path	the returned path
 * @param[in]	tag	the keyword that precedes the path
 * @param[in]	c	the deque of components from the entry
 * @param[in]	ok	if false, do nothing and return false
 * @return		true if parse was successful
 *
 * The ok parameter short-circuits parsing if an error occurs.
 * This lets us do a series of steps without checking until the
 * end.
 */
bool DiskEntry::pop_path(string &path, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 2 || c.front() != tag)
	{
		return false;
	}

	c.pop_front();
	/* Collect everything else in path with components separated by '/' */
	path = "";
	while (1) 
	{
		path += c.front();
		c.pop_front();
		if (c.empty())
		{
			break;
		}
		path += "/";
	}

	return true;
}

/*
 *brief remove a file ID from the component deque
 */
bool DiskEntry::pop_fid(int64_t &fid, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 2 || c.front() != tag)
	{
		return false;
	}

	c.pop_front();
	fid = to_number(c.front());
	c.pop_front();

	return (fid != -1);
}

/*
 *brief remove a size_t value from the component deque
 */
bool DiskEntry::pop_size(size_t &sz, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 2 || c.front() != tag)
	{
		return false;
	}

	c.pop_front();
	sz = to_number(c.front());
	c.pop_front();
	return (sz != -1u);
}

/*
 * brief remove a short value from the component deque
 */
bool DiskEntry::pop_short(int16_t &num, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 2 || c.front() != tag)
	{
		return false;
	}

	c.pop_front();
	num = (int16_t) to_number(c.front());
	c.pop_front();

	return (num != (int16_t) -1);
}

/*!
 * \brief remove a off_t value from the component deque
 */
bool DiskEntry::pop_offset(off_t &o, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 2 || c.front() != tag)
	{
		return false;
	}

	c.pop_front();
	o = to_number(c.front());
	c.pop_front();

	return (o != -1);
}

/*
 *brief remove a file type from the component deque
 */
bool DiskEntry::pop_type(FileType &t, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 2 || c.front() != tag)
	{
		return false;
	}

	c.pop_front();
	string type = c.front();
	c.pop_front();
	if (type == "file") 
	{
		t = BLADE_FILE;
	} else if (type == "dir") 
	{
		t = BLADE_DIR;
	} else
	{
		t = BLADE_NONE;
	}

	return (t != BLADE_NONE);
}

/*!
 * \brief remove a time value from the component deque
 */
bool DiskEntry::pop_time(struct timeval &tv, const string tag, deque <string> &c, bool ok)
{
	if (!ok || c.size() < 3 || c.front() != tag)
	{
		return false;
	}

	c.pop_front();
	tv.tv_sec = to_number(c.front());
	c.pop_front();
	tv.tv_usec = to_number(c.front());
	c.pop_front();
	return (tv.tv_sec != -1 && tv.tv_usec != -1);
}

bool DiskEntry::checkpoint_seq(deque <string> &c)
{
	c.pop_front();
	if (c.empty())
	{
		return false;
	}
	int64_t highest = to_number(c.front());
//	oplog.set_seqno(highest);
	return (highest >= 0);
}

bool DiskEntry::checkpoint_fid(deque <string> &c)
{
	c.pop_front();
	if (c.empty())
	{
		return false;
	}
	int64_t highest = to_number(c.front());
	fileID.setseed(highest);
	return (highest > 0);
}

bool DiskEntry::checkpoint_chunkId(deque <string> &c)
{
	c.pop_front();
	if (c.empty())
	{
		return false;
	}
	int64_t highest = to_number(c.front());
	chunkID.setseed(highest);
	return (highest > 0);
}

bool DiskEntry::checkpoint_version(deque <string> &c)
{
	c.pop_front();
	if (c.empty())
		return false;
	int version = to_number(c.front());
	return version == BtreeCheckPoint::VERSION;
}

bool DiskEntry::checkpoint_time(deque <string> &c)
{
	c.pop_front();
	std::cout << "restoring from checkpoint of " << c.front() << '\n';
	return true;
}

bool DiskEntry::checkpoint_log(deque <string> &c)
{
	c.pop_front();
	string s;
	while (!c.empty()) 
	{
		s += c.front();
		c.pop_front();
		if (!c.empty())
		{
			s += "/";
		}
	}
	std::cout << "log file is " << s << '\n';
	return true;
}

bool DiskEntry::restore_chunkVersionInc(deque <string> &c)
{
	c.pop_front();
	if (c.empty())
	{
		return false;
	}
	chunkVersionInc = to_number(c.front());
	return (chunkVersionInc >= 1);
}

bool DiskEntry::restore_dentry(deque <string> &c)
{
	string name;
	int64_t id, parent;
	c.pop_front();
	bool ok = pop_name(name, "name", c, true);
	ok = pop_fid(id, "id", c, ok);
	ok = pop_fid(parent, "parent", c, ok);
	if (!ok)
	{
		return false;
	}

	MetaDentry *d = new MetaDentry(parent, name, id);
	return (check_point_->meta_tree_.Insert(d) == 0);
}

bool DiskEntry::restore_fattr(deque <string> &c)
{
	FileType type;
	int64_t fid;
	int64_t chunkcount;
	off_t filesize = -1;
	struct timeval mtime, ctime, crtime;
	int16_t numReplicas;

	bool ok = pop_type(type, "fattr", c, true);
	ok = pop_fid(fid, "id", c, ok);
	ok = pop_fid(chunkcount, "chunkcount", c, ok);
	ok = pop_short(numReplicas, "numReplicas", c, ok);
	ok = pop_time(mtime, "mtime", c, ok);
	ok = pop_time(ctime, "ctime", c, ok);
	ok = pop_time(crtime, "crtime", c, ok);
	if (!ok)
	{
		return false;
	}
	// filesize is optional; if it isn't there, we can re-compute
	// by asking the chunkservers
	bool gotfilesize = pop_offset(filesize, "filesize", c, true);

	if (numReplicas < min_replicas_)
	{
		numReplicas = min_replicas_;
	}

	// chunkcount is an estimate; recompute it as we add chunks to the file.
	// reason for it being estimate: if a CP is in progress while the
	// metatree is updated, we have cases where the chunkcount is off by 1
	// and the checkpoint contains the newly added chunk.
	MetaFattr *f = new MetaFattr(type, fid, mtime, ctime, crtime, 0, numReplicas);

	if (gotfilesize)
	{
		f->filesize = filesize;
	}

	if (type == BLADE_DIR)
	{
//		UpdateNumDirs(1);
	}
	else 
	{
//		UpdateNumFiles(1);
//		UpdateNumChunks(chunkcount);
	}

	return (check_point_->meta_tree_.Insert(f) == 0);
}

bool DiskEntry::restore_chunkinfo(deque <string> &c)
{
	int64_t fid;
	int64_t cid;
	int64_t offset;
	int64_t chunkVersion;

	c.pop_front();
	bool ok = pop_fid(fid, "fid", c, true);
	ok = pop_fid(cid, "chunkid", c, ok);
	ok = pop_offset(offset, "offset", c, ok);
	ok = pop_fid(chunkVersion, "chunkVersion", c, ok);
	if (!ok)
	{
		return false;
	}

	MetaChunkInfo *ch = new MetaChunkInfo(fid, offset, cid, (int32_t)chunkVersion);
	if (check_point_->meta_tree_.Insert(ch) == 0) 
	{
		MetaFattr *fa = (check_point_->meta_tree_).GetFattr(fid);

		assert(fa != NULL);

		fa->chunkcount++;
		//gLayoutManager.AddChunkToServerMapping(cid, fid, NULL);
		return true;
	}

	return false;
}

void DiskEntry::init_map()
{
	add_parser("checkpoint", &DiskEntry::checkpoint_seq);
	add_parser("version", &DiskEntry::checkpoint_version);
	add_parser("fid", &DiskEntry::checkpoint_fid);
	add_parser("chunk_Id", &DiskEntry::checkpoint_chunkId);
	add_parser("time", &DiskEntry::checkpoint_time);
	add_parser("log", &DiskEntry::checkpoint_log);
	add_parser("chunk_version_inc", &DiskEntry::restore_chunkVersionInc);
	add_parser("dentry", &DiskEntry::restore_dentry);
	add_parser("fattr", &DiskEntry::restore_fattr);
	add_parser("chunkinfo", &DiskEntry::restore_chunkinfo);
}

}//end of namespace nameserver
}//end of namespace bladestore
