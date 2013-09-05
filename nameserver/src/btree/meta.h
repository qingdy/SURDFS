/*
 *version  :  1.0
 *author   :  chen yunyun
 *date     :  2012-4-18
 *
 */
#ifndef BLADESTORE_BTREE_META_H 
#define BLADESTORE_BTREE_META_H

#include <iostream>
#include <fstream>
#include <string>
#include <cassert>
#include <stdint.h>
#include "blade_common_define.h"
#include "base.h"

extern "C" 
{
#include <sys/types.h>
#include <sys/time.h>
}

using std::string;
using std::ofstream;
using namespace bladestore::common;

namespace bladestore
{
namespace btree
{

class MetaTree;

/*
 *brief fixed-length unique id generator
 *
 *Unique fixed-length id generator for each file and directory and chunk
 *in the system.
 */

class UniqueID 
{
public:
	int64_t n;		//id of this object
	int64_t seed;		//seed for generator
	/*
	 *brief generate a new id
	 */
	int64_t genid() 
	{ 
		return ++seed; 
	}
	
	int64_t getseed() 
	{ 
		return seed; 
	}

	void setseed(int64_t s) 
	{
		 seed = s; 
	}

	UniqueID(int64_t id, int64_t s): n(id), seed(s) 
	{

	}
	
	UniqueID(): n(0), seed(0) 
	{ 

	}

	int64_t id() const 
	{ 
		return n; 
	}	//return id
};

/*!
 * \brief base class for data objects (leaf nodes)
 */

class Meta: public MetaNode 
{
public:
	int64_t fid;		//id of this item's owner
	Meta(MetaType t, int64_t id): MetaNode(t), fid(id) 
	{ 
	
	}

	virtual ~Meta() 
	{ 
	}

	int64_t id() const 
	{ 
		return fid; 
	}

	bool skip() const 
	{
		 return testflag(META_SKIP); 
	}
	
	void markskip() 
	{ 
		setflag(META_SKIP); 
	}
	
	void clearskip() 
	{
		 clearflag(META_SKIP); 
	}
	
	int checkpoint(ofstream &file) const
	{
		file << show() << '\n';
		return file.fail() ? -EIO : 0;
	}

	//Compare for equality
	virtual bool match(Meta *test) 
	{
		 return id() == test->id(); 
	}
};


/*!
 * \brief Directory entry, mapping a file name to a file id
 */
class MetaDentry: public Meta 
{
	int64_t dir;	//id of parent directory
	string name;	//name of this entry

public:
	MetaDentry(int64_t parent, string fname, int64_t myID): Meta(BLADE_DENTRY, myID), dir(parent), name(fname) 
	{
 
	}
	
	MetaDentry(const MetaDentry *other) : Meta(BLADE_DENTRY, other->id()), dir(other->dir), name(other->name) 
	{
	 
	}
	
	~MetaDentry()
	{
		
	}

	const Key key() const 
	{ 
		return Key(BLADE_DENTRY, dir, fid); 
	}

	const string show() const;

	//accessor that returns the name of this Dentry
	const string getName() const 
	{ 
		return name; 
	}

	int64_t getDir() const 
	{
		return dir; 
	}

	const int compareName(const string test) const 
	{
		return name.compare(test);
	}

	bool match(Meta *test);

};

/*!
 * \brief Function object to search for file name in directory
 */
class DirMatch 
{
	const string searchname;
public:
	DirMatch(const string s): searchname(s) 
	{ 

	}

	bool operator () (const MetaDentry *d)
	{
		return (d->compareName(searchname) == 0);
	}
};

/*!
 * \brief File or directory attributes.
 *
 */
class MetaFattr: public Meta 
{
public:
	FileType type;		//file or directory
	int16_t numReplicas;    //Desired number of replicas for a file
	struct timeval mtime;	//modification time
	struct timeval ctime;	//attribute change time
	struct timeval crtime;	//creation time
	int64_t chunkcount;	//number of constituent chunks
	//size of file: is only a hint; if we don't have the size, the client will
	//compute the size whenever needed.  
	off_t filesize;		

	MetaFattr(FileType t, int64_t id, int16_t n): Meta(BLADE_FATTR, id), type(t), numReplicas(n), chunkcount(0), filesize(0)
	{
		int s = gettimeofday(&crtime, NULL);
		assert(s == 0);
		mtime = ctime = crtime;
		if (type == BLADE_DIR)
			filesize = 0;
	}

	~MetaFattr()
	{

	}

	MetaFattr(FileType t, int64_t id, struct timeval mt,
		struct timeval ct, struct timeval crt,
		long long c, int16_t n): Meta(BLADE_FATTR, id),
				 type(t), numReplicas(n), mtime(mt), ctime(ct),
				 crtime(crt), chunkcount(c), filesize(0) 
	{ 
		if (type == BLADE_DIR)
			filesize = 0;
	}

	MetaFattr(): Meta(BLADE_FATTR, 0), type(BLADE_NONE) 
	{ 

	}

	const Key key() const 
	{ 
		return Key(BLADE_FATTR, id()); 
	}

	const string show() const;

	void setReplication(int16_t val) 
	{
		numReplicas = val;
	}

    void set_file_size(off_t file_size)
    {
        filesize = file_size;
    }
};

/*
 * brief downcast from base to derived metadata types
 */
template <typename T> 
T * refine(Meta *m)
{
	return static_cast <T *>(m);
}

/*
 * brief chunk information for a given file offset
 */
class MetaChunkInfo: public Meta 
{
public:
	int64_t offset;		// offset of chunk within file
	int64_t chunkId;		// unique chunk identifier
	int32_t chunkVersion;		// version # for this chunk
	int16_t num_replicas_;

	MetaChunkInfo(int64_t file, int64_t off, int64_t id, int32_t v, int16_t num_replicas = 3): Meta(BLADE_CHUNKINFO, file), offset(off), chunkId(id), chunkVersion(v), num_replicas_(num_replicas)
	{

	}
	
	virtual ~MetaChunkInfo()
	{
	
	}

	const Key key() const 
	{ 
		return Key(BLADE_CHUNKINFO, id(), offset); 
	}

	const string show() const;
};

extern UniqueID fileID;   //Instance for generating unique fid
extern UniqueID chunkID;  //Instance for generating unique chunkId

//This number is the value used for incrementing chunk version
//numbers.  This number is incremented whenever metaserver restarts
//after a crash as well as whenever an allocation fails because of
//a replica failure.
extern int64_t chunkVersionInc;

// return a string representation of the timeval. the string is written out
// to logs/checkpoints
string showtime(struct timeval t);

}//end of namespace btree
}//end of namespace bladestore

#endif 
