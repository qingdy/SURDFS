/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-4-18
 *decription : COW META TREE
 *
 */
#ifndef BLADESTORE_BTREE_META_TREE_H
#define BLADESTORE_BTREE_META_TREE_H
#include <vector>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

#include "btree_meta.h"
#include "key_btree.h"
#include "base.h"
#include "meta.h"
#include "util.h"
#include "blade_common_define.h"
#include "layout_manager.h"

using namespace std;
using namespace bladestore::common;
using namespace bladestore::nameserver;

namespace bladestore
{
namespace btree
{

class MetaTree
{
public:
	typedef KeyBtree<Key, Meta*> MetaBtree;

public:
	MetaTree(BladeLayoutManager & layout_manager);
	~MetaTree();

	int new_tree()          //create a directory namespace
    {   
        int64_t dummy = 0;
        return Mkdir(ROOTFID, "/", &dummy);
    } 

	int Insert(Meta * m);

	int Del(Meta *m);			//remove data item

	bool block_exist(int64_t fid, int64_t block_id);

	bool Legalname(const string name);

	bool Absolute(const string path);

	int Link(int64_t dir, const string fname, FileType type, int64_t myID,int16_t numReplicas);

	int Create(int64_t dir, const string &fname, int64_t *newFid,int16_t numReplicas, bool exclusive);

	void Unlink(int64_t dir, const string fname, MetaFattr *fa, bool save_fa);

	int Remove(int64_t dir, const string &fname, const string &pathname, off_t *filesize = NULL, bool is_master = true);

	int Mkdir(int64_t dir, const string &dname, int64_t *newFid);

	bool Emptydir(int64_t dir);

	int Rmdir(int64_t dir, const string &dname, const string &pathname);

	MetaFattr * GetFattr(int64_t fid);

	MetaDentry * GetDentry(int64_t dir, const string &fname);

	MetaDentry * GetDentry(int64_t fid);

	void ListPaths(ofstream &ofs);

	void ListPaths(ofstream &ofs, string parent, int64_t dir);

	void RecomputeDirSize();

	void Travel();

	off_t RecomputeDirSize(int64_t dir);

	int ChangeDirReplication(MetaFattr *dirattr, int16_t numReplicas);

	string GetPathname(int64_t fid);

	MetaFattr * Lookup(int64_t dir, const string &fname);

	MetaFattr * LookupPath(int64_t rootdir, const string &path);

	int Readdir(int64_t dir, vector <MetaDentry *> &result);

	int Getalloc(int64_t file, vector <MetaChunkInfo *> &result);

	int Getalloc(int64_t file, int64_t offset, MetaChunkInfo **c);

	int Rename(int64_t dir, const string &oldname, string &newname, const string &oldpath, bool once);

	int GetChunkVersion(int64_t file, int64_t chunkId, int64_t *chunkVersion);

	int ChangePathReplication(int64_t file, int16_t numReplicas);

	int MoveToDumpster(int64_t dir, const string &fname);

    int get_block_fid(int64_t block_id, int64_t *fid);

    int delete_block(int64_t block_id);

    void delete_block(int64_t fid, int64_t offset, int64_t block_id);

	void CleanupDumpster();

	/*
	 * \brief Allocate a unique chunk identifier
	 * \param[in] file	The id of the file for which need to allocate space
	 * \param[in] offset	The offset in the file at which space should be allocated
	 * \param[out] chunkId  The chunkId that is allocated
	 * \param[out] numReplicas The # of replicas to be created for the chunk.  This
	 *  parameter is inhreited from the file's attributes.
	 * \retval 0 on success; -errno on failure
	 */
	int AllocateChunkId(int64_t file, int64_t offset, int64_t * chunkId, int64_t * version, int16_t * numReplicas);

	/*
	 * \brief Assign a chunk identifier to a file.  Update the metatree
	 * to reflect the assignment.
	 * \param[in] file	The id of the file for which need to allocate space
	 * \param[in] offset	The offset in the file at which space should be allocated
	 * \param[in] chunkId   The chunkId that is assigned to file/offset
	 * \retval 0 on success; -errno on failure
	 */
	int AssignChunkId(int64_t file, int64_t offset, int64_t chunkId, int64_t version);
	
	/*
	 * \brief Truncate a file to the specified file offset.  Due
	 * to truncation, chunks past the desired offset will be
	 * deleted.  When there are holes in the file or if the file
	 * is extended past the last chunk, a truncation can cause
	 * a chunk allocation to occur.  This chunk will be used to
	 * track the file's size.
	 * \param[in] file	The id of the file being truncated
	 * \param[in] offset	The offset to which the file should be
	 *			truncate to
         * \param[out] allocOffset	The offset at which an allocation
	 * 				should be done
	 * \retval 0 on success; -errno on failure; 1 if an allocation
	 * is needed
	 */
	int Truncate(int64_t file, int64_t  offset, int64_t *allocOffset);

	void UpdateSpaceUsageForPath(const string &path, off_t nbytes);

	bool is_descendant(int64_t src, int64_t dst);

	int ChangeFileReplication(MetaFattr *fa, int16_t numReplicas);

	//更改文件的副本数量，在更改的过程中设计到对block的副本数量的修改
	int chang_file_replication(int64_t file_id, int16_t num_replicas);

	MetaBtree * get_meta_btree()
	{
		return &meta_btree_;
	}

	void MakeDumpsterDir();

	void EmptyDumpsterDir();

public:
	static const int64_t MINFID;
	static const int64_t MAXFID;
	static const int64_t MIN_CHUNK_OFFSET;
	static const int64_t MAX_CHUNK_OFFSET;

private:
	static const string DUMPSTERDIR;

	MetaBtree meta_btree_;

	BladeLayoutManager & layout_manager_;
};

class ChunkIdMatch
{
	int64_t myid;

public:
	ChunkIdMatch(int64_t c) : myid(c) 
	{ 

	}

	bool operator() (MetaChunkInfo *m) 
	{
		return m->chunkId == myid;
	}
};

//static bool ChunkInfo_compare(MetaChunkInfo *first, MetaChunkInfo *second)
//{
//	return first->offset < second->offset;
//}

class RemoveDumpsterEntry 
{
	int64_t dir;
	MetaTree metatree_;

public:
	RemoveDumpsterEntry(int64_t d, MetaTree metatree) : dir(d), metatree_(metatree) { }
	void operator() (MetaDentry *e) 
	{
		metatree_.Remove(dir, e->getName(), "");
	}
};

}//end of namespace btree
}//end of namespace bladestore
#endif
