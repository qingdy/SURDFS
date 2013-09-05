#include <algorithm>
#include <functional>

#include "btree_meta.h"
//#include "log.h"

using namespace bladestore::common;
using namespace pandora;

namespace bladestore
{
namespace btree
{

//some const and static variables
const string MetaTree::DUMPSTERDIR("dumpster");
const int64_t MetaTree::MINFID = -2;
const int64_t  MetaTree::MAXFID = 9223372036854775807L; //INT64_MAX
const int64_t MetaTree::MIN_CHUNK_OFFSET = -2;
const int64_t MetaTree::MAX_CHUNK_OFFSET = 9223372036854775807L;//INT64_MAX

//construction function
MetaTree::MetaTree(BladeLayoutManager & layout_manager) : meta_btree_(sizeof(Key)), layout_manager_(layout_manager)
{
    new_tree();
}

//deconstruct function
MetaTree::~MetaTree()
{

}

/**
  *print all the leaf nodes  from left to right(maybe just for test)
  **/
void MetaTree::Travel()
{
    BtreeReadHandle handle;
    Meta* value;
    MetaDentry * metaDentry;
    int32_t ret;
    const Key fkey(BLADE_MIN, 0, 0);
    const Key tkey(BLADE_MAX, 0, 0);
	ret = meta_btree_.get_read_handle(handle);
	meta_btree_.set_key_range(handle, &fkey, 0, &tkey, 0);

	while (ERROR_CODE_OK == meta_btree_.get_next(handle, value))
	{
		LOGV(LL_DEBUG, "TRAVEL ID IS : %ld", value->id());
	}
}
/*!
 * \brief Make a dumpster directory into which we can rename busy
 * files.  When the file is non-busy, it is nuked from the dumpster.
 */
void MetaTree::MakeDumpsterDir()
{
    int64_t dummy = 0;
    Mkdir(ROOTFID, DUMPSTERDIR, &dummy);
}

int MetaTree::Insert(Meta * meta)
{
    BtreeWriteHandle handle;
    int ret;
    ret = meta_btree_.get_write_handle(handle);
    assert(ERROR_CODE_OK == ret);
    ret = meta_btree_.Put(handle,meta->key(),meta);
    return 0;    
}

int MetaTree::Del(Meta * meta)
{
    return 0;
}

/*!
 * \brief Cleanup the dumpster directory on startup.  Also, if
 * the dumpster doesn't exist, make one.
 */
void MetaTree::EmptyDumpsterDir()
{
    MakeDumpsterDir();
    CleanupDumpster();
}

/*!
 * \brief check file name for legality
 *
 * Legal means nonempty and not containing any slashes.
 *
 * \param[in]    name to check
 * \return    true if name is legal
 */
bool MetaTree::Legalname(const string name)
{
    return (!name.empty() && name.find('/', 0) == string::npos);
}

/*!
 * \brief see whether path is absolute
 */
bool MetaTree::Absolute(const string path)
{
    return (path[0] == '/');
}

/*!
 * \brief common code for create and mkdir
 * \param[in] dir    fid of parent directory
 * \param[in] fname    name of object to be created
 * \param[in] type    file or directory
 * \param[in] myID    fid of new object
 * \param[in] numReplicas desired degree of replication for file
 *
 * Create a directory entry and file attributes for the new object.
 * But don't create attributes for "." and ".." links, since these
 * share the directory's attributes.
 */
int MetaTree::Link(int64_t dir, const string fname, FileType type, int64_t myID, int16_t numReplicas)
{
    BtreeWriteHandle handle;
    int ret;
    assert(Legalname(fname));
    MetaDentry *dentry = new MetaDentry(dir, fname, myID);
    MetaFattr *fattr = new MetaFattr(type, dentry->id(), numReplicas);
    ret = meta_btree_.get_write_handle(handle);
    assert(ERROR_CODE_OK == ret);
    meta_btree_.Put(handle,dentry->key(),dentry);
    meta_btree_.Put(handle,fattr->key(),fattr);
    handle.End();
//    insert(dentry);
//    insert(fattr);
    return 0;
}

/*!
 * \brief create a new file
 * \param[in] dir    file id of the parent directory
 * \param[in] fname    file name
 * \param[out] newFid    id of new file
 * \param[in] numReplicas desired degree of replication for file
 * \param[in] exclusive  model the O_EXCL flag of open()
 *
 * \return        status code (zero on success)
 */

//创建指定的文件， 可以以overwrite方式创建

int MetaTree::Create(int64_t dir, const string &fname, int64_t *newFid, int16_t numReplicas, bool exclusive)
{
    if (!Legalname(fname)) 
    {
        return -EINVAL;
    }

    if (numReplicas <= 0) 
    {
        return -EINVAL;
    }

    MetaFattr *fa = Lookup(dir, fname);
    if (fa != NULL) 
    {
        if (fa->type != BLADE_FILE)
        {
            return -EISDIR;
        }

        // Model O_EXECL behavior in create: if the file exists
        // and exclusive is specified, fail the create.
        if (exclusive)
        {
            return -EEXIST;
        }

        int status = Remove(dir, fname, "");
        if (-EBUSY == status) 
        {
            return status;
        }
        assert(status == 0);
    }

    if (0 == *newFid)
    {
        *newFid = fileID.genid();
    }
    //UpdateNumFiles(1);

    return Link(dir, fname, BLADE_FILE, *newFid, numReplicas);
}

/*!
 * \brief common code for remove and rmdir
 * \param[in] dir    fid of parent directory
 * \param[in] fname    name of item to be removed
 * \param[in] fa    attributes for removed item
 * \pamam[in] save_fa    don't delete attributes if true
 *
 * save_fa prevents multiple deletions when removing
 * the "." and ".." links to a directory.
 */
void MetaTree::Unlink(int64_t dir, const string fname, MetaFattr *fa, bool save_fa)
{
    BtreeWriteHandle write_handle;
    const Key dentry_key(BLADE_DENTRY, dir, fa->id());
    const Key fattr_key(BLADE_FATTR, fa->id());
    int ret = meta_btree_.get_write_handle(write_handle);
    assert(ERROR_CODE_OK == ret);

    meta_btree_.Remove(write_handle, dentry_key);
    meta_btree_.Remove(write_handle, fattr_key);
    write_handle.End();
}

void MetaTree::delete_block(int64_t fid, int64_t offset, int64_t block_id)
{
    BtreeWriteHandle write_handle;
    const Key key(BLADE_CHUNKINFO, fid, offset);
    int ret = meta_btree_.get_write_handle(write_handle);
    assert(ERROR_CODE_OK == ret);

    int ret1 = meta_btree_.Remove(write_handle, key);
    LOGV(LL_DEBUG, "remove ret:%d", ret1);
    write_handle.End();
}

/*
 *brief remove a file
 *param[in] dir    file id of the parent directory
 *param[in] fname    file name
 *return        status code (zero on success)
 */
int
MetaTree::Remove(int64_t dir, const string &fname, const string &pathname, off_t *filesize, bool is_master)
{
    MetaFattr *fa = Lookup(dir, fname);
    if (NULL == fa)
	{
        return -ENOENT;
	}
    if (fa->type != BLADE_FILE)
	{
        return -EISDIR;
	}

    if (fa->filesize > 0) 
    {
        if (pathname != "") 
        {
            UpdateSpaceUsageForPath(pathname, -fa->filesize);
        } 
        else 
        {
            string pn = GetPathname(dir);
            UpdateSpaceUsageForPath(pn, -fa->filesize);
        }

        if (filesize != NULL)
        {
            *filesize = fa->filesize;
        }
    }

    if (fa->chunkcount > 0) 
    {
        vector <MetaChunkInfo *> chunkinfo;
        Getalloc(fa->id(), chunkinfo);
        assert(fa->chunkcount == (long long)chunkinfo.size());
        vector <MetaChunkInfo *>::iterator chunkinfo_iter = chunkinfo.begin();
        for ( ; chunkinfo_iter != chunkinfo.end(); chunkinfo_iter++)
        {
            const Key key(BLADE_CHUNKINFO, fa->id(), (*chunkinfo_iter)->offset);
            //不带handle的remove会在内部生产handle,在方法结束时销毁
            meta_btree_.Remove(key);
            layout_manager_.remove_block_collect((*chunkinfo_iter)->chunkId, is_master);    
        }
    }

    //UpdateNumFiles(-1);

    Unlink(dir, fname, fa, false);
    return 0;
}

/*
 *brief create a new directory
 *param[in] dir    file id of the parent directory
 *param[in] dname    name of new directory
 *param[out] newFid    id of new directory
 *return        status code (zero on success)
 */
int MetaTree::Mkdir(int64_t dir, const string &dname, int64_t *newFid)
{
    int32_t ret;
    BtreeWriteHandle handle;
    if (!Legalname(dname) && dir != ROOTFID && dname != "/")
    {
        return -EINVAL;
    }

    if (Lookup(dir, dname) != NULL)
    {
        return -EEXIST;
    }

    int64_t myID = *newFid;
    if (0 == myID)
    {
        myID = (dname == "/") ? dir : fileID.genid();
    }

    MetaDentry *dentry = new MetaDentry(dir, dname, myID);
    MetaFattr *fattr = new MetaFattr(BLADE_DIR, dentry->id(), 1);
    ret = meta_btree_.get_write_handle(handle);
    assert(ERROR_CODE_OK == ret);
    meta_btree_.Put(handle,dentry->key(),dentry);
    meta_btree_.Put(handle,fattr->key(),fattr);
    handle.End();


    *newFid = myID;
    return 0;
}

/*
 *brief check whether a directory is empty
 *param[in] dir    file ID of the directory
 */
bool MetaTree::Emptydir(int64_t dir)
{
    vector <MetaDentry *> v;
    Readdir(dir, v);

    return (v.size() == 0);
}

/*
 *brief remove a directory
 *param[in] dir    file id of the parent directory
 *param[in] dname    name of directory
 *param[in] pathname  fully qualified path to dname
 *return        status code (zero on success)
 */
int MetaTree::Rmdir(int64_t dir, const string &dname, const string &pathname)
{
    MetaFattr *fa = Lookup(dir, dname);

    if ((ROOTFID == dir) && (DUMPSTERDIR == dname)) 
    {
        return -EPERM;
    }

    if (NULL == fa)
    {
        return -ENOENT;
    }
    if (fa->type != BLADE_DIR)
    {    
        return -ENOTDIR;
    }
    if ("." == dname || ".." == dname)
    {
        return -EPERM;
    }

    int64_t myID = fa->id();

    if (!Emptydir(myID))
    {
        return -ENOTEMPTY;
    }

    if (fa->filesize > 0) 
    {
        if (pathname != "") 
        {
            UpdateSpaceUsageForPath(pathname, -fa->filesize);
        } 
        else 
        {
            string pn = GetPathname(dir);
            UpdateSpaceUsageForPath(pn, -fa->filesize);
        }
    }

    //UpdateNumDirs(-1);

    Unlink(myID, ".", fa, true);
    Unlink(myID, "..", fa, true);
    Unlink(dir, dname, fa, false);
    return 0;
}

/*
 *brief return attributes for the specified object
 *param[in] fid    the object's file id
 *return        pointer to the attributes
 */
MetaFattr * MetaTree::GetFattr(int64_t fid)
{
    BtreeReadHandle handle;
    Meta* value = NULL;
    int ret;
    const Key fkey(BLADE_FATTR, fid);
    ret = meta_btree_.get_read_handle(handle);    
    ret = meta_btree_.Get(handle, fkey, value);
//    assert(ERROR_CODE_OK == ret);
//    Node *l = findLeaf(fkey);
//    return (l == NULL) ? NULL : l->extractMeta<MetaFattr>(fkey);
    return (MetaFattr *)value;
}

MetaDentry * MetaTree::GetDentry(int64_t dir, const string &fname)
{
    vector <MetaDentry *> v;

    if (Readdir(dir, v) != 0)
    {
        return NULL;
    }

    vector <MetaDentry *>::iterator d;
    d = find_if(v.begin(), v.end(), DirMatch(fname));
    return (d == v.end()) ? NULL : *d;
}


//根据fid获取dentry信息

MetaDentry * MetaTree::GetDentry(int64_t fid)
{
    BtreeReadHandle read_handle;
    Meta * value;
    MetaDentry * meta_dentry = NULL;
    const Key fkey(BLADE_DENTRY, MINFID, MINFID);
    const Key tkey(BLADE_DENTRY, MAXFID, MAXFID);
    meta_btree_.get_read_handle(read_handle);
    meta_btree_.set_key_range(read_handle, &fkey, 0, &tkey, 0);

    while (ERROR_CODE_OK == meta_btree_.get_next(read_handle, value))
    {
        meta_dentry = static_cast<MetaDentry *>(value);    

        assert(NULL != meta_dentry);

        if(meta_dentry->fid == fid)
        {
            break;
        }
        meta_dentry = NULL;
    }
    
    return meta_dentry;
}


/*
 * Do a depth first dir listing of the tree.  This can be useful for debugging
 * purposes.
 */

void MetaTree::ListPaths(ofstream &ofs)
{
    ListPaths(ofs, "/", ROOTFID);
    ofs.flush();
    ofs << '\n';
}

void MetaTree::ListPaths(ofstream &ofs, string parent, int64_t dir)
{
    vector<MetaDentry *> entries;
    MetaFattr *dirattr = GetFattr(dir);
    struct tm tm;
    char datebuf[256];

    if (NULL == dirattr)
	{
        return;
	}

    Readdir(dir, entries);
    for (uint32_t i = 0; i < entries.size(); i++) 
    {
        string entryname = entries[i]->getName();
        if ((entryname == ".") || (entryname == "..") || (entries[i]->id() == dir))
        {
            continue;
        }

        MetaFattr *fa = GetFattr(entries[i]->id());

        if (NULL == fa)
        {
            continue;
        }

        if (fa->type == BLADE_DIR) 
        {
            string subdir = parent + entryname;
            gmtime_r(&(fa->mtime.tv_sec), &tm);
            asctime_r(&tm, datebuf);
            // ofs << subdir << ' ' << fa->mtime.tv_sec << " " << fa->mtime.tv_usec << '\n';
            ofs << subdir << ' ' << datebuf;
            ListPaths(ofs, subdir + "/", fa->id());
            continue;
        }
        gmtime_r(&(fa->mtime.tv_sec), &tm);
        asctime_r(&tm, datebuf);
        // ofs << parent << entryname << ' ' << fa->mtime.tv_sec << " " << fa->mtime.tv_usec << '\n';
        ofs << parent << entryname << ' ' << datebuf;
        // ofs << parent << entryname << '\n';
    }
}

/*
 * For fast "du", we store the size of a directory tree in the Fattr for that
 * tree id.  This method should be called whenever the size values need to be
 * recomputed for accuracy.  This is an expensive operation: we have to traverse
 * from root to each leaf in the tree.
 */
void MetaTree::RecomputeDirSize()
{
    RecomputeDirSize(ROOTFID);
}

/*
 * A simple depth first traversal of the directory tree starting at the root
 * @param[in] dir  The directory we are processing
 */
off_t MetaTree::RecomputeDirSize(int64_t dir)
{
    vector<MetaDentry *> entries;
    MetaFattr *dirattr = GetFattr(dir);

    if (dirattr == NULL)
	{
        return 0;
	}

    Readdir(dir, entries);
    dirattr->filesize = 0;
    for (uint32_t i = 0; i < entries.size(); i++) 
    {
        string entryname = entries[i]->getName();

        if ((entryname == ".") || (entryname == "..") || (entries[i]->id() == dir))
        {
            continue;
        }

        MetaFattr *fa = GetFattr(entries[i]->id());

        if (NULL == fa)
        {
            continue;
        }

        if (fa->type == BLADE_DIR) 
        {
            // Do a depth first traversal
            dirattr->filesize += RecomputeDirSize(fa->id());
            continue;
        }

        if (fa->filesize > 0)
        {
            dirattr->filesize += fa->filesize;
        }
    }

    return dirattr->filesize;
}

/*
 * Given a dir, do a depth first traversal updating the replication count for
 * all files in the dir. tree to the specified value.
 * @param[in] dirattr  The directory we are processing
 */
int MetaTree::ChangeDirReplication(MetaFattr *dirattr, int16_t numReplicas) 
{
    vector<MetaDentry *> entries;

    if ((NULL == dirattr) || (dirattr->type != BLADE_DIR))
	{
        return -ENOTDIR;
	}

    int64_t dir = dirattr->id();

    Readdir(dir, entries);
    for (uint32_t i = 0; i < entries.size(); i++) 
    {
        string entryname = entries[i]->getName();
        if ((entryname == ".") || (entryname == "..") || (entries[i]->id() == dir))
        {
            continue;
        }

        MetaFattr *fa = GetFattr(entries[i]->id());
        if (NULL == fa)
        {
            continue;
        }

        if (fa->type == BLADE_DIR) 
        {
            // Do a depth first traversal
            ChangeDirReplication(fa, numReplicas);
            continue;
        }
        ChangeFileReplication(fa, numReplicas);
    }
    return 0;
}

/*
 * Given a file-id, returns its fully qualified pathname.  This involves
 * recursively traversing the metatree until the root directory.
 */

//根据fid返回它的绝对路径, @notice  比较耗时
string MetaTree::GetPathname(int64_t fid)
{
    MetaDentry *d = NULL;
    string s = "";

    while (1) 
    {
        d = GetDentry(fid);
        if (NULL == d)
        {
            return "";
        }

        if ("" == s)
        {
            s = d->getName();
        }
        else if (d->id() == ROOTFID) 
        {
            return "/" + s;
        }
        else
        {
            s = d->getName() + "/" + s;
        }
        fid = d->getDir();
    }
    return s;
}

/*!
 * \brief look up a file name and return its attributes
 * \param[in] dir    file id of the parent directory
 * \param[in] fname    file name that we are looking up
 * \return        file attributes or NULL if not found
 */

MetaFattr * MetaTree::Lookup(int64_t dir, const string &fname)
{
    MetaDentry *d = GetDentry(dir, fname);
    if (NULL == d)
    {
        return NULL;
    }

    MetaFattr *fa = GetFattr(d->id());
    assert(fa != NULL);
    return fa;
}

/*
 *brief repeatedly apply Tree::lookup to an entire path
 *param[in] rootdir    file id of starting directory
 *param[in] path    the path to look up
 *return        attributes of the last component (or NULL)
 */

//@important 对用户创建的path进行查找

MetaFattr * MetaTree::LookupPath(int64_t rootdir, const string &path)
{
    string component;
    bool isabs = Absolute(path);
    int64_t dir = (rootdir == 0 || isabs) ? ROOTFID : rootdir;
    string::size_type cstart = isabs ? 1 : 0;
    string::size_type slash = path.find('/', cstart);

    if (path.size() == cstart)
    {
        return Lookup(dir, "/");
    }

    while (slash != string::npos) 
    {
        component.assign(path, cstart, slash - cstart);
        MetaFattr *fa = Lookup(dir, component);
        if (NULL == fa)
        {
            return NULL;
        }
        dir = fa->id();
        cstart = slash + 1;
        slash = path.find('/', cstart);
    }

    component.assign(path, cstart, path.size() - cstart);
    return Lookup(dir, component);
}

/*
 * At each level of the directory tree, we'd like to record the space used by
 * that subtree.  Then, on a stat of directory, we can provide "du" results for
 * the subtree.
 * To update space usage, start at the root and work down till the parent
 * directory where the file lives and update space used at each level by nbytes.
 */
void MetaTree::UpdateSpaceUsageForPath(const string &path, int64_t nbytes)
{
    string dumpster = "/" + DUMPSTERDIR + "/";
    if ((nbytes == 0) || (path == "") || (path.compare(0, dumpster.size(), dumpster) == 0)) 
    {
        // either we dont' have a path or the path is in the dumpster
        // and so, we don't update space used by the dumpster
        return;
    }

    string component;
    int64_t dir = ROOTFID;
    string::size_type cstart = 1;
    string::size_type slash = path.find('/', cstart);
    MetaFattr *fa;

    fa = Lookup(dir, "/");
    fa->filesize += nbytes;
    if (fa->filesize < 0)
    {
        fa->filesize = 0;
    }

    if (path.size() == cstart) 
    {
        return;
    }

    while (slash != string::npos) 
    {
        component.assign(path, cstart, slash - cstart);
        fa = Lookup(dir, component);
        if (fa == NULL)
        {
            return;
        }

        fa->filesize += nbytes;
        if (fa->filesize < 0)
        {
            // sanity
            fa->filesize = 0;
        }
        dir = fa->id();
        cstart = slash + 1;
        slash = path.find('/', cstart);
    }
}

/*
 *brief read the contents of a directory
 *param[in] dir    file id of directory
 *param[out] v    vector of directory entries
 *return        status code
 */
int MetaTree::Readdir(int64_t dir, vector <MetaDentry *> &v)
{
    BtreeReadHandle handle;
    Meta* value;
    MetaDentry * metaDentry;
    int32_t ret;
    const Key fkey(BLADE_DENTRY, dir, MINFID);
    const Key tkey(BLADE_DENTRY, dir, MAXFID);
	ret = meta_btree_.get_read_handle(handle);
	meta_btree_.set_key_range(handle, &fkey, 0, &tkey, 0);
	while(ERROR_CODE_OK == meta_btree_.get_next(handle, value))
	{
		metaDentry = dynamic_cast<MetaDentry *>(value);    
		assert(NULL != metaDentry);
		if(metaDentry != NULL)
		{
			v.push_back(metaDentry);
		}
	}
    return 0;
}

/*
 *brief return a file's chunk information (if any)
 *param[in] file    file id for the file
 *param[out] v    vector of MetaChunkInfo results
 *return        status code
 */
int MetaTree::Getalloc(int64_t file, vector <MetaChunkInfo *> &v)
{
//    const Key ckey(BLADE_CHUNKINFO, file, Key::MATCH_ANY);
//    Node *l = findLeaf(ckey);
//    if (l != NULL)
//        extractAll(l, ckey, v);
    BtreeReadHandle handle;
    Meta * value;
    MetaChunkInfo* chunkValue;
    int32_t ret;
    const Key fkey(BLADE_CHUNKINFO, file, MIN_CHUNK_OFFSET);
    const Key tkey(BLADE_CHUNKINFO, file, MAX_CHUNK_OFFSET);
    ret = meta_btree_.get_read_handle(handle);
    meta_btree_.set_key_range(handle, &fkey, 0, &tkey, 0);
      while(ERROR_CODE_OK == meta_btree_.get_next(handle, value))
      {
        chunkValue = dynamic_cast<MetaChunkInfo*>(value);
        assert(chunkValue != NULL);
        if(chunkValue != NULL)
        {
            v.push_back(chunkValue);
        }
      }

    //handle.end();
    return 0;
}

/*
 *brief return the specific chunk information from a file
 *param[in] file    file id for the file
 *param[in] offset    offset in the file
 *param[out] c    MetaChunkInfo
 *return        status code
 */
int MetaTree::Getalloc(int64_t file, int64_t offset, MetaChunkInfo **c)
{
    // Allocation information is stored for offset's in the file that
    // correspond to chunk boundaries.
//    int64_t boundary = chunk_start_offset(offset);
//    const Key ckey(BLADE_CHUNKINFO, file, boundary);
//    Node *l = findLeaf(ckey);
//    if (l == NULL)
//        return -ENOENT;
//    *c = l->extractMeta<MetaChunkInfo>(ckey);
    BtreeReadHandle handle;
    Meta * value;
    MetaChunkInfo* chunkValue;
    int32_t ret;
    ret = meta_btree_.get_read_handle(handle);
    int64_t boundary = chunk_start_offset(offset);
    const Key ckey(BLADE_CHUNKINFO, file, boundary);
    meta_btree_.Get(handle, ckey, value);
    if (value == NULL)
    {
        chunkValue = NULL;
        *c = chunkValue;
        return -ENOENT;
    }
    else
    {
        chunkValue = dynamic_cast<MetaChunkInfo *>(value);
        assert(chunkValue != NULL);
        if(chunkValue != NULL)
        {
            *c = chunkValue;
        }
        else
        {
            *c = NULL;    
        }
    }
    return 0;
}


/*
 *brief Retrieve the chunk-version for a file/chunkId
 *param[in] file    file id for the file
 *param[in] chunkId    chunkId of interest
 *param[out] chunkVersion  the version # of chunkId if such
 * a chunkId exists; 0 otherwise
 *return     status code
*/
int MetaTree::GetChunkVersion(int64_t file, int64_t chunkId, int64_t *chunkVersion)
{
    vector <MetaChunkInfo *> v;
    vector <MetaChunkInfo *>::iterator i;
    MetaChunkInfo *m;

    *chunkVersion = 0;
    Getalloc(file, v);
    i = find_if(v.begin(), v.end(), ChunkIdMatch(chunkId));
    if (i == v.end())
        return -ENOENT;
    m = *i;
    *chunkVersion = m->chunkVersion;
    return 0;
}

/*
 *brief allocate a chunk id for a file.
 *param[in] file    file id for the file
 *param[in] offset    offset in the file
 *param[out] chunkId    chunkId that is (pre) allocated.  Allocation
 * is a two-step process: we grab a chunkId and then try to place the
 * chunk on a chunkserver; only when placement succeeds can the
 * chunkId be assigned to the file.  This function does the part of
 * grabbing the chunkId.
 *param[out] chunkVersion  The version # assigned to the chunk
 *return        status code
 */

int MetaTree::AllocateChunkId(int64_t file, int64_t offset, int64_t *chunkId, int64_t *chunkVersion, int16_t *numReplicas)
{
    MetaFattr *fa = GetFattr(file);
    if (fa == NULL)
      {
        return -ENOENT;
      }

    if (numReplicas != NULL) 
      {
        assert(fa->numReplicas != 0);
        *numReplicas = fa->numReplicas;
    }

    // Allocation information is stored for offset's in the file that
    // correspond to chunk boundaries.  This simplifies finding
    // allocation information as we need to look for chunk
    // starting locations only.
    assert((offset % BLADE_BLOCK_SIZE) == 0);

    BtreeReadHandle handle;
    Meta * value = NULL;
    MetaChunkInfo* chunkValue;
    meta_btree_.get_read_handle(handle);
    int64_t boundary = chunk_start_offset(offset);
    const Key ckey(BLADE_CHUNKINFO, file, boundary);
    meta_btree_.Get(handle, ckey, value);

    if (value == NULL)
    {
        *chunkId = chunkID.genid();
        *chunkVersion = chunkVersionInc;
        return 0;
    }    
    else 
    {    
        chunkValue = static_cast<MetaChunkInfo *>(value);
        if(NULL == chunkValue)
        {
            *chunkId = chunkID.genid();
            *chunkVersion = chunkVersionInc;
            return 0;
        }
        *chunkId = chunkValue->chunkId;
        *chunkVersion = chunkValue->chunkVersion;
        return -EEXIST;
    }

    return 0;
}

/*!
 * \brief update the metatree to link an allocated a chunk id with
 * its associated file.
 * \param[in] file    file id for the file
 * \param[in] offset    offset in the file
 * \param[in] chunkId    chunkId that is (pre) allocated.  Allocation
 * is a two-step process: we grab a chunkId and then try to place the
 * chunk on a chunkserver; only when placement succeeds can the
 * chunkId be assigned to the file.  This function does the part of
 * assinging the chunkId to the file.
 * \param[in] chunkVersion chunkVersion that is (pre) assigned.
 * \return        status code
 */
int MetaTree::AssignChunkId(int64_t file, int64_t offset, int64_t chunkId, int64_t chunkVersion)
{
    BtreeWriteHandle handle;
    int32_t ret; 

    MetaChunkInfo *block_info = new MetaChunkInfo(file, offset, chunkId, chunkVersion);
    ret = meta_btree_.get_write_handle(handle);
    assert(ERROR_CODE_OK == ret);
    meta_btree_.Put(handle, block_info->key(),block_info, true);
    handle.End();
    return 0;
}

int MetaTree::Truncate(int64_t file, int64_t offset, int64_t *allocOffset)
{
//    MetaFattr *fa = getFattr(file);
//
//    if (fa == NULL)
//        return -ENOENT;
//    if (fa->type != BLADE_FILE)
//        return -EISDIR;
//
//    vector <MetaChunkInfo *> chunkInfo;
//    vector <MetaChunkInfo *>::iterator m;
//
//    getalloc(fa->id(), chunkInfo);
//    assert(fa->chunkcount == (long long)chunkInfo.size());
//
//    fa->filesize = -1;
//
//    // compute the starting offset for what will be the
//    // "last" chunk for the file
//    int64_t lastChunkStartOffset = chunk_start_offset(offset);
//
//    MetaChunkInfo last(fa->id(), lastChunkStartOffset, 0, 0);
//
//    m = lower_bound(chunkInfo.begin(), chunkInfo.end(),
//            &last, ChunkInfo_compare);
//
//        //
//        // If there is no chunk corresponding to the offset to which
//        // the file should be truncated, allocate one at that point.
//        // This can happen due to the following cases:
//        // 1. The offset to truncate to exceeds the last chunk of
//        // the file.
//        // 2. There is a hole in the file and the offset to truncate
//        // to corresponds to the hole.
//        //
//    if ((m == chunkInfo.end()) || ((*m)->offset != lastChunkStartOffset)) 
//    {
//        // Allocate a chunk at this offset
//        *allocOffset = lastChunkStartOffset;
//        return 1;
//    }
//
//    if ((*m)->offset <= offset) 
//    {
//        // truncate the last chunk so that the file
//        // has the desired size.
//        (*m)->TruncateChunk(offset - (*m)->offset);
//        ++m;
//    }
//
//    // delete everything past the last chunk
//    while (m != chunkInfo.end()) 
//    {
//        (*m)->DeleteChunk();
//        ++m;
//        fa->chunkcount--;
//        UpdateNumChunks(-1);
//    }
//
//    gettimeofday(&fa->mtime, NULL);
    return 0;
}

/*!
 * \brief check whether one directory is a descendant of another
 * \param[in] src    file ID of possible ancestor
 * \param[in] dst    file ID of possible descendant
 *
 * Check dst and each of its ancestors to see whether src is
 * among them; used to avoid making a directory into its own
 * child via rename.
 */
bool MetaTree::is_descendant(int64_t src, int64_t dst)
{
    while (src != dst && dst != ROOTFID) 
    {
        MetaFattr *dotdot = Lookup(dst, "..");
        dst = dotdot->id();
    }

    return (src == dst);
}

/*!
 * \brief rename a file or directory
 * \param[in]    parent    file id of parent directory
 * \param[in]    oldname    the file's current name
 * \param[in]    newname    the new name for the file
 * \param[in]    oldpath    the fully qualified path for the file's current name
 * \param[in]    overwrite when set, overwrite the dest if it exists
 * \return        status code
 */

int MetaTree::Rename(int64_t parent, const string &oldname, string &newname, const string &oldpath, bool overwrite)
{
    int status;

    MetaDentry *src = GetDentry(parent, oldname);

    if (src == NULL)
    {
        return -ENOENT;
    }

    int64_t ddir;
    string dname;
    string::size_type rslash = newname.rfind('/');
    if (rslash == string::npos) 
    {
        ddir = parent;
        dname = newname;
    } 
    else 
    {
        MetaFattr *ddfattr = LookupPath(parent, newname.substr(0, rslash));

        if (ddfattr == NULL)
        {
            return -ENOENT;
        }
        else if (ddfattr->type != BLADE_DIR)
        {
            return -ENOTDIR;
        }    
        else
        {
            ddir = ddfattr->id();
        }

        dname = newname.substr(rslash + 1);
    }

    if (!Legalname(dname))
    {
        return -EINVAL;
    }

    if (ddir == parent && dname == oldname)
    {    
        return 0;
    }

    MetaFattr *sfattr = Lookup(parent, oldname);
    MetaFattr *dfattr = Lookup(ddir, dname);
    bool dexists = (dfattr != NULL);
    FileType t = sfattr->type;

    if ((!overwrite) && dexists)
    {
        return -EEXIST;
    }

    if (dexists && t != dfattr->type)
    {
        return (t == BLADE_DIR) ? -ENOTDIR : -EISDIR;
    }

    if (dexists && t == BLADE_DIR && !Emptydir(dfattr->id()))
    {
        return -ENOTEMPTY;
    }

    if (t == BLADE_DIR && is_descendant(sfattr->id(), ddir))
    {    
        return -EINVAL;
    }

    if (dexists) 
    {
        status = (t == BLADE_DIR) ? Rmdir(ddir, dname, newname) : Remove(ddir, dname, newname);

        if (status != 0)
        {
            return status;
        }

    }

    if (sfattr->filesize > 0) 
    {
        UpdateSpaceUsageForPath(oldpath, -sfattr->filesize);
        UpdateSpaceUsageForPath(newname, sfattr->filesize);
    }

    int64_t srcFid = src->id();

    if (t == BLADE_DIR) 
    {
        // get rid of the linkage of the "old" ..
        Unlink(srcFid, "..", sfattr, true);
    }

    status = Del(src);
    assert(status == 0);
    MetaDentry *newSrc = new MetaDentry(ddir, dname, srcFid);

    //status = insert(newSrc);
    assert(status == 0);
    if (t == BLADE_DIR) 
    {
        // create a new linkage for ..
        status = Link(srcFid, "..", BLADE_DIR, ddir, 1);
        assert(status == 0);
    }

    return 0;
}


/*!
 * \brief Change the degree of replication for a file.
 * \param[in] dir    file id of the file
 * \param[in] numReplicas    desired degree of replication
 * \return        status code (-errno on failure)
 */
int MetaTree::ChangePathReplication(int64_t fid, int16_t numReplicas)
{
    MetaFattr *fa = GetFattr(fid);

    if (fa == NULL)
    {
        return -ENOENT;
    }

    if (fa->type == BLADE_DIR)
    {
        return ChangeDirReplication(fa, numReplicas);
    }

    return ChangeFileReplication(fa, numReplicas);

}

int MetaTree::get_block_fid(int64_t block_id, int64_t *fid)
{
    BtreeReadHandle read_handle;
    MetaChunkInfo * meta_chunk_info;
    int ret = meta_btree_.get_read_handle(read_handle);
    assert(ERROR_CODE_OK == ret);
    Meta * value; 

    const Key fkey(BLADE_CHUNKINFO, MINFID, MIN_CHUNK_OFFSET);
    const Key tkey(BLADE_CHUNKINFO, MAXFID, MAX_CHUNK_OFFSET);
    meta_btree_.set_key_range(read_handle, &fkey, 0, &tkey, 0);

    while(ERROR_CODE_OK == meta_btree_.get_next(read_handle, value)) 
    {
        meta_chunk_info = dynamic_cast<MetaChunkInfo *>(value);                                                                                     
        assert(NULL != meta_chunk_info);
        if(meta_chunk_info != NULL && meta_chunk_info->chunkId == block_id)
        {    
            *fid = meta_chunk_info->fid;
            return 0;
        }    
        continue;
    }
    return -1;

}

//更改文件的备份数量,需要对这个文件所有的block的备份数量同时进行修改
int MetaTree::chang_file_replication(int64_t fid, int16_t num_replicas)
{
    BtreeReadHandle read_handle;
    BtreeWriteHandle write_handle;

    const Key fid_key(BLADE_FATTR, fid);
    Meta * value;
    MetaChunkInfo * meta_chunk_info;
    std::vector<MetaChunkInfo *> chunks;
    std::vector<MetaChunkInfo *>::iterator chunk_iter;
    int ret = meta_btree_.get_read_handle(read_handle);
    assert(ERROR_CODE_OK == ret);
    ret = meta_btree_.get_write_handle(write_handle);
    assert(ERROR_CODE_OK == ret);
    ret = meta_btree_.Get(read_handle, fid_key, value);

    //file存在
    if(ERROR_CODE_OK == ret)
    {
        MetaFattr * file_attr= static_cast<MetaFattr *>(value);

        if(file_attr->numReplicas != num_replicas)
        {
            file_attr->numReplicas = num_replicas;
            meta_btree_.Put(write_handle, fid_key, file_attr, true);
        }

        //获取这个文件上面所有的block
        const Key fkey(BLADE_CHUNKINFO, fid, MIN_CHUNK_OFFSET);
        const Key tkey(BLADE_CHUNKINFO, fid, MAX_CHUNK_OFFSET);
        meta_btree_.set_key_range(read_handle, &fkey, 0, &tkey, 0);

          while(ERROR_CODE_OK == meta_btree_.get_next(read_handle, value))
          {
            meta_chunk_info = dynamic_cast<MetaChunkInfo *>(value);    
            assert(NULL != meta_chunk_info);
            if(meta_chunk_info != NULL)
            {
                chunks.push_back(meta_chunk_info);
            }
          }

        chunk_iter = chunks.begin();
        for( ; chunk_iter != chunks.end(); chunk_iter++)
        {
            if((*chunk_iter)->num_replicas_ != num_replicas);
            {
                const Key chunk_tmp_key(BLADE_CHUNKINFO, fid, (*chunk_iter)->offset);
                (*chunk_iter)->num_replicas_ =  num_replicas;
                meta_btree_.Put(write_handle, chunk_tmp_key, *chunk_iter, true);
                //调用layout_manager对block的副本数量进行修改
                layout_manager_.change_replication_num((*chunk_iter)->chunkId, num_replicas);
            }
        }

        write_handle.End();
        return ERROR_CODE_OK;
    }
    else // file 不存在
    {

    }
    return ERROR_CODE_NOT_FOUND;
}


int MetaTree::ChangeFileReplication(MetaFattr *fa, int16_t numReplicas)
{
    if (fa->type != BLADE_FILE)
        return -EINVAL;

    vector<MetaChunkInfo*> chunkInfo;

    fa->setReplication(numReplicas);

    Getalloc(fa->id(), chunkInfo);

    for (vector<MetaChunkInfo *>::size_type i = 0; i < chunkInfo.size(); ++i) 
    {
//        gLayoutManager.ChangeChunkReplication(chunkInfo[i]->chunkId);
    }

    return 0;
}


/*!
 * \brief  A file that has to be removed is currently busy.  So, rename the
 * file to the dumpster and we'll clean it up later.
 * \param[in] dir    file id of the parent directory
 * \param[in] fname    file name
 * \return        status code (zero on success)
 */
int MetaTree::MoveToDumpster(int64_t dir, const string &fname)
{
    static uint64_t counter = 1;
    string tempname = "/" + DUMPSTERDIR + "/";
    MetaFattr *fa = Lookup(ROOTFID, DUMPSTERDIR);

    if (fa == NULL) 
    {
        // Someone nuked the dumpster
        MakeDumpsterDir();
        fa = Lookup(ROOTFID, DUMPSTERDIR);
        if (fa == NULL) 
        {
            assert(!"No dumpster");
            return -1;
        }
    }

    // can't move something in the dumpster back to dumpster
    if (fa->id() == dir)
    {
        return -EEXIST;
    }

    // generate a unique name
    tempname += fname + to_string(counter);

    counter++;
    // space accounting has been done before the call to this function.  so,
    // we don't rename to do any accounting and hence pass in "" for the old
    // path name.
    return Rename(dir, fname, tempname, "", true);
}


/*!
 * \brief Periodically, cleanup the dumpster and reclaim space.  If
 * the lease issued on a file has expired, then the file can be nuked.
 */
void MetaTree::CleanupDumpster()
{
    MetaFattr *fa = Lookup(ROOTFID, DUMPSTERDIR);

    if (fa == NULL) 
    {
        // Someone nuked the dumpster
        MakeDumpsterDir();
    }

    int64_t dir = fa->id();

    vector <MetaDentry *> v;
    Readdir(dir, v);

    for_each(v.begin(), v.end(), RemoveDumpsterEntry(dir, *this));
}

bool MetaTree::block_exist(int64_t fid, int64_t block_id)
{
    BtreeReadHandle read_handle;
    MetaChunkInfo * meta_chunk_info;
    int ret = meta_btree_.get_read_handle(read_handle);
    assert(ERROR_CODE_OK == ret);
    Meta * value; 

    const Key fkey(BLADE_CHUNKINFO, fid, MIN_CHUNK_OFFSET);
    const Key tkey(BLADE_CHUNKINFO, fid, MAX_CHUNK_OFFSET);
    meta_btree_.set_key_range(read_handle, &fkey, 0, &tkey, 0);

    while(ERROR_CODE_OK == meta_btree_.get_next(read_handle, value)) 
    {
        meta_chunk_info = dynamic_cast<MetaChunkInfo *>(value);                                                                                     
        assert(NULL != meta_chunk_info);
        if(NULL != meta_chunk_info && meta_chunk_info->chunkId == block_id)
        {
            return true;
        }
        continue;
    }
    return false;
}

}//end of namespace btree
}//end of namespace bladestore
