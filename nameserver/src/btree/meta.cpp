#include <iostream>
#include <fstream>
#include <sstream>
#include <time.h>
#include "meta.h"
#include "util.h"

using namespace bladestore::common;
namespace bladestore
{
namespace btree
{

/*
 * Seed the unique id generators for files/chunks to start at 2
 */

UniqueID fileID(0, ROOTFID);
UniqueID chunkID(1, ROOTFID);

/*
 * Initialize the chunk version increment to start at 0.  It'll get bumped 
 * when the system starts up.
*/

int64_t chunkVersionInc = 1;

/*
 *brief compare key against test value
 *param[in] test	the test value
 *return	negative, zero, or positive, according as the
 *this key value is <, =, or > than the test value.
 */
int Key::Compare(const Key &test) const
{
	int kdiff = kind - test.kind;
	int d = 0;

	if (kdiff != 0)
	{
		d = kdiff;
	}
	else if (kdata1 != test.kdata1) 
	{
		d = (kdata1 < test.kdata1) ? -1 : 1;
	}
	else if (kdata2 != test.kdata2 && kdata2 != MATCH_ANY && test.kdata2 != MATCH_ANY)
	{
		d = (kdata2 < test.kdata2) ? -1 : 1;
	}

	return d;
}

const string MetaDentry::show() const
{
	return "dentry/name/" + name + "/id/" + to_string(id()) + "/parent/" + to_string(dir);
}

bool MetaDentry::match(Meta *m)
{
	MetaDentry *d = refine<MetaDentry>(m);
	return (d != NULL && d->compareName(name) == 0);
}

string showtime(struct timeval t)
{
	std::ostringstream n(std::ostringstream::out);
	n << t.tv_sec << "/" << t.tv_usec;
	return n.str();
}

const string MetaFattr::show() const
{
	static string fname[] = { "empty", "file", "dir" };

	return "fattr/" + fname[type] + "/id/" + to_string(id()) +
		"/chunkcount/" + to_string(chunkcount) 
		+ "/numReplicas/" + to_string(numReplicas) 
		+ "/mtime/" + showtime(mtime) 
		+ "/ctime/" + showtime(ctime) + "/crtime/" + showtime(crtime) 
		+ "/filesize/" + to_string(filesize);
}

const string MetaChunkInfo::show() const
{
	return "chunkinfo/fid/" + to_string(id()) + 
		"/chunkid/" + to_string(chunkId) + 
		"/offset/" + to_string(offset) +
		"/chunkVersion/" + to_string(chunkVersion);
}

}//end of namespace btree
}//end of namespace bladestore
