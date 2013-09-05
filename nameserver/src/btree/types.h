/*
 *version  :  1.0
 *author   :  chen yunyun
 *date     :  2012-4-18
 *
 */

#ifndef BLADESTORE_TYPES_H
#define BLADESTORE_TYPES_H

#include <cerrno>
#include <cassert>

namespace bladestore
{
namespace btree
{

enum MetaType 
{
	BLADE_MIN,
	BLADE_UNINIT,		//uninitialized
	BLADE_INTERNAL,		//internal node
	BLADE_FATTR,		//file attributes
	BLADE_DENTRY,		//directory entry
	BLADE_CHUNKINFO,		//chunk information
	BLADE_SENTINEL = 99999,	//internal use, must be largest
	BLADE_MAX
};


enum FileType 
{
	BLADE_NONE,		//uninitialized
	BLADE_FILE,		//plain file
	BLADE_DIR		//directory
};


}//end of btree
}//end of bladestore
#endif 
