/*
 *version  : 1.0
 *authro   : chen yunyun
 *date     : 2012-4-18
 *
 */
#ifndef BLADESTORE_BASE_H
#define BLADESTORE_BASE_H

#include <stdint.h>
#include <string>
#include "blade_common_define.h"
#include "types.h"

using std::string;
using namespace bladestore::common;

namespace bladestore
{
namespace btree
{

typedef int64_t KeyData;	//"opaque" key data

/*
 *brief search key
 */
class Key 
{
	MetaType kind;	//for what kind of metadata
	KeyData kdata1;	//associated identification
	KeyData kdata2; //and more identification
public:
	static const KeyData MATCH_ANY = -1;

	Key(MetaType k, KeyData d): kind(k), kdata1(d), kdata2(0) 
	{ 

	}
	
	Key(MetaType k, KeyData d1, KeyData d2): kind(k), kdata1(d1), kdata2(d2) 
	{ 

	}

	Key(): kind(BLADE_UNINIT), kdata1(0), kdata2(0) 
	{ 

	}

	int Compare(const Key &test) const;
	
	bool operator < (const Key &test) const 
	{ 
		return Compare(test) < 0; 
	}
	
	int operator - (const Key &test) const 
	{
		return Compare(test);
	}
	
	bool operator == (const Key &test) const 
	{ 
		return Compare(test) == 0; 
	}

	bool operator != (const Key &test) const 
	{ 
		return Compare(test) != 0; 
	}
};

//MetaNode flag values
static const int META_CPBIT = 1;//CP parity bit
static const int META_NEW = 2;	//new since start of CP
static const int META_ROOT = 4;	//root node
static const int META_LEVEL1 = 8; //children are leaves
static const int META_SKIP = 16; //exclude from current CP

/*
 *brief base class for both internal and leaf nodes
 */
class MetaNode 
{
	MetaType type;
	int flagbits;
public:
	MetaNode(MetaType t): type(t), flagbits(0) 
	{ 

	}

	MetaNode(MetaType t, int f): type(t), flagbits(f) 
	{ 

	}

	MetaType metaType() const 
	{ 
		return type; 
	}

	virtual ~MetaNode() 
	{ 

	}

	virtual const Key key() const = 0;	//cons up key value for node
	virtual const string show() const = 0;	//print out contents

	int flags() const 
	{ 
		return flagbits; 
	}

	void setflag(int bit) 
	{ 
		flagbits |= bit; 
	}
	
	void clearflag(int bit) 
	{
		flagbits &= ~bit; 
	}

	bool testflag(int bit) const 
	{ 
		return (flagbits & bit) != 0; 
	}
};

}//end of namespace btree
}//end of namespace bladestore
#endif
