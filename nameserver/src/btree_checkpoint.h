/*
 *version : 1.0
 *author  : chen yunyun 
 *date    : 2012-5-10
 *
 */
#ifndef BLADESTORE_NAMESERVER_BTREE_CHECK_POINT_H
#define BLADESTORE_NAMESERVER_BTREE_CHECK_POINT_H

#include <fstream>
#include <sstream>
#include <string>

#include "btree_meta.h"
#include "meta.h"

using namespace std;
using namespace bladestore::btree;

namespace bladestore
{
namespace nameserver
{

class DiskEntry;

class BtreeCheckPoint 
{
public:
	BtreeCheckPoint(MetaTree & meta_tree); 
	~BtreeCheckPoint(); 

	void init_entry_map();

	//对外接口， 生成和重做checkpoint
	bool Rebuild(char * filename, int16_t min_replicas);
	int do_cp(char * cpname);	

public:
	static const int VERSION = 1;
	MetaTree & meta_tree_;

private:

	//对叶子节点进行遍历，同时checkpoint到文件中
	int write_leaves();

	DiskEntry * disk_entry_map_;  //rebuild checkpoint文件时的处理文件
	ofstream offile_;		//生成checkpoint时使用
	ifstream iffile_;      //restore时使用
};

}//end of namespace nameserver
}//end of namespace bladestore

#endif
