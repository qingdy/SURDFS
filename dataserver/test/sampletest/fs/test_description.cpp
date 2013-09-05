/*
 * =====================================================================================
 *
 *       Filename:  test_description.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  04/24/2012 09:34:00 AM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  ZQ (), 
 *   Organization:  
 *
 * =====================================================================================
 */
#include <stdlib.h>
#include <string>
#include "block.h"
#include "fsdataset.h"
#include <iostream>
#include <map>
using namespace std;
using namespace bladestore::dataserver;
int main()
{
	Block block(1,1,1);
    FSDataSet dataset = FSDataSet("/home/fstest");
	WriteBlockFd fds;
	int32_t block_fd;
	int32_t meta_fd;
	int32_t return_code;
	int64_t used = AtomicGet64(&((dataset.data_dir_)->used_space_));
	cout << "used space: " << used << endl;
	fds = dataset.write_block_and_meta_fd(block);
	block_fd = fds.block_fd_;
	meta_fd = fds.meta_fd_;
	return_code = fds.return_code_;

	used = AtomicGet64(&((dataset.data_dir_)->used_space_));
	cout << "used space : " << used << endl;
	cout << "block fd: " << block_fd << endl;
	cout << "meta_fd: " << meta_fd << endl;

	pwrite(block_fd,"hello",5,0);
	fsync(block_fd);

	bool ret = dataset.close_file(block,block_fd,meta_fd,kReadAfterWriteCompleteMode);
	cout << "close file : " << ret << endl;
	used = AtomicGet64(&((dataset.data_dir_)->used_space_));
	cout << "used space after close_file : " << used << endl;
	bool res = true;
	cout << res << endl;
	//close(block_fd);
	//close(meta_fd);
	remove("/home/fstest/blk_1_1.meta");
	set<BlockInfo *> blocks_info;
	dataset.get_block_report(blocks_info);
	used = AtomicGet64(&((dataset.data_dir_)->used_space_));
	cout << "used space after get_block_report : " << used << endl;

	set<BlockInfo *>::const_iterator begin = blocks_info.begin();
	while(begin != blocks_info.end())
	{
	    int64_t blockid = (*begin)->block_id_;
		int32_t blockversion = (*begin)->version_;
		cout << "blockid: " << blockid << endl;
		cout << "block version: " << blockversion << endl;
		++ begin;
	} 

	set<int64_t> ds_id;
	map<int64_t,int64_t> dsid_length;
	ds_id.insert(1);
	ds_id.insert(2);
	ds_id.insert(3);
	dataset.get_blocks_length(ds_id,dsid_length);
	map<int64_t,int64_t>::iterator map_it = dsid_length.begin();
	while(map_it != dsid_length.end())
	{
	    int64_t len = map_it->second;
		cout << len << endl;
		++map_it;
	}

	Block block2(2,2,2);
	int32_t version = block.get_block_version();
	cout << "version : " << version << endl;

	ReadBlockInfo info;
    info = dataset.read_block_and_meta_file(block2);
	block_fd = info.block_fd_;
	meta_fd = info.meta_fd_;
	int64_t block_length = info.block_length_;
	cout << "block_fd: " << block_fd;
	cout << "meta_fd: " << meta_fd;
	cout << "block_length " << block_length;
//	int32_t fd = dataset.get_block_file_description(block,0);
//	cout << "fd: " << fd << endl;
//	int64_t len = dataset.get_block_length(block);
//	cout << "len: " << len << endl;
//	bool exist = dataset.block_file_exists(block);
//	cout << "exist: " << exist << endl;
   
	set<int64_t> todelete;
    
	todelete.insert(1);
    cout << "after insert" << endl;
//	dataset.remove_blocks(todelete);
}
