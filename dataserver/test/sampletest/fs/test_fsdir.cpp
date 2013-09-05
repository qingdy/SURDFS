/*
 * =====================================================================================
 *
 *       Filename:  test_fsdir.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  2012年04月09日 17时21分45秒
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  ZQ (), 
 *   Organization:  
 *
 * =====================================================================================
 */
#include <stdlib.h>
#include "fsdir.h"
#include <string>
#include <stdio.h>
#include <iostream>
using namespace std;
using namespace bladestore::dataserver;
int main()
{
	string dir = "/home/fstest";
	FSDir fsdir = FSDir(dir);
//	printf("dir : %s\n",fsdir.dir_.c_str());
//	printf("numblocks : %d\n",fsdir.num_blocks_);
//	printf("lastindex : %d\n",fsdir.last_child_idx_);
//	string subdirname = (*(fsdir.children_[0]))->dir_;
//	cout << subdirname << endl;
    int64_t blockid = 1;
	int64_t numbytes = 1;
	int32_t blockversion = 1;
  //  for(int i = 0;i < 8;i++)
//	{
//		blockid += i;
//		numbytes += i;
//		blockversion += i;
  //	Block block = Block(blockid,numbytes,blockversion);
    //	string src = "/home/tmp/" + block.get_block_name();
    //	fsdir.add_block(block,src);
//	}

    Block block1 = Block(blockid,numbytes,blockversion);
	string src1 = "/home/tmp/blk_1";
	fsdir.add_block(block1,src1);

	Block block2 = Block(blockid+1,numbytes+1,blockversion+1);
	string src2 = "/home/tmp/blk_2";
	fsdir.add_block(block2,src2);

	Block block3 = Block(blockid+2,numbytes+2,blockversion+2);
	string src3 = "/home/tmp/blk_3";
	fsdir.add_block(block3,src3);

	Block block4 = Block(blockid+3,numbytes+3,blockversion+3);
	string src4 = "/home/tmp/blk_4";
	fsdir.add_block(block4,src4);

	Block block5 = Block(blockid+4,numbytes+4,blockversion+4);
	string src5 = "/home/tmp/blk_5";
	fsdir.add_block(block5,src5);

	Block block6 = Block(blockid+5,numbytes+5,blockversion+5);
	string src6 = "/home/tmp/blk_6";
	fsdir.add_block(block6,src6);

	Block block7 = Block(blockid+6,numbytes+6,blockversion+6);
	string src7 = "/home/tmp/blk_7";
	fsdir.add_block(block7,src7);

	Block block8 = Block(blockid+7,numbytes+7,blockversion+7);
	string src8 = "/home/tmp/blk_8";
	fsdir.add_block(block8,src8);

	Block block9 = Block(blockid+8,numbytes+8,blockversion+8);
	string src9 = "/home/tmp/blk_9";
	fsdir.add_block(block9,src9);

	Block block10 = Block(blockid+9,numbytes+9,blockversion+9);
	string src10 = "/home/tmp/blk_10";
	fsdir.add_block(block10,src10);

	Block block11 = Block(blockid+10,numbytes+10,blockversion+10);
	string src11 = "/home/tmp/blk_11";
	fsdir.add_block(block11,src11);

	Block block12 = Block(blockid+11,numbytes+11,blockversion+11);
	string src12 = "/home/tmp/blk_12";
	fsdir.add_block(block12,src12);

	Block block13 = Block(blockid+12,numbytes+12,blockversion+12);
	string src13 = "/home/tmp/blk_13";
	fsdir.add_block(block13,src13);

	Block block14 = Block(blockid+13,numbytes+13,blockversion+13);
	string src14 = "/home/tmp/blk_14";
	fsdir.add_block(block14,src14);

	Block block15 = Block(blockid+14,numbytes+14,blockversion+14);
	string src15 = "/home/tmp/blk_15";
	fsdir.add_block(block15,src15);

	Block block16 = Block(blockid+15,numbytes+15,blockversion+15);
	string src16 = "/home/tmp/blk_16";
	fsdir.add_block(block16,src16);

	Block block17 = Block(blockid+16,numbytes+16,blockversion+16);
	string src17 = "/home/tmp/blk_17";
	fsdir.add_block(block17,src17);

	Block block18 = Block(blockid+17,numbytes+17,blockversion+17);
	string src18 = "/home/tmp/blk_18";
	fsdir.add_block(block18,src18);

	Block block19 = Block(blockid+18,numbytes+18,blockversion+18);
	string src19 = "/home/tmp/blk_19";
	fsdir.add_block(block19,src19);

	Block block20 = Block(blockid+19,numbytes+19,blockversion+19);
	string src20 = "/home/tmp/blk_20";
	fsdir.add_block(block20,src20);

    Block block21 = Block(blockid+20,numbytes+20,blockversion+20);
	string src21 = "/home/tmp/blk_21";
	fsdir.add_block(block21,src21);

	Block block22 = Block(blockid+21,numbytes+21,blockversion+21);
	string src22 = "/home/tmp/blk_22";
	fsdir.add_block(block22,src22);

	Block block23 = Block(blockid+22,numbytes+22,blockversion+22);
	string src23 = "/home/tmp/blk_23";
	fsdir.add_block(block23,src23);

	Block block24 = Block(blockid+23,numbytes+23,blockversion+23);
	string src24 = "/home/tmp/blk_24";
	fsdir.add_block(block24,src24);

	Block block25 = Block(blockid+24,numbytes+24,blockversion+24);
	string src25 = "/home/tmp/blk_25";
	fsdir.add_block(block25,src25);




	return 0;

}
