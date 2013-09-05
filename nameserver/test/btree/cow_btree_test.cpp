#include <stdio.h>
#include <vector>

#include <gtest/gtest.h>

#include "btree_meta.h"
#include "layout_manager.h"

using namespace bladestore::btree;
using namespace std;
using namespace bladestore::nameserver;
using namespace pandora;

int main()
{
	int64_t fid = 0;
	BladeLayoutManager layout_manager;
	MetaTree meta_tree(layout_manager);	
	vector<MetaDentry *> v;
	meta_tree.mkdir(2,"a",&fid);
	fid=0;
	meta_tree.mkdir(2,"b",&fid);
	fid=0;
	meta_tree.mkdir(2,"c",&fid);
	fid=0;
	meta_tree.mkdir(2,"d",&fid);
	fid=0;
	meta_tree.mkdir(2,"e",&fid);
	fid=0;
	meta_tree.mkdir(2,"f",&fid);
	fid=0;
	meta_tree.mkdir(2,"g",&fid);
	fid=0;
	meta_tree.mkdir(2,"h",&fid);
	fid=0;
	meta_tree.mkdir(2,"i",&fid);

	meta_tree.readdir(2,v);	


	EXPECT_EQ(9, v.size());

	for(int i = 0; i < v.size(); i++)
	{
		printf("vector id is: %lld\n",v.at(i)->id());
	}

	return 0;
}
