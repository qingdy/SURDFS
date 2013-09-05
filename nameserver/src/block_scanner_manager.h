/*
 *version  : 1.0
 *author   : chen yunyun, funing
 *date     : 2012-4-19
 *
 */
#ifndef BLADESTORE_NAMESERVER_BLOCK_SCANNER_MANAGER_H
#define BLADESTORE_NAMESERVER_BLOCK_SCANNER_MANAGER_H
#include <ext/hash_map>

#include "block_scanner.h"
#include "blade_meta_manager.h"

namespace bladestore
{
namespace nameserver
{

using namespace bladestore::common;

class Launcher
{
public:
	virtual ~Launcher()
    {

    }

	//返回CONTINUE的时候，需要把刚刚检测的block再次加入到无锁队列里， NEXT的话不加入
	virtual int Check(int64_t block_id) = 0;

	virtual int build_blocks(const std::set<int64_t> & blocks) = 0;
};

class BlockScannerManager : public BlockScanner
{
public:
	struct Scanner
	{
	public:
		//scan the blocks, there are two, one for replicate, one for redundant 
		Scanner(bool is_check, int32_t limits, Launcher & launcher, std::set<int64_t> & result) : is_check_(is_check), launcher_(launcher), limits_(limits), result_(result)
		{

		}

	public:
		bool is_check_;

		Launcher & launcher_;

		int32_t limits_;

		std::set<int64_t> & result_;

	private:
		Scanner();
	};

public:
	BlockScannerManager(MetaManager & meta_manager);

	~BlockScannerManager();

	bool add_scanner(const int id, Scanner * scanner);

	virtual int Process(int64_t block_id);

	void Destroy()
	{
		destroy_ = true;	
	}

	int Run();

public:
	__gnu_cxx::hash_map<int, Scanner *> scanners_;

	MetaManager & meta_manager_;

	bool destroy_;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
