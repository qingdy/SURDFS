/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-4-16
 *
 */
#ifndef BLADESTORE_NAMESERVER_BLOCK_SCANNER_H
#define BLADESTORE_NAMESERVER_BLOCK_SCANNER_H

namespace bladestore
{
namespace nameserver
{

class BlockScanner
{
public:
	BlockScanner()
	{

	}

	virtual ~BlockScanner()
	{

	}

	virtual int Process(int64_t block_id) = 0;
};

}//end of namespace nameserver
}//end of namespace bladestore
#endif
