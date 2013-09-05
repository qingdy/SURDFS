#include "block_scanner_manager.h"
#include "blade_common_data.h"
#include "blade_common_define.h"

using namespace bladestore::common;

namespace bladestore
{
namespace nameserver
{

BlockScannerManager::BlockScannerManager(MetaManager & meta_manager): meta_manager_(meta_manager), destroy_(false)
{

}

BlockScannerManager::~BlockScannerManager()
{

}

bool BlockScannerManager::add_scanner(int id, Scanner * scanner)
{
	__gnu_cxx::hash_map<int, Scanner *>::iterator iter = scanners_.find(id);
	if (iter == scanners_.end())
	{
		scanners_[id] = scanner;
		return true;
	}
	iter->second = scanner;
	return true;
}

int BlockScannerManager::Run()
{
	if (destroy_)
	{
		return BLADE_SUCCESS;
	}

	meta_manager_.get_layout_manager().Foreach(*this);

	__gnu_cxx::hash_map<int ,Scanner*>::iterator iter = scanners_.begin();
	for ( ; iter != scanners_.end(); iter++)
	{
		Scanner * scanner = iter->second;
		if (NULL == scanner)
		{
			continue;
		}

		if (scanner->result_.size() > 0)
		{
			scanner->launcher_.build_blocks(scanner->result_);
		}

		if (destroy_)
		{
			return BLADE_SUCCESS;
		}
	}
	return BLADE_SUCCESS;
}

int BlockScannerManager::Process(int64_t block_id)
{
	int ret = SCANNER_CONTINUE;

	if (destroy_)
	{
		return SCANNER_BREAK;
	}
	__gnu_cxx::hash_map<int, Scanner * >::iterator iter = scanners_.begin();
	for ( ; iter != scanners_.end(); iter++)
	{
		Scanner * scanner = iter->second;

		if (NULL == scanner)
		{
			continue;
		}

		if (scanner->is_check_)
		{
			if (static_cast<int32_t>((scanner->result_).size()) > scanner->limits_)
			{
				LOGV(MSG_ERROR, "return size (%u) > scanner's limit (%d)", (scanner->result_).size(), scanner->limits_);
				continue;	
			}

			ret = scanner->launcher_.Check(block_id);

			if (SCANNER_CONTINUE == ret)
			{
				scanner->result_.insert(block_id);
			}
			else if (SCANNER_SKIP == ret)
			{
				break;
			}
		}

		if (destroy_)
		{
			return SCANNER_BREAK;
		}
	}
	return ret;
}

}//end of namespace nameserver
}//end of namespace bladestore
