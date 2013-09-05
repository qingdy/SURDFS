#ifndef FSDIR_H
#define FSDIR_H

#include <stdint.h>
#include <string>
#include <ext/hash_map>
#include <vector>
#include "log.h"
#include "atomic.h"
#include "blade_common_define.h"
#include "block.h"
#include "blade_rwlock.h"
#include "block_meta_info.h"

using namespace std;
using namespace __gnu_cxx;
using namespace pandora;
using namespace bladestore::common;

namespace bladestore
{
namespace dataserver
{

class FSDir
{
public:
    //@para dir, local path
    explicit FSDir(const string & dir);
    ~FSDir();

    bool Init();
    //@return new block's local path
    string AddBlock();
    /* Delete a Block 
     * @para file_path, block's local path 
     */
    void DelBlock(const char* file);

    // iterator local storage directory and rebuild block map
    void GetBlockMap(hash_map<int64_t, BlockMetaInfo> &block_map);

private:
    string              dir_;
    int32_t             num_blocks_[BLADE_MAX_DIR_NUM];
    pthread_mutex_t     num_blocks_mutex_;

    DISALLOW_COPY_AND_ASSIGN(FSDir);
};

}
}

#endif
