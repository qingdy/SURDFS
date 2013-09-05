/*
 * version : 1.0
 * author : YeWeimin
 * date : 2012-5-30
 */

#ifndef BLADESTORE_COMMON_FILE_INFO_H
#define BLADESTORE_COMMON_FILE_INFO_H
namespace bladestore 
{
namespace common
{
//文件元信息，包括文件名，类型，含有的Block数目等；
class FileInfo
{
public:
    FileInfo():file_id_(0), file_type_(0), block_count_(0), num_replicas_(0), file_size_(0)
    {
    }

    FileInfo(int64_t file_id, int8_t file_type, struct timeval mt, struct timeval ct, struct timeval crt, int64_t block_count, int16_t num_replicas, off_t file_size):file_id_(file_id), file_type_(file_type), mtime_(mt), ctime_(ct), crtime_(crt), block_count_(block_count), num_replicas_(num_replicas), file_size_(file_size)
    {

    }

    ~FileInfo()
    {
    }

    int64_t get_file_id() const
    {
        return file_id_;
    }

    int8_t get_file_type() const
    {
        return file_type_;
    }

    struct timeval get_mtime() const
    {
        return mtime_;
    }

    struct timeval get_ctime() const
    {
        return ctime_;
    }

    struct timeval get_crtime() const
    {
        return crtime_;
    }

    int64_t get_block_count() const
    {
        return block_count_;
    }

    int16_t get_num_replicas() const
    {
        return num_replicas_;
    }

    off_t get_file_size() const
    {
        return file_size_;
    }

    void set_file_id(int64_t file_id)
    {
        file_id_ = file_id;
    }

    void set_file_type(int8_t file_type)
    {
        file_type_ = file_type;
    }

    void set_mtime(struct timeval mtime)
    {
        mtime_ = mtime;
    }

    void set_ctime(struct timeval ctime)
    {
        ctime_ = ctime;
    }

    void set_crtime(struct timeval crtime)
    {
        crtime_ = crtime;
    }

    void set_block_count(int64_t block_count)
    {
        block_count_ = block_count;
    }

    void set_num_replicas(int16_t num_replicas) 
    {
        num_replicas_ = num_replicas;
    }

    void set_file_size(off_t file_size)
    {
        file_size_ = file_size;
    }
private:
    int64_t file_id_;
    int8_t file_type_;
    struct timeval mtime_, ctime_, crtime_;
    int64_t block_count_;
    int16_t num_replicas_;
    off_t  file_size_;
};

} //end of namespace common 
} //end of namespace bladestore 
#endif
