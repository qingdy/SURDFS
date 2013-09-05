#ifndef BLADESTORE_CLIENT_CLIENT_CURRENT_BLOCK_H
#define BLADESTORE_CLIENT_CLIENT_CURRENT_BLOCK_H

#include <vector>
#include "blade_common_define.h"

using std::vector;

namespace bladestore
{
namespace client
{
class CurrentBlock 
{
public:
    CurrentBlock(): file_id_(0), block_id_(0), block_version_(0), offset_(0),
                    block_offset_(0), lseek_offset_(0), length_(0), file_length_(0),
                    mode_(0), dataserver_id_() {}
    ~CurrentBlock() {}

    int16_t GetDataServerNum() {return dataserver_id_.size();}
    void    CleanAll()
    {
        file_id_ = 0;
        block_id_ = 0;
        block_version_ = 0;
        offset_ = 0;
        block_offset_ = 0;
        lseek_offset_ = 0;
        length_ = 0;
        file_length_ = 0;
        mode_ = 0;
        dataserver_id_.clear();
    }

    void set_file_id(const int64_t file_id) {file_id_ = file_id;}
    void set_block_id(const int64_t block_id) {block_id_ = block_id;}
    void set_block_version(const int32_t block_version) {block_version_ = block_version;}
    void set_offset(const int64_t offset) {offset_ = offset;}
    void set_block_offset(const int64_t block_offset) {block_offset_ = block_offset;}
    void set_lseek_offset(const int64_t lseek_offset) {lseek_offset_ = lseek_offset;}
    void set_length(const int64_t length) {length_ = length;}
    void set_file_length(const int64_t file_length) {file_length_ = file_length;}
    void set_mode(const int8_t mode) {mode_ = mode;}
    void set_dataserver_id(const vector<uint64_t> &dataserver_id) {dataserver_id_ = dataserver_id;}
    
    int64_t file_id() {return file_id_;}
    int64_t block_id() {return block_id_;}
    int32_t block_version() {return block_version_;}
    int64_t offset() {return offset_;}
    int64_t block_offset() {return block_offset_;}
    int64_t lseek_offset() {return lseek_offset_;}
    int64_t length() {return length_;}
    int64_t file_length() {return file_length_;}
    int8_t  mode() {return mode_;}
    const vector<uint64_t> &dataserver_id() const {return dataserver_id_;}
private:
    int64_t file_id_;
    int64_t block_id_;
    int32_t block_version_;
    int64_t offset_;
    int64_t block_offset_;
    int64_t lseek_offset_;
    int64_t length_;
    int64_t file_length_;
    int8_t  mode_;
    vector<uint64_t> dataserver_id_;

    DISALLOW_COPY_AND_ASSIGN(CurrentBlock);
};//end of class Currentblock

}//end of namespace client
}//end of namespace bladestore
#endif
