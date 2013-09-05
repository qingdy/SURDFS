/*
 * Copyright (c) 2012, Sohu R&D
 * All rights reserved.
 * 
 * File   name: lease_checker.h
 * Description: This file defines the class LeaseChecker.
 * 
 * Version : 1.0
 * Author  : guili
 * Date    : 2012-4-24
 */
#ifndef BLADESTORE_CLIENT_CLIENT_LEASE_CHECKER_H
#define BLADESTORE_CLIENT_CLIENT_LEASE_CHECKER_H

#include <ctime>
#include "cthread.h"
#include "blade_common_define.h"

using pandora::CThread;
using pandora::Runnable;

namespace bladestore
{
namespace client
{

class ClientImpl;

class LeaseChecker : public Runnable
{
public:
    LeaseChecker(ClientImpl* client);
    ~LeaseChecker();

    int32_t Start();
    void Run(CThread *thread, void *arg);
    pthread_t GetThreadId() const;
    bool Renew();
    bool UpdateLease(int64_t block_id);

private:
    int64_t block_id_;
    time_t last_renewed_;
    CThread lease_check_;
    ClientImpl* client_;

    DISALLOW_COPY_AND_ASSIGN(LeaseChecker);
};

}  // end of namespace client
}  // end of namespace bladestore
#endif
