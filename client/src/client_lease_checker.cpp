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

#include "client_lease_checker.h"
#include "client_impl.h"
#include "bladestore_ops.h"
#include "blade_common_define.h"
#include "time_wrap.h"
#include "log.h"

namespace bladestore
{
namespace client
{
LeaseChecker::LeaseChecker(ClientImpl* client) : block_id_(0), last_renewed_(0)
{
    client_ = client;
}

LeaseChecker::~LeaseChecker()
{
    client_ = NULL;
}

int32_t LeaseChecker::Start()
{
    return lease_check_.Start(this, NULL);
}

void LeaseChecker::Run(CThread *thread, void *arg)
{
    Time sleepval(CLIENT_LEASE_CHECK_SLEEP_USECS);
    while(1) {
        if (block_id_ <= 0){
            lease_check_.Ssleep(sleepval);
            continue;
        }

        LOGV(LL_INFO, "LeaseChecker,Run,block_id: %ld.", block_id_);
        if (time(NULL) - last_renewed_ > (CLIENT_LEASE_SOFTLIMIT_PERIOD / 2)) {
        //if (time(NULL) - last_renewed_ > (4 / 2)) {
            LOGV(LL_DEBUG, "time(NULL) - last_renewed_: %ld.", time(NULL) - last_renewed_);
            LOGV(LL_DEBUG, "Last_renewed: %ld.", last_renewed_);
            if (Renew()) {
                last_renewed_ = time(NULL);
            } else {
                LOGV(LL_ERROR, "RenewLease failed.");
            }
        }

        lease_check_.Ssleep(sleepval);
    }
}

pthread_t LeaseChecker::GetThreadId() const
{
    return lease_check_.GetThreadId();
}

bool LeaseChecker::Renew()
{
    if(RET_SUCCESS == client_->RenewLease(block_id_)){
        return true;
    } else {
        return false;
    }
}

bool LeaseChecker::UpdateLease(int64_t block_id)
{
    block_id_ = block_id;
    LOGV(LL_INFO, "LeaseChecker:set_lease_of_block_id: %ld.", block_id_);
    LOGV(LL_DEBUG, "Before:Last_renewed: %ld.", last_renewed_);
    last_renewed_ = time(NULL);
    LOGV(LL_DEBUG, "After:Last_renewed: %ld.", last_renewed_);
    return true;
}
}
}
