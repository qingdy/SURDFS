/**
 * Copyright (c) 2011��Sohu R&D
 * All rights reserved.
 *
 * File name: atomic-inl.h
 * Description: interget atomic operate
 * Date: 2012-2-2
 */

#ifndef __ATOMIC_H__
#define __ATOMIC_H__

#include <stdint.h>
#include "atomic-inl.h"

namespace pandora {

__inline__ int32_t AtomicGet(atomic_t *v);
__inline__ int64_t AtomicGet64(atomic64_t *v);
__inline__ void AtomicSet(atomic_t *v, int32_t i);
__inline__ void AtomicSet64(atomic64_t *v, int64_t i);
__inline__ int32_t AtomicAdd(atomic_t *v, int32_t i);
__inline__ int64_t AtomicAdd64(atomic64_t *v, int64_t i);
__inline__ int32_t AtomicSub(atomic_t *v, int32_t i);
__inline__ int64_t AtomicSub64(atomic64_t *v, int64_t i);
__inline__ void AtomicInc(atomic_t *v);
__inline__ void AtomicInc64(atomic64_t *v);
__inline__ void AtomicDec(atomic_t *v);
__inline__ void AtomicDec64(atomic64_t *v);
__inline__ int32_t AtomicExchange(atomic_t *v, int32_t i);
__inline__ int64_t AtomicExchange64(atomic64_t *v, int64_t i);

}
#endif
