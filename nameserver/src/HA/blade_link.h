/*
 *version : 1.0
 *author  : chen yunyun
 *date    : 2012-5-15
 *
 */
#ifndef BLADESTORE_HA_LINK_H
#define BLADESTORE_HA_LINK_H

#include <stddef.h>

#include "blade_common_define.h"

using namespace bladestore::common;

namespace bladestore
{
namespace ha 
{

class BladeSLink
{
public:
	inline BladeSLink()
	{
		Initialize();
	}

	virtual ~BladeSLink()
	{
		next_ = NULL;
	}

	inline bool IsEmpty()
	{
		return next_ == NULL;
	}

	inline BladeSLink * Next()
	{
		return next_;
	}

	inline BladeSLink * ExtractNext()
	{
		BladeSLink *result = next_;
		if (result != NULL)
		{
			next_ = result->next_;
        }
        return result;
	}

private:
	inline void Initialize()
	{
		next_ = NULL;
	}

	BladeSLink * next_;
};

/// @class  double link list item
class BladeDLink
{
public:
	inline BladeDLink()
	{
		Initialize();
	}


	inline bool IsEmpty()
	{
		if(NULL == next_ || NULL == prev_)
		{
			LOGV(LL_ERROR, "dlink item corrupt [link_addr:%p,next:%p,prev:%p]", this, next_, prev_);
			return true;
		}
		return next_ == this;
	}

	inline void InsertNext(BladeDLink & link)
	{
		/// next_和prev_是私有变量，实现的是循环双向链表，因此只有在内存越界的情况下才为空
        /// 所以这里只打印了fatal日志，没有返回错误，其实应该打印fatal，然后core掉
        if (NULL == next_  || NULL == prev_ )
        {
          LOGV(LL_ERROR, "dlink item corrupt [link_addr:%p,next:%p,prev:%p]", this, next_, prev_);
        }
        else
        {
          link.next_ = next_;
          link.prev_ = this;
          next_->prev_ = &link;
          next_ = &link;
		}
	}

	inline void InsertPrev(BladeDLink & link)
	{
		if (NULL == next_ || NULL == prev_)
        {
			LOGV(LL_ERROR, "dlink item corrupt [link_addr:%p,next:%p,prev:%p]", this, next_, prev_);
        }
        else
        {
          link.prev_ = prev_;
          link.next_ = this;
          prev_->next_ = &link;
          prev_ = &link;
		}
	}

	inline void Remove()
	{
		if (NULL == next_ || NULL == prev_)
		{
			LOGV(LL_ERROR, "dlink item corrupt [link_addr:%p,next:%p,prev:%p]", this, next_, prev_);
        }
        else
        {
			prev_->next_ = next_;
			next_->prev_ = prev_;
			Initialize();
        }
	}

	inline BladeDLink * ExtractNext()
	{
		BladeDLink *result = NULL;
		if (!IsEmpty())
		{
			result = next_;
			result->Remove();
		}
		return result;
	}

	inline BladeDLink * ExtractPrev()
	{
		BladeDLink *result = NULL;
		if (!IsEmpty())
		{
			result = prev_;
			result->Remove();
		}
		return result;
	}

	inline void Replace(BladeDLink &link)
	{
		if (NULL == next_ || NULL == prev_)
		{
			LOGV(LL_ERROR, "dlink item corrupt [link_addr:%p,next:%p,prev:%p]", this, next_, prev_);
        }
        else
        {
			next_->prev_ = &link;
			prev_->next_ = &link;
			link.prev_ = prev_;
			link.next_ = next_;
			Initialize();
        }
	}

	inline BladeDLink *Next()
	{
		return next_;
	}

	inline BladeDLink *Prev()
	{
		return prev_;
	}

private:
	inline void Initialize()
	{
		next_ = this;
		prev_ = this;
	}

	BladeDLink * next_;
	BladeDLink * prev_;
};

/// @def get the object of type "type" that contains the field 
/// "field" stating in address "address"
#ifndef CONTAINING_RECORD
#define CONTAINING_RECORD(address, type, field) ((type *)( 				            \
                                                 (char*)(address) - 			      \
                                                 (long)(&((type *)1)->field) + 1))
#endif

}//end of namespace ha
}//end of namespace nameserver
#endif

