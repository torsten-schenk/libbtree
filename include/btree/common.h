#ifndef _BTREE_COMMON_H
#define _BTREE_COMMON_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif


enum {
	BTREE_OPT_DEFAULT = 0x00000000,
	BTREE_OPT_KEEP_NODES = 0x00000001, /* don't free unused nodes, keep old ones and reuse them before allocating new nodes */
	BTREE_OPT_MULTI_KEY = 0x00000002, /* allow same key multiple times. iteration/index order is same as insertion order */
	BTREE_OPT_ALLOW_INDEX = 0x00000004, /* allow using an index for insertions/replacements (i.e. insert_at and put_at methods) while still using a compare function. however, a check will be performed whether the insertion/replacement is allowed at that position */
	BTREE_OPT_USE_SUBELEMENTS = 0x00000008, /* each element contains an array of subelements. requires size and subelement hooks */
	BTREE_OPT_INSERT_LOWER = 0x00000010, /* required BTREE_OPT_MULTI_KEY; insert new elements at lower end of the group */

	BTREE_OPT_RESERVED = 0xff000000 /* those are used internally (see btree.c) */
};

enum {
	BTREE_RDONLY = 0x00000001
};

#ifdef __cplusplus
}
#endif

#endif

