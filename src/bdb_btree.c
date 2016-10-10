#include <stdbool.h>
#include <assert.h>
#include <stdlib.h>
#include <errno.h>

#include <btree/common.h>
#include <btree/bdb.h>

#include "bdb_common.h"

#define BTREE_MAGIC 0x9a91bcd0
#define BTREE_VERSION 1

#ifndef MIN
#define MIN(X, Y) ((X) < (Y) ? (X) : (Y))
#endif

/* TODO on failure, one can abort a DB_TXN transaction. This also must be reflected here:
 * it must be possible to revert all state to a previous one (i.e. by reloading REC_HEADER and
 * clearing the cache). */

#define MIN_CACHE_BUFFERS 3
#define DEFAULT_CACHE_BUFFERS 25
#define DEFRAG_BLOCK_SIZE 256 /* maximum number of 'record holes' to fill in a single iteration */
/* TODO remove and use tree->cache_buffers */
#define REC_BUFFERS 10 /* MUST be >= 3 as some functions require 3 parallel nodes */

/* RESOURCE: http://www.scholarpedia.org/article/B-tree_and_UB-tree */

/* IDEA: cache uppermost nodes permanently by simply incrementing refcnt after first retrieval/storage
 * Make the amount of levels cached dependent on 'cache_buffers' (and of course 'order) */

/* TODO DUMMY */
static void print_int(
		const void *a_)
{
	uint32_t a = *(uint32_t*)a_;
	printf("%d", a);
}

enum {
	OPT_NOCMP = 0x01000000, /* no compare function given, use indices only */
	OPT_USE_DATA = 0x10000000, /* use the 'data' versions of the callback functions */
	OPTMASK_PERMANENT = 0x0fffffff
};

enum {
	REC_NULL = 0,
	REC_HEADER = 1
};

/* E: pointer to element */
#define ACQUIRE_E(TREE, E) \
	do { \
		int ret = 0; \
		if(!(TREE->options & OPT_USE_DATA) && TREE->cb.data.acquire != NULL) \
			ret = TREE->cb.nodata.acquire(E); \
		else if((TREE->options & OPT_USE_DATA) && TREE->cb.nodata.acquire != NULL) \
			ret = TREE->cb.data.acquire(E, TREE->data); \
		if(ret != 0) \
			return ret; \
	} while(false)

#define RELEASE_E(TREE, E) \
	do { \
		if(!(TREE->options & OPT_USE_DATA) && TREE->cb.data.release != NULL) \
			TREE->cb.nodata.release(E); \
		else if((TREE->options & OPT_USE_DATA) && TREE->cb.nodata.release != NULL) \
			TREE->cb.data.release(E, TREE->data); \
	} while(false)

#define CMP_E(TREE, A, B) \
	((TREE->options & OPT_USE_DATA) ? TREE->cb.data.cmp(A, B, TREE->data) : TREE->cb.nodata.cmp(A, B))

#define FIND_LOWER_IMPL(CMP) \
	int u; \
	int l; \
	int m; \
	int cmp; \
	db_recno_t node_candidate = REC_NULL; \
	int pos_candidate = 0; \
	cache_t *cur_cache; \
	bool found = false; \
	db_recno_t cur = tree->root; \
	db_recno_t prev = REC_NULL; \
	uint32_t prev_fill; \
\
	while(cur != REC_NULL) { \
		cur_cache = cache_get(tree, txn, cur); \
		if(cur_cache == NULL) \
			return false; /* errno already set */ \
		u = buffer_get_u32(cur_cache->buffer, OFF_NODE_FILL) - 1; \
		l = 0; \
		prev = cur; \
		prev_fill = u + 1; \
		while(l <= u) { \
			m = l + (u - l) / 2; \
			cmp = CMP; \
			if(cmp >= 0) { \
				node_candidate = cur; \
				pos_candidate = m; \
				u = m - 1; \
				if(cmp == 0) \
					found = true; \
			} \
			else \
				l = m + 1; \
		} \
		cur = buffer_get_rec(cur_cache->buffer, OFF_NODE_LINK(tree, l) + OFF_NODE_LINK_CHILD); \
	} \
\
	if(node_candidate == REC_NULL && prev != REC_NULL) { /* all element keys less than requested key, select imaginary element after end (rightmost leaf node) */ \
		node_candidate = prev; \
		pos_candidate = prev_fill; \
	} \
	if(node != NULL) \
		*node = node_candidate; \
	if(pos != NULL) \
		*pos = pos_candidate; \
	errno = 0; \
	return found;

#define FIND_UPPER_IMPL(CMP) \
	int u; \
	int l; \
	int m; \
	int cmp; \
	db_recno_t node_candidate = REC_NULL; \
	int pos_candidate = 0; \
	cache_t *cur_cache; \
	bool found = false; \
	db_recno_t cur = tree->root; \
	db_recno_t prev = REC_NULL; \
	uint32_t prev_fill; \
\
	while(cur != REC_NULL) { \
		cur_cache = cache_get(tree, txn, cur); \
		if(cur_cache == 0) \
			return false; /* errno already set */ \
		u = buffer_get_u32(cur_cache->buffer, OFF_NODE_FILL) - 1; \
		l = 0; \
		prev = cur; \
		prev_fill = u + 1; \
		while(l <= u) { \
			m = l + (u - l) / 2; \
			cmp = CMP; \
			if(cmp > 0) { \
				node_candidate = cur; \
				pos_candidate = m; \
				u = m - 1; \
			} \
			else { \
				if(cmp == 0) \
					found = true; \
				l = m + 1; \
			} \
		} \
		cur = buffer_get_rec(cur_cache->buffer, OFF_NODE_LINK(tree, l) + OFF_NODE_LINK_CHILD); \
	} \
\
	if(node_candidate == REC_NULL && prev != REC_NULL) { /* all element keys less than requested key, select imaginary element after end (rightmost leaf node) */ \
		node_candidate = prev; \
		pos_candidate = prev_fill; \
	} \
	if(node != NULL) \
		*node = node_candidate; \
	if(pos != NULL) \
		*pos = pos_candidate; \
	errno = 0; \
	return found;

#define API_FIND_LIM_IMPL(FIND_FN) \
	db_recno_t node; \
	cache_t *node_cache; \
	int node_fill; \
	int pos; \
	int index; \
\
	if(self->options & OPT_NOCMP) \
		return -EINVAL; \
\
	FIND_FN; \
	if(errno != 0) \
		return cache_cleanup(self, txn, errno); \
	index = to_index(self, txn, node, pos); \
	if(index < 0) \
		return cache_cleanup(self, txn, index); \
	if(it != NULL) { \
		memset(it, 0, sizeof(*it)); \
		it->tree = self; \
		it->pos = pos; \
		it->node = node; \
		it->index = index; \
		if(node == REC_NULL) \
			it->element = NULL; \
		else { \
			node_cache = cache_get(self, txn, node); \
			if(node_cache == NULL) \
				return cache_cleanup(self, txn, errno); \
			node_fill = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL); \
			if(pos == node_fill) \
				it->element = NULL; \
			else \
				it->element = node_cache->buffer + OFF_NODE_ELEMENT(self, pos); \
		} \
	} \
	return cache_cleanup(self, txn, index);

enum {
	OFF_HEADER_MAGIC = 0,
	OFF_HEADER_VERSION = OFF_HEADER_MAGIC + SIZE_U32,
	OFF_HEADER_ORDER = OFF_HEADER_VERSION + SIZE_U32,
	OFF_HEADER_ELEMENT_SIZE = OFF_HEADER_ORDER + SIZE_U32,
	OFF_HEADER_OPTIONS = OFF_HEADER_ELEMENT_SIZE + SIZE_U32,
	OFF_HEADER_ROOT = OFF_HEADER_OPTIONS + SIZE_U32,
	OFF_HEADER_FREE_LIST = OFF_HEADER_ROOT + SIZE_REC,
	OFF_HEADER_MAX_RECNO = OFF_HEADER_FREE_LIST + SIZE_U32,
	SIZE_HEADER = OFF_HEADER_MAX_RECNO + SIZE_REC
};

enum {
	OFF_NODE_PARENT = 0,
	OFF_NODE_FILL = OFF_NODE_PARENT + SIZE_REC,
	SIZE_NODE_HEADER = OFF_NODE_FILL + SIZE_U32
};

enum {
	OFF_FREE_MARKER = OFF_NODE_PARENT, /* set this field to REC_HEADER; it is at the same offset as the node's parent and the header record cannot be the parent of a btree node */
	OFF_FREE_NEXT = OFF_FREE_MARKER + SIZE_REC,
	OFF_FREE_PREV = OFF_FREE_NEXT + SIZE_REC,
	SIZE_FREE = OFF_FREE_PREV + SIZE_REC
};

enum {
	OFF_NODE_LINK_OFFSET = 0,
	OFF_NODE_LINK_COUNT = OFF_NODE_LINK_OFFSET + SIZE_U32,
	OFF_NODE_LINK_CHILD = OFF_NODE_LINK_COUNT + SIZE_U32,
	SIZE_NODE_LINK = OFF_NODE_LINK_CHILD + SIZE_REC
};

/* child index map on parent node
 * left-padded with REC_NULL */
enum {
	OFF_NODE_CIMAP_CHILD = 0,
	OFF_NODE_CIMAP_INDEX = OFF_NODE_CIMAP_CHILD + SIZE_REC,
	SIZE_NODE_CIMAP = OFF_NODE_CIMAP_INDEX + SIZE_U32
};

#define OFF_NODE_ELEMENT(TREE, INDEX) (SIZE_NODE_HEADER + (INDEX) * TREE->element_size)
#define OFF_NODE_LINK(TREE, INDEX) (SIZE_NODE_HEADER + (TREE->order - 1) * TREE->element_size + (INDEX) * SIZE_NODE_LINK)
#define OFF_NODE_CIMAP(TREE, INDEX) (SIZE_NODE_HEADER + (TREE->order - 1) * TREE->element_size + TREE->order * SIZE_NODE_LINK + (INDEX) * SIZE_NODE_CIMAP)
 /* [parent, child_cache, fill], [elements], [links: count, offset, child] */
//#define NODE_SIZE(ORDER, ELEMENT_SIZE) (3 * SIZE_U32 + ((ORDER) - 1) * (ELEMENT_SIZE) + (ORDER) * 3 * SIZE_U32)
#define NODE_SIZE(TREE) (SIZE_NODE_HEADER + (TREE->order - 1) * TREE->element_size + TREE->order * SIZE_NODE_LINK + TREE->order * SIZE_NODE_CIMAP)
#define NODE_SIZE2(ORDER, ELEMENT_SIZE) (SIZE_NODE_HEADER + ((ORDER) - 1) * (ELEMENT_SIZE) + (ORDER) * SIZE_NODE_LINK + (ORDER) * SIZE_NODE_CIMAP)


typedef struct {
	uint32_t offset;
	uint32_t count;
	db_recno_t child;
} link_t;

typedef struct {
	char *buffer;
	db_recno_t recno;
	bool modified;
	int refcnt;
} cache_t;

/* TODO locking if parallel access is required */
struct bdb_btree {
	int order;
	bufsize_t element_size;
	uint32_t options;
	union {
		struct {
			int (*cmp)(const void *a, const void *b);
			int (*acquire)(void *a);
			void (*release)(void *a);
		} nodata;
		struct {
			int (*cmp)(const void *a, const void *b, void *data);
			int (*acquire)(void *a, void *data);
			void (*release)(void *a, void *data);
		} data;
	} cb;
	void *data;
	db_recno_t root;
	db_recno_t free_list;
	db_recno_t max_recno;
	/* TODO add field: 'size' (number of elements in tree) and return this value in bdb_btree_size() */

	DB *db;
	cache_t cache[REC_BUFFERS];

	db_recno_t overflow_node;
	void *overflow_element;
	char overflow_link[SIZE_NODE_LINK];
};

static void dump_node(
		int indent,
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t node,
		void (*print)(const void *element));

static void dump_tree(
		bdb_btree_t *tree,
		DB_TXN *txn,
		void (*print)(const void *element));

static void dump_nodecache(
		bdb_btree_t *tree,
		cache_t *cache)
{
	uint32_t i;
	uint32_t fill = buffer_get_u32(cache->buffer, OFF_NODE_FILL);

	printf("NODE CACHE DUMP: recno=%d, fill=%d, parent=%d\n", cache->recno, fill, buffer_get_rec(cache->buffer, OFF_NODE_PARENT));
	printf("-> links:\n");
	for(i = 0; i < tree->order; i++)
		printf("  %d: child=%d, offset=%d, count=%d\n", i, buffer_get_rec(cache->buffer, OFF_NODE_LINK(tree, i) + OFF_NODE_LINK_CHILD), buffer_get_u32(cache->buffer, OFF_NODE_LINK(tree, i) + OFF_NODE_LINK_OFFSET), buffer_get_u32(cache->buffer, OFF_NODE_LINK(tree, i) + OFF_NODE_LINK_COUNT));
	printf("-> cimap:\n");
	for(i = 0; i < tree->order; i++)
		printf("  %d: child=%d, index=%d\n", i, buffer_get_rec(cache->buffer, OFF_NODE_CIMAP(tree, i) + OFF_NODE_CIMAP_CHILD), buffer_get_u32(cache->buffer, OFF_NODE_CIMAP(tree, i) + OFF_NODE_CIMAP_INDEX));
}

static void dump_cache(
		bdb_btree_t *tree,
		cache_t *cache);

static int update_header(
		bdb_btree_t *tree,
		DB_TXN *txn);

/* error: return NULL and set errno */
static cache_t *cache_unused(
		bdb_btree_t *tree,
		DB_TXN *txn)
{
	int i;
	cache_t *first_free = NULL;
	cache_t *first_free_unmodified = NULL;
	cache_t *cache;
	int ret;

	for(i = 0; i < REC_BUFFERS; i++) {
		cache = tree->cache + i;
		if(cache->recno == REC_NULL)
			return cache;
		else if(cache->refcnt == 0) {
			if(cache->modified)
				first_free = cache;
			else
				first_free_unmodified = cache;
		}
	}
	if(first_free_unmodified != NULL)
		cache = first_free_unmodified;
	else if(first_free != NULL)
		cache = first_free;
	else {
		errno = -EOVERFLOW;
		return NULL;
	}

	if(cache->modified) {
		DBT dbt_key;
		DBT dbt_data;

		memset(&dbt_key, 0, sizeof(dbt_key));
		memset(&dbt_data, 0, sizeof(dbt_data));
		dbt_key.data = &cache->recno;
		dbt_key.size = SIZE_REC;
		dbt_data.data = cache->buffer;
		dbt_data.size = NODE_SIZE(tree);
		ret = tree->db->put(tree->db, txn, &dbt_key, &dbt_data, 0);
		if(ret != 0) {
			errno = ret;
			return NULL;
		}
		cache->modified = false;
	}
	cache->recno = REC_NULL;
	return cache;
}

/* stores buffer into db if the modified flag is set on the resulting buffer
 * returns index in rec_buffer[]; NULL and errno in case of error */
static cache_t *cache_get(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t recno)
{
	int ret;
	DBT dbt_key;
	DBT dbt_data;
	int i;
	cache_t *cache;

	for(i = 0; i < REC_BUFFERS; i++)
		if(tree->cache[i].recno == recno)
			return tree->cache + i;
	cache = cache_unused(tree, txn);
	if(cache == NULL) /* errno already set */
		return NULL;

	memset(&dbt_key, 0, sizeof(dbt_key));
	memset(&dbt_data, 0, sizeof(dbt_data));
	dbt_key.data = &recno;
	dbt_key.size = SIZE_REC;
	dbt_data.data = cache->buffer;
	dbt_data.ulen = NODE_SIZE(tree);
	dbt_data.flags = DB_DBT_USERMEM;

	ret = tree->db->get(tree->db, txn, &dbt_key, &dbt_data, 0);
	if(ret != 0) {
		errno = ret;
		return NULL;
	}
	/* TODO check whether loaded node is part of the free list and not an actual node; return DB_NOTFOUND in that case */
	cache->recno = recno;
	assert(cache->refcnt == 0);

	return cache;
}

static void cache_free(
		cache_t *cache)
{
	cache->refcnt = 0;
	cache->modified = false;
	cache->recno = 0;
}

/* returns recno of allocated node or REC_NULL and errno in case of error.
 * NOTE consumes a tree->rec_buffer */
static db_recno_t alloc_node(
		bdb_btree_t *tree,
		DB_TXN *txn)
{
	int ret;
	cache_t *cache;
	db_recno_t recno;
	db_recno_t second;
	DBT dbt_key;
	DBT dbt_data;

	if(tree->free_list == REC_NULL) {
		cache = cache_unused(tree, txn);
		if(cache < 0) /* errno already set */
			return REC_NULL;

		memset(&dbt_key, 0, sizeof(dbt_key));
		memset(&dbt_data, 0, sizeof(dbt_data));
		dbt_key.data = &recno;
		dbt_key.size = SIZE_REC;
		dbt_key.ulen = sizeof(recno);
		dbt_key.flags = DB_DBT_USERMEM;
		dbt_data.data = cache->buffer;
		dbt_data.size = NODE_SIZE(tree);
		dbt_data.ulen = NODE_SIZE(tree);
		dbt_data.flags = DB_DBT_USERMEM;
		buffer_fill(cache->buffer, 0, 0, NODE_SIZE(tree));
		ret = tree->db->put(tree->db, txn, &dbt_key, &dbt_data, DB_APPEND);
		if(ret != 0) {
			errno = ret;
			return REC_NULL;
		}
		tree->max_recno = recno;
		cache->recno = recno;
	}
	else {
		/* get first record in free list */
		recno = tree->free_list;
		cache = cache_get(tree, txn, tree->free_list);
		if(cache == NULL)
			return REC_NULL;
		assert(buffer_get_rec(cache->buffer, OFF_FREE_MARKER) == REC_HEADER);
		tree->free_list = buffer_get_rec(cache->buffer, OFF_FREE_NEXT);
		second = buffer_get_rec(cache->buffer, OFF_FREE_NEXT); /* store second element in free list for later usage */
		buffer_fill(cache->buffer, 0, 0, NODE_SIZE(tree));
		cache->modified = true;

		/* get second element in free ist */
		if(second != REC_NULL) {
			cache = cache_get(tree, txn, second);
			if(cache == NULL)
				return REC_NULL;
			buffer_set_rec(cache->buffer, OFF_FREE_PREV, REC_NULL);
			cache->modified = true;
		}
	}

	ret = update_header(tree, txn);
	if(ret != 0) {
		errno = ret;
		return REC_NULL;
	}
	return recno;
}

/* returns error code */
static int free_node(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t node)
{
	cache_t *node_cache;

	if(tree->free_list != REC_NULL) {
		node_cache = cache_get(tree, txn, tree->free_list);
		if(node_cache == NULL)
			return errno;
		buffer_set_rec(node_cache->buffer, OFF_FREE_PREV, node);
		node_cache->modified = true;
	}
	node_cache = cache_get(tree, txn, node);
	if(node_cache == NULL)
		return errno;
	buffer_fill(node_cache->buffer, 0, 0, NODE_SIZE(tree));
	buffer_set_rec(node_cache->buffer, OFF_FREE_MARKER, REC_HEADER);
	buffer_set_rec(node_cache->buffer, OFF_FREE_NEXT, tree->free_list);
	buffer_set_rec(node_cache->buffer, OFF_FREE_PREV, REC_NULL);
	node_cache->modified = true;
	tree->free_list = node;
	return update_header(tree, txn);
}

static int erase_node(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t node)
{
	DBT dbt_key;

	memset(&dbt_key, 0, sizeof(dbt_key));
	dbt_key.data = &node;
	dbt_key.size = SIZE_REC;
	return tree->db->del(tree->db, txn, &dbt_key, 0);
}

/* flush all nodes which are currently buffered
 * if 'ret' is != 0, don't store the nodes, just clear the buffers */
static int cache_flush(
		bdb_btree_t *tree,
		DB_TXN *txn)
{
	int i;
	DBT dbt_key;
	DBT dbt_data;
	int ret = 0;

	memset(&dbt_key, 0, sizeof(dbt_key));
	memset(&dbt_data, 0, sizeof(dbt_data));
	dbt_key.size = SIZE_REC;
	dbt_data.size = NODE_SIZE(tree);

	for(i = 0; i < REC_BUFFERS; i++) {
		if(tree->cache[i].modified) {
			assert(tree->cache[i].recno != REC_NULL);
			if(ret == 0) { /* if at least one error occured, clear all other nodes but don't try to store them back to the db */
				dbt_key.data = &tree->cache[i].recno;
				dbt_data.data = tree->cache[i].buffer;
				ret = tree->db->put(tree->db, txn, &dbt_key, &dbt_data, 0);
			}
		}
		tree->cache[i].refcnt = 0;
		tree->cache[i].recno = REC_NULL;
		tree->cache[i].modified = false;
	}
	return ret;
}

/* CAUTION: _find_X methods depend on the cache being accessible further!
 * TODO serial numbers for iterators? */
static int cache_cleanup(
		bdb_btree_t *tree,
		DB_TXN *txn,
		int ret)
{
	int i;
	for(i = 0; i < REC_BUFFERS; i++) {
		assert(tree->cache[i].refcnt == 0);
	}
	return ret;
}

/* returns error code */
static int update_header(
		bdb_btree_t *tree,
		DB_TXN *txn)
{
	cache_t *cache;

	cache = cache_get(tree, txn, REC_HEADER);
	if(cache == NULL)
		return errno;
	cache->modified = true;
	buffer_set_u32(cache->buffer, OFF_HEADER_MAGIC, BTREE_MAGIC); /* see also _create() */
	buffer_set_u32(cache->buffer, OFF_HEADER_VERSION, BTREE_VERSION); /* TODO which version to write? always BTREE_VERSION or the version already found in the file? */
	buffer_set_u32(cache->buffer, OFF_HEADER_ORDER, tree->order);
	buffer_set_u32(cache->buffer, OFF_HEADER_ELEMENT_SIZE, tree->element_size);
	buffer_set_u32(cache->buffer, OFF_HEADER_OPTIONS, tree->options & OPTMASK_PERMANENT);
	buffer_set_rec(cache->buffer, OFF_HEADER_ROOT, tree->root);
	buffer_set_rec(cache->buffer, OFF_HEADER_FREE_LIST, tree->free_list);
	buffer_set_rec(cache->buffer, OFF_HEADER_MAX_RECNO, tree->max_recno);
	return 0;
}

static void cimap_remove(
		bdb_btree_t *tree,
		cache_t *parent_cache,
		db_recno_t child)
{
	int l;
	int u;
	int m;
	db_recno_t cur;

	l = 0;
	u = tree->order - 1;
	while(l <= u) {
		m = l + (u - l) / 2;
		cur = buffer_get_rec(parent_cache->buffer, OFF_NODE_CIMAP(tree, m) + OFF_NODE_CIMAP_CHILD);
		if(cur == child) {
			buffer_move_internal(parent_cache->buffer, OFF_NODE_CIMAP(tree, 1), OFF_NODE_CIMAP(tree, 0), SIZE_NODE_CIMAP * m);
			buffer_fill(parent_cache->buffer, OFF_NODE_CIMAP(tree, 0), 0, SIZE_NODE_CIMAP);
			parent_cache->modified = true;
			return;
		}
		else if(cur > child)
			u = m - 1;
		else
			l = m + 1;
	}
}

static void cimap_put(
		bdb_btree_t *tree,
		cache_t *parent_cache,
		db_recno_t child,
		uint32_t index)
{
	int l;
	int u;
	int m;
	db_recno_t cur;

	if(child == REC_NULL)
		return;
	l = 0;
	u = tree->order - 1;
	while(l <= u) {
		m = l + (u - l) / 2;
		cur = buffer_get_rec(parent_cache->buffer, OFF_NODE_CIMAP(tree, m) + OFF_NODE_CIMAP_CHILD);
		if(cur == child) {
			buffer_set_u32(parent_cache->buffer, OFF_NODE_CIMAP(tree, m) + OFF_NODE_CIMAP_INDEX, index);
			parent_cache->modified = true;
			return;
		}
		else if(cur > child)
			u = m - 1;
		else
			l = m + 1;
	}
	/* insert child at 'u' (note: cimap is right-aligned) */
	assert(buffer_get_rec(parent_cache->buffer, OFF_NODE_CIMAP(tree, 0) + OFF_NODE_CIMAP_CHILD) == REC_NULL);
	buffer_move_internal(parent_cache->buffer, OFF_NODE_CIMAP(tree, 0), OFF_NODE_CIMAP(tree, 1), SIZE_NODE_CIMAP * u);
	buffer_set_rec(parent_cache->buffer, OFF_NODE_CIMAP(tree, u) + OFF_NODE_CIMAP_CHILD, child);
	buffer_set_u32(parent_cache->buffer, OFF_NODE_CIMAP(tree, u) + OFF_NODE_CIMAP_INDEX, index);
	parent_cache->modified = true;
}

static uint32_t cimap_get(
		bdb_btree_t *tree,
		cache_t *parent_cache,
		db_recno_t child)
{
	int l;
	int u;
	int m;
	db_recno_t cur = REC_NULL;

	if(parent_cache == NULL || child == REC_NULL)
		return 0;

	l = tree->order - buffer_get_u32(parent_cache->buffer, OFF_NODE_FILL) - 1;
	u = tree->order - 1;
	while(l <= u) {
		m = l + (u - l) / 2;
		cur = buffer_get_rec(parent_cache->buffer, OFF_NODE_CIMAP(tree, m) + OFF_NODE_CIMAP_CHILD);
		if(cur == child)
			return buffer_get_u32(parent_cache->buffer, OFF_NODE_CIMAP(tree, m) + OFF_NODE_CIMAP_INDEX);
		else if(cur > child)
			u = m - 1;
		else
			l = m + 1;
	}
	assert(false);
	return tree->order;
}

/* increment child index by 1 for all children having an index >= lower */
static void cimap_inc(
		bdb_btree_t *tree,
		cache_t *parent_cache,
		uint32_t lower)
{
	int i;
	int l;
	uint32_t index;
	db_recno_t child;

	l = tree->order - buffer_get_u32(parent_cache->buffer, OFF_NODE_FILL) - 1;
	for(i = l; i < tree->order; i++) {
		child = buffer_get_rec(parent_cache->buffer, OFF_NODE_CIMAP(tree, i) + OFF_NODE_CIMAP_CHILD);
		index = buffer_get_u32(parent_cache->buffer, OFF_NODE_CIMAP(tree, i) + OFF_NODE_CIMAP_INDEX);
		if(child != REC_NULL && index >= lower)
			buffer_add_u32(parent_cache->buffer, OFF_NODE_CIMAP(tree, i) + OFF_NODE_CIMAP_INDEX, 1);
	}
}

/* decrement child index by 1 for all children having an index >= lower */
static void cimap_dec(
		bdb_btree_t *tree,
		cache_t *parent_cache,
		uint32_t lower)
{
	int i;
	int l;
	uint32_t index;
	db_recno_t child;

	l = tree->order - buffer_get_u32(parent_cache->buffer, OFF_NODE_FILL) - 1;
	for(i = l; i < tree->order; i++) {
		child = buffer_get_rec(parent_cache->buffer, OFF_NODE_CIMAP(tree, i) + OFF_NODE_CIMAP_CHILD);
		index = buffer_get_u32(parent_cache->buffer, OFF_NODE_CIMAP(tree, i) + OFF_NODE_CIMAP_INDEX);
		if(child != REC_NULL && index >= lower)
			buffer_add_u32(parent_cache->buffer, OFF_NODE_CIMAP(tree, i) + OFF_NODE_CIMAP_INDEX, -1);
	}
}

static bool isleaf(
		bdb_btree_t *tree,
		cache_t *node_cache)
{
	return buffer_get_rec(node_cache->buffer, OFF_NODE_LINK(tree, 0) + OFF_NODE_LINK_CHILD) == REC_NULL;
}

/* error case: return REC_NULL and set errno */
static db_recno_t query_parent(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t node)
{
	cache_t *node_cache;

	if(node == tree->root || node == REC_NULL)
		return REC_NULL;

	node_cache = cache_get(tree, txn, node);
	if(node_cache == NULL) /* errno already set */
		return REC_NULL;
	return buffer_get_rec(node_cache->buffer, OFF_NODE_PARENT);
}

static inline bool near_overflowing(
		bdb_btree_t *tree,
		cache_t *node_cache)
{
	return buffer_get_u32(node_cache->buffer, OFF_NODE_FILL) == tree->order - 1;
}

static inline bool underflowing(
		bdb_btree_t *tree,
		cache_t *node_cache)
{
	return buffer_get_u32(node_cache->buffer, OFF_NODE_FILL) < tree->order / 2;
}

static inline bool near_underflowing(
		bdb_btree_t *tree,
		cache_t *node_cache)
{
	return buffer_get_u32(node_cache->buffer, OFF_NODE_FILL) == tree->order / 2;
}

static db_recno_t left_sibling(
		bdb_btree_t *tree,
		cache_t *parent_cache,
		cache_t *node_cache)
{
	uint32_t child_index = cimap_get(tree, parent_cache, node_cache->recno);

	if(parent_cache == NULL) /* root node */
		return REC_NULL;
	else if(child_index == 0) /* first child */
		return REC_NULL;
	else
		return buffer_get_rec(parent_cache->buffer, OFF_NODE_LINK(tree, child_index - 1) + OFF_NODE_LINK_CHILD);
}

static db_recno_t right_sibling(
		bdb_btree_t *tree,
		cache_t *parent_cache,
		cache_t *node_cache)
{
	uint32_t child_index = cimap_get(tree, parent_cache, node_cache->recno);
	uint32_t parent_fill = buffer_get_u32(parent_cache->buffer, OFF_NODE_FILL);

	if(parent_cache == NULL) /* root node */
		return REC_NULL;
	else if(child_index == parent_fill) /* last child */
		return REC_NULL;
	else
		return buffer_get_rec(parent_cache->buffer, OFF_NODE_LINK(tree, child_index + 1) + OFF_NODE_LINK_CHILD);
}

static void link_to_buffer(
		void *di,
		int doff,
		const link_t *link)
{
	buffer_set_u32(di, doff + OFF_NODE_LINK_OFFSET, link->offset);
	buffer_set_u32(di, doff + OFF_NODE_LINK_COUNT, link->count);
	buffer_set_rec(di, doff + OFF_NODE_LINK_CHILD, link->child);
}

static void buffer_to_link(
		link_t *link,
		const void *si,
		int soff)
{
	link->offset = buffer_get_u32(si, soff + OFF_NODE_LINK_OFFSET);
	link->count = buffer_get_u32(si, soff + OFF_NODE_LINK_COUNT);
	link->child = buffer_get_rec(si, soff + OFF_NODE_LINK_CHILD);
}

/* returns error code */
static int newroot(
		bdb_btree_t *tree,
		DB_TXN *txn)
{
	db_recno_t root;
	link_t link;
	
	root = alloc_node(tree, txn);
	if(root == REC_NULL)
		return errno;
	if(tree->root != REC_NULL) {
		cache_t *oldroot_cache;
		cache_t *root_cache;

		oldroot_cache = cache_get(tree, txn, tree->root);
		if(oldroot_cache == NULL)
			return errno;
		oldroot_cache->refcnt++;
		root_cache = cache_get(tree, txn, root);
		if(root_cache == NULL) {
			oldroot_cache->refcnt--;
			return errno;
		}

		buffer_set_rec(oldroot_cache->buffer, OFF_NODE_PARENT, root);
		cimap_put(tree, root_cache, tree->root, 0);
		buffer_to_link(&link, tree->overflow_link, 0);
		link.child = tree->root;

		buffer_set_rec(root_cache->buffer, OFF_NODE_LINK(tree, 0) + OFF_NODE_LINK_CHILD, tree->root);
		if(tree->overflow_node == tree->root)
			link.count += link.offset;
		else {
			uint32_t fill = buffer_get_u32(oldroot_cache->buffer, OFF_NODE_FILL);
			uint32_t roffset = buffer_get_rec(oldroot_cache->buffer, OFF_NODE_LINK(tree, fill) + OFF_NODE_LINK_OFFSET);
			uint32_t rcount = buffer_get_rec(oldroot_cache->buffer, OFF_NODE_LINK(tree, fill) + OFF_NODE_LINK_COUNT);
			link.count = roffset + rcount;
		}
		link.offset = 0;
		link_to_buffer(root_cache->buffer, OFF_NODE_LINK(tree, 0), &link);
		root_cache->modified = true;
		oldroot_cache->modified = true;
		oldroot_cache->refcnt--;
	}
	tree->root = root;
	update_header(tree, txn);
	return 0;
}

static int split(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t l)
{
	db_recno_t p;
	db_recno_t r;
	char *r_link_buffer;
	link_t r_link;
	uint32_t sidx = tree->order / 2;
	uint32_t i;
	uint32_t n;
	uint32_t l_child_index;
	uint32_t r_child_index;
	uint32_t l_fill;
	uint32_t r_fill;
	uint32_t p_fill;
	cache_t *l_cache;
	cache_t *r_cache;
	cache_t *p_cache;
	db_recno_t child = REC_NULL;
	cache_t *child_cache;

	assert(l == tree->overflow_node);
	assert(l != tree->root);

	l_cache = cache_get(tree, txn, l);
	if(l_cache == NULL)
		return errno;
	l_cache->refcnt++;
	l_fill = buffer_get_u32(l_cache->buffer, OFF_NODE_FILL);
	p = buffer_get_rec(l_cache->buffer, OFF_NODE_PARENT);
	assert(l_fill == tree->order - 1);

	p_cache = cache_get(tree, txn, p);
	if(p_cache == NULL) {
		l_cache->refcnt--;
		return errno;
	}
	p_cache->refcnt++;
	p_fill = buffer_get_u32(p_cache->buffer, OFF_NODE_FILL);
	l_child_index = cimap_get(tree, p_cache, l);

 	r = alloc_node(tree, txn);
	if(r == REC_NULL) {
		l_cache->refcnt--;
		p_cache->refcnt--;
		return errno;
	}
	r_cache = cache_get(tree, txn, r);
	if(r_cache == NULL) {
		l_cache->refcnt--;
		p_cache->refcnt--;
		return errno;
	}
	r_cache->refcnt++;
	r_fill = l_fill - sidx;
	r_child_index = l_child_index + 1;

	/* copy overflow data to back of right node */
	child = buffer_get_rec(tree->overflow_link, OFF_NODE_LINK_CHILD);
	cimap_put(tree, r_cache, child, r_fill); /* insert child into right node cimap */
	cimap_remove(tree, l_cache, child); /* remove child from left node cimap */
	buffer_move_external(r_cache->buffer, OFF_NODE_ELEMENT(tree, r_fill - 1), tree->overflow_element, 0, tree->element_size); /* move overflow element to last position of right node */
	buffer_move_external(r_cache->buffer, OFF_NODE_LINK(tree, r_fill), tree->overflow_link, 0, SIZE_NODE_LINK); /* move overflow link to last position of right node */

/* insert new right node into parent */
	if(r_child_index == tree->order) { /* new right node will be in overflow position */
		buffer_move_external(tree->overflow_element, 0, l_cache->buffer, OFF_NODE_ELEMENT(tree, sidx), tree->element_size); /* move single element from left node to overflow */
		r_link_buffer = tree->overflow_link;
		tree->overflow_node = p;
	}
	else {
		if(p_fill == tree->order - 1) { /* parent will overflow, move last element to overflow position */
			child = buffer_get_rec(p_cache->buffer, OFF_NODE_LINK(tree, p_fill) + OFF_NODE_LINK_CHILD);
			cimap_remove(tree, p_cache, child); /* remove overflowing child from cimap */
			buffer_move_external(tree->overflow_element, 0, p_cache->buffer, OFF_NODE_ELEMENT(tree, p_fill - 1), tree->element_size);
			buffer_move_external(tree->overflow_link, 0, p_cache->buffer, OFF_NODE_LINK(tree, p_fill), SIZE_NODE_LINK);
			tree->overflow_node = p;
			p_fill--;
		}
		else { /* parent will not overflow, clear overflow position */
			memset(tree->overflow_element, 0, tree->element_size);
			memset(tree->overflow_link, 0, SIZE_NODE_LINK);
			tree->overflow_node = REC_NULL;
		}
		cimap_inc(tree, p_cache, r_child_index);
		cimap_put(tree, p_cache, r, r_child_index);
		buffer_move_internal(p_cache->buffer, OFF_NODE_ELEMENT(tree, l_child_index + 1), OFF_NODE_ELEMENT(tree, l_child_index), (p_fill - l_child_index) * tree->element_size); /* insert new element at l->child_index */
		buffer_move_internal(p_cache->buffer, OFF_NODE_LINK(tree, l_child_index + 2), OFF_NODE_LINK(tree, l_child_index + 1), (p_fill - l_child_index) * SIZE_NODE_LINK); /* insert new link at l->child_index + 1 */
		buffer_move_external(p_cache->buffer, OFF_NODE_ELEMENT(tree, l_child_index), l_cache->buffer, OFF_NODE_ELEMENT(tree, sidx), tree->element_size); /* move single element from left node to parent */
		r_link_buffer = p_cache->buffer + OFF_NODE_LINK(tree, r_child_index);

		p_fill++;
	}
	r_link.child = r;

	for(i = 0; i < r_fill; i++) {
		child = buffer_get_rec(l_cache->buffer, OFF_NODE_LINK(tree, i + sidx + 1) + OFF_NODE_LINK_CHILD);
		cimap_remove(tree, l_cache, child);
		cimap_put(tree, r_cache, child, i);
	}
	buffer_move_external(r_cache->buffer, OFF_NODE_ELEMENT(tree, 0), l_cache->buffer, OFF_NODE_ELEMENT(tree, sidx + 1), (r_fill - 1) * tree->element_size); /* move all remaining elements except overflow element from left node to right node */
	buffer_move_external(r_cache->buffer, OFF_NODE_LINK(tree, 0), l_cache->buffer, OFF_NODE_LINK(tree, sidx + 1), r_fill * SIZE_NODE_LINK); /* move links except overflow link from left node to right node */
	buffer_fill(l_cache->buffer, OFF_NODE_ELEMENT(tree, sidx), 0, r_fill * tree->element_size); /* clear moved elements in left node */
	buffer_fill(l_cache->buffer, OFF_NODE_LINK(tree, sidx + 1), 0, r_fill * SIZE_NODE_LINK); /* clear moved links in left node */
	l_fill = sidx;

	n = 0;
	for(i = 0; i <= r_fill; i++) {
		buffer_set_u32(r_cache->buffer, OFF_NODE_LINK(tree, i) + OFF_NODE_LINK_OFFSET, n);
		n += buffer_get_u32(r_cache->buffer, OFF_NODE_LINK(tree, i) + OFF_NODE_LINK_COUNT) + 1;
	}
	n--;
	buffer_add_u32(p_cache->buffer, OFF_NODE_LINK(tree, l_child_index) + OFF_NODE_LINK_COUNT, -1 - n); /* n elements go to right node, one element to parent node */
	r_link.count = n;
	r_link.offset = buffer_get_u32(p_cache->buffer, OFF_NODE_LINK(tree, l_child_index) + OFF_NODE_LINK_OFFSET) + buffer_get_u32(p_cache->buffer, OFF_NODE_LINK(tree, l_child_index) + OFF_NODE_LINK_COUNT) + 1;

	buffer_set_rec(r_cache->buffer, OFF_NODE_PARENT, p);
	buffer_set_u32(l_cache->buffer, OFF_NODE_FILL, l_fill);
	buffer_set_u32(r_cache->buffer, OFF_NODE_FILL, r_fill);
	buffer_set_u32(p_cache->buffer, OFF_NODE_FILL, p_fill);
	link_to_buffer(r_link_buffer, 0, &r_link);
	l_cache->modified = true;
	r_cache->modified = true;
	p_cache->modified = true;
	l_cache->refcnt--;
	p_cache->refcnt--;

	for(i = 0; i <= r_fill; i++) { /* update parent of right node's children */
		child = buffer_get_rec(r_cache->buffer, OFF_NODE_LINK(tree, i) + OFF_NODE_LINK_CHILD);
		if(child != REC_NULL) {
			child_cache = cache_get(tree, txn, child);
			if(child_cache == NULL) {
				p_cache->refcnt--;
				r_cache->refcnt--;
				return errno;
			}
			buffer_set_rec(child_cache->buffer, OFF_NODE_PARENT, r);
			child_cache->modified = true;
		}
	}
	r_cache->refcnt--;
	return 0;
}

/* returns an error code */
static int concatenate(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t l)
{
	db_recno_t p;
	db_recno_t r;
	cache_t *l_cache;
	cache_t *r_cache;
	cache_t *p_cache;
	int ret;
	uint32_t i;
	uint32_t n;
	uint32_t l_child_index;
	uint32_t r_child_index;
	uint32_t l_fill;
	uint32_t r_fill;
	uint32_t p_fill;
	db_recno_t child = REC_NULL;
	cache_t *child_cache;

	assert(tree->overflow_node == REC_NULL);

	l_cache = cache_get(tree, txn, l);
	if(l_cache == NULL)
		return errno;
	l_cache->refcnt++;
	l_fill = buffer_get_u32(l_cache->buffer, OFF_NODE_FILL);
	p = buffer_get_rec(l_cache->buffer, OFF_NODE_PARENT);

	p_cache = cache_get(tree, txn, p);
	if(p_cache == NULL) {
		l_cache->refcnt--;
		return errno;
	}
	p_cache->refcnt++;
	p_fill = buffer_get_u32(p_cache->buffer, OFF_NODE_FILL);
	l_child_index = cimap_get(tree, p_cache, l);
	assert(l_child_index < tree->order - 1);
	r = buffer_get_rec(p_cache->buffer, OFF_NODE_LINK(tree, l_child_index + 1) + OFF_NODE_LINK_CHILD);

	r_cache = cache_get(tree, txn, r);
	if(r_cache == NULL) {
		p_cache->refcnt--;
		l_cache->refcnt--;
		return errno;
	}
	r_fill = buffer_get_u32(r_cache->buffer, OFF_NODE_FILL);
	r_child_index = l_child_index + 1;
	assert(r_child_index == cimap_get(tree, p_cache, r));

	assert(l_fill + 1 + r_fill <= tree->order);

	if(l_fill + 1 + r_fill == tree->order) { /* left element will overflow */
		buffer_move_external(tree->overflow_element, 0, r_cache->buffer, OFF_NODE_ELEMENT(tree, r_fill - 1), tree->element_size); /* move last element of right node into overflow position */
		buffer_move_external(tree->overflow_link, 0, r_cache->buffer, OFF_NODE_LINK(tree, r_fill), SIZE_NODE_LINK); /* move last link of right node into overflow position */
		tree->overflow_node = l;
		r_fill--;
	}
	for(i = 0; i <= r_fill; i++) {
		child = buffer_get_rec(r_cache->buffer, OFF_NODE_LINK(tree, i) + OFF_NODE_LINK_CHILD);
		cimap_put(tree, l_cache, child, i + l_fill + 1);
	}
	buffer_move_external(l_cache->buffer, OFF_NODE_ELEMENT(tree, l_fill), p_cache->buffer, OFF_NODE_ELEMENT(tree, l_child_index), tree->element_size); /* append element from parent to left node */
	buffer_move_external(l_cache->buffer, OFF_NODE_ELEMENT(tree, l_fill + 1), r_cache->buffer, OFF_NODE_ELEMENT(tree, 0), r_fill * tree->element_size); /* append elements except overflow from right node to left node */
	buffer_move_external(l_cache->buffer, OFF_NODE_LINK(tree, l_fill + 1), r_cache->buffer, OFF_NODE_LINK(tree, 0), (r_fill + 1) * SIZE_NODE_LINK); /* append links except overflow from right node to left node */
	l_fill += 1 + r_fill;

	p_fill--;
	cimap_remove(tree, p_cache, r);
	cimap_dec(tree, p_cache, r_child_index + 1);
	buffer_move_internal(p_cache->buffer, OFF_NODE_ELEMENT(tree, l_child_index), OFF_NODE_ELEMENT(tree, l_child_index + 1), (p_fill - l_child_index) * tree->element_size); /* delete element from parent */
	buffer_move_internal(p_cache->buffer, OFF_NODE_LINK(tree, l_child_index + 1), OFF_NODE_LINK(tree, l_child_index + 2), (p_fill - l_child_index) * SIZE_NODE_LINK); /* delete link from parent */
	buffer_fill(p_cache->buffer, OFF_NODE_ELEMENT(tree, p_fill), 0, tree->element_size); /* clear last element from parent */
	buffer_fill(p_cache->buffer, OFF_NODE_LINK(tree, p_fill + 1), 0, SIZE_NODE_LINK); /* clear last link from parent */
	ret = free_node(tree, txn, r);
	if(ret != 0)
		return ret;

	n = 0;
	for(i = 0; i <= l_fill; i++) {
		buffer_set_u32(l_cache->buffer, OFF_NODE_LINK(tree, i) + OFF_NODE_LINK_OFFSET, n);
		n += buffer_get_u32(l_cache->buffer, OFF_NODE_LINK(tree, i) + OFF_NODE_LINK_COUNT) + 1;
	}
	n--;
	if(tree->overflow_node == l) {
		n++;
		buffer_set_u32(tree->overflow_link, OFF_NODE_LINK_OFFSET, n);
		n += buffer_get_u32(tree->overflow_link, OFF_NODE_LINK_COUNT);
	}
	buffer_set_u32(p_cache->buffer, OFF_NODE_LINK(tree, l_child_index) + OFF_NODE_LINK_COUNT, n);

	buffer_set_u32(l_cache->buffer, OFF_NODE_FILL, l_fill);
	buffer_set_u32(p_cache->buffer, OFF_NODE_FILL, p_fill);
	l_cache->modified = true;
	p_cache->modified = true;
	p_cache->refcnt--;

	for(i = l_fill - r_fill; i <= l_fill; i++) {
		child = buffer_get_rec(l_cache->buffer, OFF_NODE_LINK(tree, i) + OFF_NODE_LINK_CHILD);
		if(child != REC_NULL) {
			child_cache = cache_get(tree, txn, child);
			if(child_cache == NULL)
				return errno;
			buffer_set_rec(child_cache->buffer, OFF_NODE_PARENT, l);
			child_cache->modified = true;
		}
	}
	l_cache->refcnt--;

	return 0;
}

/* returns error code */
static int lr_redistribute(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t l)
{
	db_recno_t p;
	db_recno_t r;
	cache_t *l_cache;
	cache_t *r_cache;
	cache_t *p_cache;
	uint32_t l_child_index;
	uint32_t r_child_index;
	uint32_t l_fill;
	uint32_t r_fill;
	uint32_t i;
	uint32_t n;
	db_recno_t child;
	cache_t *child_cache;

	assert(l == tree->overflow_node || tree->overflow_node == REC_NULL);
	assert(l != tree->root);

	l_cache = cache_get(tree, txn, l);
	if(l_cache == NULL)
		return errno;
	l_cache->refcnt++;
	l_fill = buffer_get_u32(l_cache->buffer, OFF_NODE_FILL);
	p = buffer_get_rec(l_cache->buffer, OFF_NODE_PARENT);

	p_cache = cache_get(tree, txn, p);
	if(p_cache == NULL) {
		l_cache->refcnt--;
		return errno;
	}
	p_cache->refcnt++;
	l_child_index = cimap_get(tree, p_cache, l);
	assert(l_child_index < tree->order - 1);
	r = buffer_get_rec(p_cache->buffer, OFF_NODE_LINK(tree, l_child_index + 1) + OFF_NODE_LINK_CHILD);

	r_cache = cache_get(tree, txn, r);
	if(r_cache == NULL) {
		p_cache->refcnt--;
		l_cache->refcnt--;
		return errno;
	}
	r_cache->refcnt++;
	r_fill = buffer_get_u32(r_cache->buffer, OFF_NODE_FILL);
	r_child_index = l_child_index + 1;
	assert(cimap_get(tree, p_cache, r) == r_child_index);
	assert(r_fill < tree->order - 1);

	cimap_inc(tree, r_cache, 0);
	buffer_move_internal(r_cache->buffer, OFF_NODE_ELEMENT(tree, 1), OFF_NODE_ELEMENT(tree, 0), r_fill * tree->element_size); /* insert new first element at right node */
	buffer_move_internal(r_cache->buffer, OFF_NODE_LINK(tree, 1), OFF_NODE_LINK(tree, 0), (r_fill + 1) * SIZE_NODE_LINK); /* insert new link at right node */
	buffer_move_external(r_cache->buffer, OFF_NODE_ELEMENT(tree, 0), p_cache->buffer, OFF_NODE_ELEMENT(tree, l_child_index), tree->element_size); /* move element from parent to first position at right node */
	if(l == tree->overflow_node) {
		child = buffer_get_rec(tree->overflow_link, OFF_NODE_LINK_CHILD);
		cimap_put(tree, r_cache, child, 0);
		buffer_move_external(p_cache->buffer, OFF_NODE_ELEMENT(tree, l_child_index), tree->overflow_element, 0, tree->element_size); /* move overflow element from left node to parent */
		buffer_move_external(r_cache->buffer, OFF_NODE_LINK(tree, 0), tree->overflow_link, 0, SIZE_NODE_LINK); /* move overflow link from left node to first link of right node */
		buffer_fill(tree->overflow_element, 0, 0, tree->element_size); /* clear overflow element */
		buffer_fill(tree->overflow_link, 0, 0, SIZE_NODE_LINK); /* clear overflow link */
		tree->overflow_node = REC_NULL;
	}
	else {
		child = buffer_get_rec(l_cache->buffer, OFF_NODE_LINK(tree, l_fill) + OFF_NODE_LINK_CHILD);
		cimap_remove(tree, l_cache, child);
		cimap_put(tree, r_cache, child, 0);
		buffer_move_external(p_cache->buffer, OFF_NODE_ELEMENT(tree, l_child_index), l_cache->buffer, OFF_NODE_ELEMENT(tree, l_fill - 1), tree->element_size); /* move last element from left node to parent */
		buffer_move_external(r_cache->buffer, OFF_NODE_LINK(tree, 0), l_cache->buffer, OFF_NODE_LINK(tree, l_fill), SIZE_NODE_LINK); /* move last link from left node to first link of right node */
		buffer_fill(l_cache->buffer, OFF_NODE_ELEMENT(tree, l_fill - 1), 0, tree->element_size); /* clear last element from left node */
		buffer_fill(l_cache->buffer, OFF_NODE_LINK(tree, l_fill), 0, SIZE_NODE_LINK); /* clear last link of left node */
		l_fill--;
	}
	r_fill++;

	n = buffer_get_u32(r_cache->buffer, OFF_NODE_LINK(tree, 0) + OFF_NODE_LINK_COUNT) + 1; /* number of elements moved from left to right... */
	buffer_add_u32(p_cache->buffer, OFF_NODE_LINK(tree, l_child_index) + OFF_NODE_LINK_COUNT, -n); /* ...which are missing on left node now */
	buffer_add_u32(p_cache->buffer, OFF_NODE_LINK(tree, r_child_index) + OFF_NODE_LINK_COUNT, n); /* ...are present on right node now */
	buffer_add_u32(p_cache->buffer, OFF_NODE_LINK(tree, r_child_index) + OFF_NODE_LINK_OFFSET, -n); /* ...which shift the offset of the right node */
	buffer_set_u32(r_cache->buffer, OFF_NODE_LINK(tree, 0) + OFF_NODE_LINK_OFFSET, 0);
	for(i = 1; i <= r_fill; i++)
		buffer_add_u32(r_cache->buffer, OFF_NODE_LINK(tree, i) + OFF_NODE_LINK_OFFSET, n); /* ...and the offset of all consecutive links on right node */

	buffer_set_u32(l_cache->buffer, OFF_NODE_FILL, l_fill);
	buffer_set_u32(r_cache->buffer, OFF_NODE_FILL, r_fill);
	l_cache->modified = true;
	r_cache->modified = true;
	p_cache->modified = true;
	l_cache->refcnt--;
	p_cache->refcnt--;

	child = buffer_get_rec(r_cache->buffer, OFF_NODE_LINK(tree, 0) + OFF_NODE_LINK_CHILD);
	if(child != REC_NULL) {
		child_cache = cache_get(tree, txn, child);
		if(child_cache == NULL) {
			p_cache->refcnt--;
			r_cache->refcnt--;
			return errno;
		}
		buffer_set_rec(child_cache->buffer, OFF_NODE_PARENT, r);
		child_cache->modified = true;
	}
	r_cache->refcnt--;
	return 0;
}

/* returns error code */
static int rl_redistribute(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t r)
{
	db_recno_t p;
	db_recno_t l;
	cache_t *l_cache;
	cache_t *r_cache;
	cache_t *p_cache;
	uint32_t l_child_index;
	uint32_t r_child_index;
	uint32_t l_fill;
	uint32_t r_fill;
	uint32_t i;
	uint32_t n;
	db_recno_t child;
	cache_t *child_cache;

	assert(r == tree->overflow_node || tree->overflow_node == REC_NULL);
	assert(r != tree->root);

	r_cache = cache_get(tree, txn, r);
	if(r_cache == NULL)
		return errno;
	r_cache->refcnt++;
	r_fill = buffer_get_u32(r_cache->buffer, OFF_NODE_FILL);
	p = buffer_get_rec(r_cache->buffer, OFF_NODE_PARENT);

	p_cache = cache_get(tree, txn, p);
	if(p_cache == NULL) {
		r_cache->refcnt--;
		return errno;
	}
	p_cache->refcnt++;
	r_child_index = cimap_get(tree, p_cache, r);
	l = buffer_get_rec(p_cache->buffer, OFF_NODE_LINK(tree, r_child_index - 1) + OFF_NODE_LINK_CHILD);
	assert(r_child_index > 0);

	l_cache = cache_get(tree, txn, l);
	if(l_cache == NULL) {
		p_cache->refcnt--;
		r_cache->refcnt--;
		return errno;
	}
	l_cache->refcnt++;
	l_fill = buffer_get_u32(l_cache->buffer, OFF_NODE_FILL);
	l_child_index = r_child_index - 1;
	assert(cimap_get(tree, p_cache, l) == l_child_index);
	assert(l_fill < tree->order - 1);

	child = buffer_get_rec(r_cache->buffer, OFF_NODE_LINK(tree, 0) + OFF_NODE_LINK_CHILD);
	cimap_put(tree, l_cache, child, l_fill + 1);
	cimap_remove(tree, r_cache, child);
	cimap_dec(tree, r_cache, 1); /* first element will be deleted, update child index of following children */
	buffer_move_external(l_cache->buffer, OFF_NODE_ELEMENT(tree, l_fill), p_cache->buffer, OFF_NODE_ELEMENT(tree, l_child_index), tree->element_size); /* move element from parent to last position at left node */
	buffer_move_external(p_cache->buffer, OFF_NODE_ELEMENT(tree, l_child_index), r_cache->buffer, OFF_NODE_ELEMENT(tree, 0), tree->element_size); /* move first element from right node to parent */
	buffer_move_external(l_cache->buffer, OFF_NODE_LINK(tree, l_fill + 1), r_cache->buffer, OFF_NODE_LINK(tree, 0), SIZE_NODE_LINK); /* move first link from right node to left node */
	buffer_move_internal(r_cache->buffer, OFF_NODE_ELEMENT(tree, 0), OFF_NODE_ELEMENT(tree, 1), (r_fill - 1) * tree->element_size); /* delete first element at right node */
	buffer_move_internal(r_cache->buffer, OFF_NODE_LINK(tree, 0), OFF_NODE_LINK(tree, 1), r_fill * SIZE_NODE_LINK); /* delete first link at right node */
	l_fill++;
	if(tree->overflow_node == r) {
		child = buffer_get_rec(tree->overflow_link, OFF_NODE_LINK_CHILD);
		cimap_put(tree, r_cache, child, r_fill);
		buffer_move_external(r_cache->buffer, OFF_NODE_ELEMENT(tree, r_fill - 1), tree->overflow_element, 0, tree->element_size);
		buffer_move_external(r_cache->buffer, OFF_NODE_LINK(tree, r_fill), tree->overflow_link, 0, SIZE_NODE_LINK);
		buffer_fill(tree->overflow_element, 0, 0, tree->element_size); /* clear overflow element */
		buffer_fill(tree->overflow_link, 0, 0, SIZE_NODE_LINK); /* clear overflow link */
		tree->overflow_node = REC_NULL;
	}
	else {
		buffer_fill(r_cache->buffer, OFF_NODE_ELEMENT(tree, r_fill - 1), 0, tree->element_size); /* clear last element from right node */
		buffer_fill(r_cache->buffer, OFF_NODE_LINK(tree, r_fill), 0, SIZE_NODE_LINK); /* clear last link from right node */
		r_fill--;
	}

	n = buffer_get_u32(l_cache->buffer, OFF_NODE_LINK(tree, l_fill) + OFF_NODE_LINK_COUNT) + 1;
	buffer_add_u32(p_cache->buffer, OFF_NODE_LINK(tree, l_child_index) + OFF_NODE_LINK_COUNT, n);
	buffer_add_u32(p_cache->buffer, OFF_NODE_LINK(tree, r_child_index) + OFF_NODE_LINK_COUNT, -n);
	buffer_add_u32(p_cache->buffer, OFF_NODE_LINK(tree, r_child_index) + OFF_NODE_LINK_OFFSET, n);
	for(i = 0; i <= r_fill; i++)
		buffer_add_u32(r_cache->buffer, OFF_NODE_LINK(tree, i) + OFF_NODE_LINK_OFFSET, -n);

	if(l_fill == 0)
		buffer_set_u32(l_cache->buffer, OFF_NODE_LINK(tree, 0) + OFF_NODE_LINK_OFFSET, 0);
	else {
		uint32_t tmpoffset = buffer_get_u32(l_cache->buffer, OFF_NODE_LINK(tree, l_fill - 1) + OFF_NODE_LINK_OFFSET);
		uint32_t tmpcount = buffer_get_u32(l_cache->buffer, OFF_NODE_LINK(tree, l_fill - 1) + OFF_NODE_LINK_COUNT);
		buffer_set_u32(l_cache->buffer, OFF_NODE_LINK(tree, l_fill) + OFF_NODE_LINK_OFFSET, tmpoffset + tmpcount + 1);
	}

	buffer_set_u32(l_cache->buffer, OFF_NODE_FILL, l_fill);
	buffer_set_u32(r_cache->buffer, OFF_NODE_FILL, r_fill);
	l_cache->modified = true;
	r_cache->modified = true;
	p_cache->modified = true;
	r_cache->refcnt--;
	p_cache->refcnt--;

	child = buffer_get_rec(l_cache->buffer, OFF_NODE_LINK(tree, l_fill) + OFF_NODE_LINK_CHILD);
	if(child != REC_NULL) {
		child_cache = cache_get(tree, txn, child);
		if(child_cache == NULL) {
			l_cache->refcnt--;
			r_cache->refcnt--;
			return errno;
		}
		buffer_set_rec(child_cache->buffer, OFF_NODE_PARENT, l);
		child_cache->modified = true;
	}
	l_cache->refcnt--;
	return 0;
}

static int adjust(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t node)
{
	cache_t *parent_cache;
	cache_t *node_cache;
	int ret = 0;
	db_recno_t parent;
	db_recno_t left;
	db_recno_t right;
	node_cache = cache_get(tree, txn, node);
	if(node_cache == NULL)
		return errno;
	parent = buffer_get_rec(node_cache->buffer, OFF_NODE_PARENT);

	if(tree->overflow_node == node) {
		if(parent == REC_NULL) {
			ret = newroot(tree, txn);
			if(ret != 0)
				return ret;
			return split(tree, txn, node);
		}
		else {
			node_cache->refcnt++;
			parent_cache = cache_get(tree, txn, parent);
			if(parent_cache == NULL) {
				node_cache->refcnt--;
				return errno;
			}
			parent_cache->refcnt++;

			left = left_sibling(tree, parent_cache, node_cache);
			right = right_sibling(tree, parent_cache, node_cache);
			if(right != REC_NULL) {
				cache_t *right_cache = cache_get(tree, txn, right);
				if(right_cache == NULL) {
					parent_cache->refcnt--;
					node_cache->refcnt--;
					return errno;
				}
				/* no refcnt on right node required as from here on no further cache_get() will be called */
				if(!near_overflowing(tree, right_cache)) {
					node_cache->refcnt--;
					parent_cache->refcnt--;
					lr_redistribute(tree, txn, node);
					return 0;
				}
			}
			if(left != REC_NULL) {
				cache_t *left_cache = cache_get(tree, txn, left);
				if(left_cache == NULL) {
					parent_cache->refcnt--;
					node_cache->refcnt--;
					return errno;
				}
				/* no refcnt on left node required as from here on no further cache_get() will be called */
				if(!near_overflowing(tree, left_cache)) {
					node_cache->refcnt--;
					parent_cache->refcnt--;
					rl_redistribute(tree, txn, node);
					return 0;
				}
			}
			node_cache->refcnt--;
			parent_cache->refcnt--;
			ret = split(tree, txn, node);
			if(ret != 0)
				return ret;
			return adjust(tree, txn, parent);
		}
	}
	else if(underflowing(tree, node_cache)) {
		if(parent == REC_NULL) {
			if(buffer_get_u32(node_cache->buffer, OFF_NODE_FILL) == 0) {
				tree->root = buffer_get_rec(node_cache->buffer, OFF_NODE_LINK(tree, 0) + OFF_NODE_LINK_CHILD);
				update_header(tree, txn);
				node_cache = cache_get(tree, txn, tree->root);
				if(node_cache == NULL)
					return errno;
				buffer_set_rec(node_cache->buffer, OFF_NODE_PARENT, REC_NULL);
				return free_node(tree, txn, node);
			}
		}
		else {
			node_cache->refcnt++;
			parent_cache = cache_get(tree, txn, parent);
			if(parent_cache == NULL) {
				node_cache->refcnt--;
				return errno;
			}
			parent_cache->refcnt++;

			left = left_sibling(tree, parent_cache, node_cache);
			right = right_sibling(tree, parent_cache, node_cache);
			if(left != REC_NULL) {
				cache_t *left_cache = cache_get(tree, txn, left);
				if(left_cache == NULL) {
					parent_cache->refcnt--;
					node_cache->refcnt--;
					return errno;
				}
				/* no refcnt on left node required as from here on no further cache_get() will be called */
				if(!near_underflowing(tree, left_cache)) {
					node_cache->refcnt--;
					parent_cache->refcnt--;
					return lr_redistribute(tree, txn, left);
				}
			}
			if(right != REC_NULL) {
				cache_t *right_cache = cache_get(tree, txn, right);
				if(right_cache == NULL) {
					parent_cache->refcnt--;
					node_cache->refcnt--;
					return errno;
				}
				/* no refcnt on right node required as from here on no further cache_get() will be called */
				if(!near_underflowing(tree, right_cache)) {
					node_cache->refcnt--;
					parent_cache->refcnt--;
					return rl_redistribute(tree, txn, right);
				}
			}
			node_cache->refcnt--;
			parent_cache->refcnt--;
			if(right != REC_NULL) {
				ret = concatenate(tree, txn, node);
				if(ret != 0)
					return ret;
				return adjust(tree, txn, parent);
			}
			if(left != REC_NULL) {
				ret = concatenate(tree, txn, left);
				if(ret != 0)
					return ret;
				return adjust(tree, txn, parent);
			}
			assert(false);
		}
		return 0;
	}
	else
		return 0;
}

/* returns whether the resulting element is equal to 'key'.
 * in case false is returned, errno must additionally be checked for database errors */
static bool find_lower(
		bdb_btree_t *tree,
		DB_TXN *txn,
		const void *key,
		db_recno_t *node,
		int *pos)
{
	FIND_LOWER_IMPL(CMP_E(tree, cur_cache->buffer + OFF_NODE_ELEMENT(tree, m), key))
}

static bool find_lower_custom(
		bdb_btree_t *tree,
		DB_TXN *txn,
		const void *key,
		int (*cmpfn)(const void *a, const void *b),
		db_recno_t *node,
		int *pos)
{
	FIND_LOWER_IMPL(cmpfn(cur_cache->buffer + OFF_NODE_ELEMENT(tree, m), key))
}

static bool find_lower_custom_user(
		bdb_btree_t *tree,
		DB_TXN *txn,
		const void *key,
		int (*cmpfn)(const void *a, const void *b, void *data, void *cmpdata),
		void *cmpdata,
		db_recno_t *node,
		int *pos)
{
	FIND_LOWER_IMPL(cmpfn(cur_cache->buffer + OFF_NODE_ELEMENT(tree, m), key, tree->data, cmpdata))
}

static bool find_upper(
		bdb_btree_t *tree,
		DB_TXN *txn,
		const void *key,
		db_recno_t *node,
		int *pos)
{
	FIND_UPPER_IMPL(CMP_E(tree, cur_cache->buffer + OFF_NODE_ELEMENT(tree, m), key))
}

static bool find_upper_custom(
		bdb_btree_t *tree,
		DB_TXN *txn,
		const void *key,
		int (*cmpfn)(const void *a, const void *b),
		db_recno_t *node,
		int *pos)
{
	FIND_UPPER_IMPL(cmpfn(cur_cache->buffer + OFF_NODE_ELEMENT(tree, m), key))
}

static bool find_upper_custom_user(
		bdb_btree_t *tree,
		DB_TXN *txn,
		const void *key,
		int (*cmpfn)(const void *a, const void *b, void *data, void *cmpdata),
		void *cmpdata,
		db_recno_t *node,
		int *pos)
{
	FIND_UPPER_IMPL(cmpfn(cur_cache->buffer + OFF_NODE_ELEMENT(tree, m), key, tree->data, cmpdata))
}

/* returns whether the given index has been found. if false:
 *   - 'node' == NULL: given index greater than size
 *   - 'node' != NULL: given index == size, can append at node->elements[pos] (note: 'pos' may be the overflow position)
 * in case false is returned, check errno for error */
static bool find_index(
		bdb_btree_t *tree,
		DB_TXN *txn,
		int index,
		db_recno_t *node,
		int *pos)
{
	int u;
	int l;
	int m;
	db_recno_t cur = tree->root;
	cache_t *cur_cache;
	uint32_t cur_fill;
	int offset = 0;
	int o;
	int c;

	while(cur != REC_NULL) {
		cur_cache = cache_get(tree, txn, cur);
		if(cur_cache == NULL)
			return false;
		cur_fill = buffer_get_u32(cur_cache->buffer, OFF_NODE_FILL);
		u = cur_fill;
		l = 0;
		while(l <= u) {
			m = l + (u - l) / 2;
			c = buffer_get_u32(cur_cache->buffer, OFF_NODE_LINK(tree, m) + OFF_NODE_LINK_COUNT);
			o = offset + buffer_get_u32(cur_cache->buffer, OFF_NODE_LINK(tree, m) + OFF_NODE_LINK_OFFSET);
//			c = cur->links[m].count;
//			o = offset + cur->links[m].offset;
			if(o + c == index) {
				if(m == cur_fill && !isleaf(tree, cur_cache)) {
					cur = buffer_get_rec(cur_cache->buffer, OFF_NODE_LINK(tree, m) + OFF_NODE_LINK_CHILD);
//					cur = cur->links[m].child;
					offset = o;
					break;
				}
				else {
					if(node != NULL)
						*node = cur;
					if(pos != NULL)
						*pos = m;
					errno = 0;
					return m < cur_fill;
				}
			}
			else if(o > index)
				u = m - 1;
			else if(o + c < index)
				l = m + 1;
			else {
				cur = buffer_get_rec(cur_cache->buffer, OFF_NODE_LINK(tree, m) + OFF_NODE_LINK_CHILD);
				//cur = cur->links[m].child;
				offset = o;
				break;
			}
		}
		if(l > u)
			break;
	}
	if(node != NULL)
		*node = REC_NULL;
	if(pos != NULL)
		*pos = 0;
	errno = 0;
	return false;
}

/* returns an error code */
static int to_insert_before(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t *node,
		int *pos)
{
	cache_t *node_cache;
	if(*node == REC_NULL) { /* rightmost position in tree; find_lower returns NULL is all elements are less, therefore insertion would take place at rightmost slot */
		if(tree->root == REC_NULL) /* no root node yet, *node remains NULL */
			return 0;
		node_cache = cache_get(tree, txn, tree->root);
		if(node_cache == NULL)
			return errno;
		*node = tree->root;
		*pos = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL);
	}
	node_cache = cache_get(tree, txn, *node);
	if(node_cache == NULL)
		return errno;
	while(!isleaf(tree, node_cache)) {
		*node = buffer_get_rec(node_cache->buffer, OFF_NODE_LINK(tree, *pos) + OFF_NODE_LINK_CHILD);
		node_cache = cache_get(tree, txn, *node);
		if(node_cache == NULL)
			return errno;
		*pos = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL);
	}
	return 0;
}

/* returns an error code */
static int update_count(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t node,
		int amount)
{
	uint32_t i;
	uint32_t ci;
	db_recno_t parent;
	cache_t *node_cache;
	uint32_t fill;

	node_cache = cache_get(tree, txn, node);
	if(node_cache == NULL)
		return errno;
	parent = buffer_get_rec(node_cache->buffer, OFF_NODE_PARENT);

	while(parent != REC_NULL) {
		node_cache = cache_get(tree, txn, parent); /* node_cache: now is parent_cache (i.e. node and node_cache refer to different nodes!) */
		if(node_cache == NULL)
			return errno;
		ci = cimap_get(tree, node_cache, node); /* mind: node_cache is parent_cache */
		node = parent; /* now node and node_cache refer to the same node */
		parent = buffer_get_rec(node_cache->buffer, OFF_NODE_PARENT);

		buffer_add_u32(node_cache->buffer, OFF_NODE_LINK(tree, ci) + OFF_NODE_LINK_COUNT, amount);
		fill = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL);
		for(i = ci + 1; i <= fill; i++)
			buffer_add_u32(node_cache->buffer, OFF_NODE_LINK(tree, i) + OFF_NODE_LINK_OFFSET, amount);
		node_cache->modified = true;
	}
	return 0;
}

/* returns an error code */
static int node_insert(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t node,
		int pos,
		void *element)
{
	int ret;
	int fill;
	cache_t *node_cache;

	if(tree->root == REC_NULL) {
		ret = newroot(tree, txn);
		if(ret != 0)
			return ret;
	}
	if(node == REC_NULL) {
		node = tree->root;
		pos = 0;
	}
	node_cache = cache_get(tree, txn, node);
	if(node_cache == NULL)
		return errno;

	fill = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL);
	if(pos == tree->order - 1) { /* put new element into overflow position */
		buffer_set_data(tree->overflow_element, 0, element, tree->element_size);
		tree->overflow_node = node;
	}
	else {
		if(fill == tree->order - 1) { /* node will overflow, move last element to overflow position */
			buffer_get_data(node_cache->buffer, OFF_NODE_ELEMENT(tree, fill - 1), tree->overflow_element, tree->element_size);
			tree->overflow_node = node;
			fill--;
		}
		buffer_move_internal(node_cache->buffer, OFF_NODE_ELEMENT(tree, pos + 1), OFF_NODE_ELEMENT(tree, pos), (fill - pos) * tree->element_size);
		buffer_set_data(node_cache->buffer, OFF_NODE_ELEMENT(tree, pos), element, tree->element_size);
		fill++;
		buffer_set_u32(node_cache->buffer, OFF_NODE_FILL, fill);
	}
	if(tree->overflow_node == node)
		buffer_set_u32(tree->overflow_link, OFF_NODE_LINK_OFFSET, fill + 1);
	else
		buffer_set_u32(node_cache->buffer, OFF_NODE_LINK(tree, fill) + OFF_NODE_LINK_OFFSET, fill);
	node_cache->modified = true;

 	update_count(tree, txn, node, 1);
	ret = adjust(tree, txn, node);
	if(ret != 0)
		return ret;
	ACQUIRE_E(tree, element);
	return 0;
}

static int node_replace(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t node,
		int pos,
		void *element)
{
	cache_t *node_cache;

	node_cache = cache_get(tree, txn, node);
	if(node_cache == NULL)
		return errno;

	ACQUIRE_E(tree, element);
	RELEASE_E(tree, node_cache->buffer + OFF_NODE_ELEMENT(tree, pos));
	buffer_set_data(node_cache->buffer, OFF_NODE_ELEMENT(tree, pos), element, tree->element_size);
	return 0;
}

static int node_remove(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t node,
		int pos)
{
	db_recno_t cur;
	uint32_t node_fill;
	uint32_t cur_fill;
	cache_t *node_cache;
	cache_t *cur_cache;
	int ret;

	node_cache = cache_get(tree, txn, node);
	if(node_cache == NULL)
		return errno;
	node_fill = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL);
	node_cache->refcnt++;
	RELEASE_E(tree, node_cache->buffer + OFF_NODE_ELEMENT(tree, pos));

	if(isleaf(tree, node_cache)) { /* node where the element is contained within is a leaf, simply remove it */
		node_cache->refcnt--;
		node_fill--;
		buffer_move_internal(node_cache->buffer, OFF_NODE_ELEMENT(tree, pos), OFF_NODE_ELEMENT(tree, pos + 1), (node_fill - pos) * tree->element_size);
		if(node == tree->root && node_fill == 0) {
			tree->root = REC_NULL;
			return free_node(tree, txn, node);
		}
		buffer_set_u32(node_cache->buffer, OFF_NODE_FILL, node_fill);
		node_cache->modified = true;
	}
	else { /* node where the element is contained within is not a leaf, search cur leaf of right subtree */
		cur = buffer_get_rec(node_cache->buffer, OFF_NODE_LINK(tree, pos + 1) + OFF_NODE_LINK_CHILD);
		cur_cache = cache_get(tree, txn, cur);
		if(cur_cache == NULL)
			return errno;
		while(!isleaf(tree, cur_cache)) {
			cur = buffer_get_rec(cur_cache->buffer, OFF_NODE_LINK(tree, 0) + OFF_NODE_LINK_CHILD);
			cur_cache = cache_get(tree, txn, cur);
			if(cur_cache == NULL)
				return errno;
		}
		cur_fill = buffer_get_rec(cur_cache->buffer, OFF_NODE_FILL);
		buffer_move_external(node_cache->buffer, OFF_NODE_ELEMENT(tree, pos), cur_cache->buffer, OFF_NODE_ELEMENT(tree, 0), tree->element_size); /* move first element to position of deleted element */
		cur_fill--;
		buffer_move_internal(cur_cache->buffer, OFF_NODE_ELEMENT(tree, 0), OFF_NODE_ELEMENT(tree, 1), cur_fill * tree->element_size); /* delete moved element */
		buffer_fill(cur_cache->buffer, OFF_NODE_ELEMENT(tree, cur_fill), 0, tree->element_size);
		buffer_set_u32(cur_cache->buffer, OFF_NODE_FILL, cur_fill);
		cur_cache->modified = true;
		node_cache->modified = true;
		node_cache->refcnt--;
		node = cur;
	}
	ret = update_count(tree, txn, node, -1);
	if(ret != 0)
		return ret;
	return adjust(tree, txn, node);
}

static int to_index(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t node,
		int pos)
{
	cache_t *node_cache;
	db_recno_t parent;
	db_recno_t child;
	int index;

	if(node == REC_NULL)
		return 0;
	node_cache = cache_get(tree, txn, node);
	if(node_cache == NULL)
		return errno;
	index = buffer_get_u32(node_cache->buffer, OFF_NODE_LINK(tree, pos) + OFF_NODE_LINK_COUNT);
	while(true) {
		index += buffer_get_u32(node_cache->buffer, OFF_NODE_LINK(tree, pos) + OFF_NODE_LINK_OFFSET);
		child = node;
		node = buffer_get_rec(node_cache->buffer, OFF_NODE_PARENT);
		if(node == REC_NULL)
			return index;
		node_cache = cache_get(tree, txn, node);
		if(node_cache == NULL)
			return errno;
		pos = cimap_get(tree, node_cache, child);
	}
}

static bool to_next(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t *node_,
		int *pos_)
{
	db_recno_t node = *node_;
	db_recno_t child;
	cache_t *node_cache;
	int node_fill;
	int pos = *pos_;

	node_cache = cache_get(tree, txn, node);
	if(node_cache == NULL) /* errno already set */
		return false;
	node_fill = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL);
	if(pos == node_fill) {
		errno = 0;
		return false;
	}
	pos++;
	/* descend */
	child = buffer_get_rec(node_cache->buffer, OFF_NODE_LINK(tree, pos) + OFF_NODE_LINK_CHILD);
	while(child != REC_NULL) {
		node = child;
		node_cache = cache_get(tree, txn, node);
		if(node_cache == NULL) /* errno already set */
			return false;
		child = buffer_get_rec(node_cache->buffer, OFF_NODE_LINK(tree, pos) + OFF_NODE_LINK_CHILD);
		pos = 0;
	}
	/* ascend */
	node_fill = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL);
	while(pos == node_fill) {
		child = node;
		node = buffer_get_rec(node_cache->buffer, OFF_NODE_PARENT);
		if(node == REC_NULL) {
			(*pos_)++;
			errno = 0;
			return true;
		}
		node_cache = cache_get(tree, txn, node);
		if(node_cache == NULL) /* errno already set */
			return false;
		pos = cimap_get(tree, node_cache, child);
		node_fill = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL);
	}
	*node_ = node;
	*pos_ = pos;
	errno = 0;
	return true;
}

static bool to_prev(
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t *node_,
		int *pos_)
{
	db_recno_t node = *node_;
	db_recno_t child;
	cache_t *node_cache;
	int pos = *pos_;

	node_cache = cache_get(tree, txn, node);
	if(node_cache == NULL) /* errno already set */
		return false;
	/* descend */
	child = buffer_get_rec(node_cache->buffer, OFF_NODE_LINK(tree, pos) + OFF_NODE_LINK_CHILD);
	while(child != REC_NULL) {
		node = child;
		node_cache = cache_get(tree, txn, node);
		if(node_cache == NULL) /* errno already set */
			return false;
		child = buffer_get_rec(node_cache->buffer, OFF_NODE_LINK(tree, pos) + OFF_NODE_LINK_CHILD);
		pos = 0;
	}
	/* ascend */
	while(pos == 0) {
		child = node;
		node = buffer_get_rec(node_cache->buffer, OFF_NODE_PARENT);
		if(node == REC_NULL)
			return false;
		node_cache = cache_get(tree, txn, node);
		if(node_cache == NULL) /* errno already set */
			return false;
		pos = cimap_get(tree, node_cache, child);
	}
	pos--;
	*node_ = node;
	*pos_ = pos;
	errno = 0;
	return true;
}

/* TODO do a more intense search to find possible better ways of doing this */
int bdb_btree_exists(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name)
{
	char namebuf[MAX_NAME_LEN + 1 + 6];
	DB *db;
	int ret;
	
	if(strlen(name) > MAX_NAME_LEN)
		return -EINVAL;

	ret = db_create(&db, env, 0);
	if(ret != 0)
		return ret;

	sprintf(namebuf, "%s.btree", name);
	ret = db->open(db, txn, namebuf, NULL, DB_RECNO, DB_RDONLY, 0);
	if(ret != 0)
		return ret;
	db->close(db, 0);
	return 0;
}

static bdb_btree_t *internal_create(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name,
		int cache_buffers,
		int order,
		int element_size,
		uint32_t options)
{
	int ret;
	bdb_btree_t *self;
	char namebuf[MAX_NAME_LEN + 1 + 6];
	int node_size = NODE_SIZE2(order, element_size);
	DBT dbt_key;
	DBT dbt_data;
	db_recno_t recno;
	int i;

	memset(&dbt_key, 0, sizeof(dbt_key));
	memset(&dbt_data, 0, sizeof(dbt_data));

	if(cache_buffers == 0)
		cache_buffers = MIN_CACHE_BUFFERS;
	else if(cache_buffers == -1)
		cache_buffers = DEFAULT_CACHE_BUFFERS;
	if(strlen(name) > MAX_NAME_LEN) {
		ret = -EINVAL;
		goto error_1;
	}
	else if(element_size <= 0 || order < 3) {
		ret = -EINVAL;
		goto error_1;
	}
	else if(cache_buffers < MIN_CACHE_BUFFERS) {
		ret = -EINVAL;
		goto error_1;
	}

	self = calloc(1, sizeof(*self) + node_size * REC_BUFFERS + element_size);
	if(self == NULL) {
		ret = -ENOMEM;
		goto error_1;
	}
	self->overflow_element = (char*)self + sizeof(*self);
	for(i = 0; i < REC_BUFFERS; i++) {
		self->cache[i].buffer = (char*)self + sizeof(*self) + element_size + i * node_size;
		self->cache[i].recno = REC_NULL;
	}
	self->root = REC_NULL;
	self->free_list = REC_NULL;
	self->max_recno = REC_HEADER;
	self->order = order;
	self->element_size = element_size;
	self->options = options;

	ret = db_create(&self->db, env, 0);
	if(ret != 0)
		goto error_2;

	ret = self->db->set_re_len(self->db, node_size);
	if(ret != 0)
		goto error_3;
	
	sprintf(namebuf, "%s.btree", name);
	ret = self->db->open(self->db, txn, namebuf, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0);
	if(ret != 0)
		goto error_3;

	/* here: use rec_buffer[0] for header */
	assert(SIZE_HEADER <= node_size);
	buffer_set_u32(self->cache[0].buffer, OFF_HEADER_MAGIC, BTREE_MAGIC); /* keep in sync with update_header() */
	buffer_set_u32(self->cache[0].buffer, OFF_HEADER_VERSION, BTREE_VERSION);
	buffer_set_u32(self->cache[0].buffer, OFF_HEADER_ORDER, order);
	buffer_set_u32(self->cache[0].buffer, OFF_HEADER_ELEMENT_SIZE, element_size);
	buffer_set_u32(self->cache[0].buffer, OFF_HEADER_OPTIONS, options & OPTMASK_PERMANENT);
	buffer_set_rec(self->cache[0].buffer, OFF_HEADER_ROOT, REC_NULL);
	buffer_set_rec(self->cache[0].buffer, OFF_HEADER_FREE_LIST, REC_NULL);
	buffer_set_rec(self->cache[0].buffer, OFF_HEADER_MAX_RECNO, REC_HEADER);
	dbt_key.data = &recno;
	dbt_key.ulen = sizeof(recno);
	dbt_key.flags = DB_DBT_USERMEM;
	dbt_data.data = self->cache[0].buffer;
	dbt_data.size = node_size;
	ret = self->db->put(self->db, txn, &dbt_key, &dbt_data, DB_APPEND);
	if(ret != 0)
		goto error_3;
	if(recno != REC_HEADER) {
		ret = -EIO;
		goto error_3;
	}

	return self;

error_3:
	self->db->close(self->db, 0);
error_2:
	free(self);
error_1:
	errno = ret;
	return NULL;
}

static bdb_btree_t *internal_open(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name,
		int cache_buffers,
		int flags)
{
	int ret;
	bdb_btree_t *self;
	char namebuf[MAX_NAME_LEN + 1 + 6];
	char headerbuf[SIZE_HEADER];
	uint32_t node_size;
	uint32_t element_size;
	uint32_t order;
	DB *db;
	DBT dbt_key;
	DBT dbt_data;
	db_recno_t recno;
	u_int32_t dbflags = 0;
	int i;

	memset(&dbt_key, 0, sizeof(dbt_key));
	memset(&dbt_data, 0, sizeof(dbt_data));

	if(cache_buffers == 0)
		cache_buffers = MIN_CACHE_BUFFERS;
	else if(cache_buffers == -1)
		cache_buffers = DEFAULT_CACHE_BUFFERS;
	if(strlen(name) > MAX_NAME_LEN) {
		ret = -EINVAL;
		goto error_1;
	}
	else if(cache_buffers < MIN_CACHE_BUFFERS) {
		ret = -EINVAL;
		goto error_1;
	}

	ret = db_create(&db, env, 0);
	if(ret != 0)
		goto error_1;

	if((flags & BTREE_RDONLY) != 0)
		dbflags |= DB_RDONLY;

	sprintf(namebuf, "%s.btree", name);
	ret = db->open(db, txn, namebuf, NULL, DB_RECNO, dbflags, 0);
	if(ret != 0)
		goto error_2;

	recno = REC_HEADER;
	dbt_key.data = &recno;
	dbt_key.size = sizeof(recno);
	dbt_data.data = headerbuf;
	dbt_data.ulen = SIZE_HEADER;
	dbt_data.doff = 0;
	dbt_data.dlen = SIZE_HEADER;
	dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;
	ret = db->get(db, txn, &dbt_key, &dbt_data, 0);
	if(ret != 0)
		goto error_2;
	else if(buffer_get_u32(headerbuf, OFF_HEADER_MAGIC) != BTREE_MAGIC) {
		errno = -EIO;
		goto error_2;
	}
	else if(buffer_get_u32(headerbuf, OFF_HEADER_VERSION) != BTREE_VERSION) {
		errno = -EIO;
		goto error_2;
	}

	order = buffer_get_u32(headerbuf, OFF_HEADER_ORDER);
	element_size = buffer_get_u32(headerbuf, OFF_HEADER_ELEMENT_SIZE);
	node_size = NODE_SIZE2(order, element_size);

	self = calloc(1, sizeof(*self) + node_size * REC_BUFFERS + element_size);
	if(self == NULL) {
		ret = -ENOMEM;
		goto error_2;
	}
	self->overflow_element = (char*)self + sizeof(*self);
	for(i = 0; i < REC_BUFFERS; i++) {
		self->cache[i].buffer = (char*)self + sizeof(*self) + element_size + i * node_size;
		self->cache[i].recno = REC_NULL;
	}
	self->db = db;
	self->root = buffer_get_rec(headerbuf, OFF_HEADER_ROOT);
	self->free_list = buffer_get_rec(headerbuf, OFF_HEADER_FREE_LIST);
	self->max_recno = buffer_get_rec(headerbuf, OFF_HEADER_MAX_RECNO);
	self->order = order;
	self->element_size = element_size;
	self->options = buffer_get_u32(headerbuf, OFF_HEADER_OPTIONS);

	return self;

error_2:
	db->close(db, 0);
error_1:
	errno = ret;
	return NULL;
}


bdb_btree_t *bdb_btree_create(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name,
		int cache_buffers,
		int order,
		int element_size,
		int (*cmp)(const void *a, const void *b),
		int (*acquire)(void *a),
		void (*release)(void *a),
		uint32_t options)
{
	bdb_btree_t *self;

	if(cmp == NULL)
		options |= OPT_NOCMP;
	self = internal_create(env, txn, name, cache_buffers, order, element_size, options);
	if(self == NULL)
		return NULL;

	self->cb.nodata.cmp = cmp;
	self->cb.nodata.acquire = acquire;
	self->cb.nodata.release = release;

	return self;
}

bdb_btree_t *bdb_btree_create_user(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name,
		int cache_buffers, /* -1: default value, 0: minimum value */
		int order,
		int element_size,
		int (*cmp)(const void *a, const void *b, void *user),
		int (*acquire)(void *a, void *user),
		void (*release)(void *a, void *user),
		uint32_t options,
		void *user)
{
	bdb_btree_t *self;

	if(cmp == NULL)
		options |= OPT_NOCMP;
	options |= OPT_USE_DATA;
	self = internal_create(env, txn, name, cache_buffers, order, element_size, options);
	if(self == NULL)
		return NULL;

	self->cb.data.cmp = cmp;
	self->cb.data.acquire = acquire;
	self->cb.data.release = release;
	self->data = user;

	return self;
}

bdb_btree_t *bdb_btree_open(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name,
		int cache_buffers,
		int (*cmp)(const void *a, const void *b),
		int (*acquire)(void *a),
		void (*release)(void *a),
		int flags)
{
	bdb_btree_t *self;

	self = internal_open(env, txn, name, cache_buffers, flags);
	if(self == NULL)
		return NULL;

	if((self->options & OPT_NOCMP) != 0 && cmp != NULL) {
		errno = -EINVAL;
		goto error;
	}
	else if((self->options & OPT_NOCMP) == 0 && cmp == NULL) {
		errno = -EINVAL;
		goto error;
	}

	self->cb.nodata.cmp = cmp;
	self->cb.nodata.acquire = acquire;
	self->cb.nodata.release = release;
	return self;

error:
	self->db->close(self->db, 0);
	free(self);
	return NULL;
}

bdb_btree_t *bdb_btree_open_user(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name,
		int cache_buffers,
		int (*cmp)(const void *a, const void *b, void *user),
		int (*acquire)(void *a, void *user),
		void (*release)(void *a, void *user),
		int flags,
		void *user)
{
	bdb_btree_t *self;

	self = internal_open(env, txn, name, cache_buffers, flags);
	if(self == NULL)
		return NULL;

	if((self->options & OPT_NOCMP) != 0 && cmp != NULL) {
		errno = -EINVAL;
		goto error;
	}
	else if((self->options & OPT_NOCMP) == 0 && cmp == NULL) {
		errno = -EINVAL;
		goto error;
	}

	self->cb.data.cmp = cmp;
	self->cb.data.acquire = acquire;
	self->cb.data.release = release;
	self->data = user;
	self->options |= OPT_USE_DATA;
	return self;

error:
	self->db->close(self->db, 0);
	free(self);
	return NULL;
}

/* NOTE: valgrind reports an uninitialized value in self->db->close() (at least) if header crosses page boundary */
void bdb_btree_destroy(
		bdb_btree_t *self)
{
	cache_flush(self, NULL);
	self->db->close(self->db, 0);
	free(self);
}

int bdb_btree_flush_cache(
		bdb_btree_t *self,
		DB_TXN *txn)
{
	return cache_flush(self, txn);
}

int bdb_btree_flush_full(
		bdb_btree_t *self,
		DB_TXN *txn)
{
	int ret;
	ret = cache_flush(self, txn);
	if(ret != 0)
		return ret;
	return self->db->sync(self->db, 0);
}

/* TODO create a static trim() function which is called here and from defrag() */
/* TODO check all API methods to call cache_cleanup() before returning */
int bdb_btree_trim(
		bdb_btree_t *self,
		DB_TXN *txn)
{
	db_recno_t cur;
	db_recno_t prev;
	db_recno_t next;
	cache_t *cache;
	int ret;
	bool update = false;

	for(cur = self->max_recno; cur > REC_HEADER; cur--) {
		cache = cache_get(self, txn, cur);
		if(cache == NULL)
			return errno;
		if(buffer_get_rec(cache->buffer, OFF_FREE_MARKER) == REC_HEADER) {
			next = buffer_get_rec(cache->buffer, OFF_FREE_NEXT);
			prev = buffer_get_rec(cache->buffer, OFF_FREE_PREV);
			cache_free(cache);
			ret = erase_node(self, txn, cur);
			if(ret != 0)
				return ret;

			if(next != REC_NULL) {
				cache = cache_get(self, txn, next);
				if(cache == NULL)
					return errno;
				buffer_set_rec(cache->buffer, OFF_FREE_PREV, prev);
				cache->modified = true;
			}
			if(prev != REC_NULL) {
				cache = cache_get(self, txn, prev);
				if(cache == NULL)
					return errno;
				buffer_set_rec(cache->buffer, OFF_FREE_NEXT, next);
				cache->modified = true;
			}
			else
				self->free_list = next;
			self->max_recno--;
			update = true;
		}
		else
			break;
	}
	if(update)
		return update_header(self, txn);
	else
		return 0;
}

int bdb_btree_defrag(
		bdb_btree_t *self,
		DB_TXN *txn)
{
	uint32_t i;
	int ret;
	uint32_t ci;
	uint32_t fill;
	db_recno_t source;
	db_recno_t target;
	db_recno_t dep;
	cache_t *source_cache;
	cache_t *target_cache;
	cache_t *dep_cache;
	cache_t *header_cache;

	header_cache = cache_get(self, txn, REC_HEADER);
	if(header_cache == NULL)
		return errno;
	header_cache->refcnt++;
	ret = bdb_btree_trim(self, txn);
	if(ret != 0)
		return ret;
	while(self->free_list != REC_NULL) {
		source = self->max_recno;
		target = self->free_list;
		target_cache = cache_get(self, txn, target);
		if(target_cache == NULL) {
			header_cache->refcnt--;
			return errno;
		}
		target_cache->refcnt++;
		source_cache = cache_get(self, txn, source);
		if(source_cache == NULL) {
			target_cache->refcnt--;
			header_cache->refcnt--;
			return errno;
		}

		assert(buffer_get_rec(target_cache->buffer, OFF_FREE_MARKER) == REC_HEADER);
		self->free_list = buffer_get_rec(target_cache->buffer, OFF_FREE_NEXT);

		buffer_move_external(target_cache->buffer, 0, source_cache->buffer, 0, NODE_SIZE(self));

		/* modify parent */
		if(source == self->root)
			self->root = target;
		else {
			dep = buffer_get_rec(target_cache->buffer, OFF_NODE_PARENT);
			dep_cache = cache_get(self, txn, dep);
			if(dep_cache == NULL) {
				target_cache->refcnt--;
				header_cache->refcnt--;
				return errno;
			}
			ci = cimap_get(self, dep_cache, source);
			cimap_remove(self, dep_cache, source);
			cimap_put(self, dep_cache, target, ci);
			buffer_set_rec(dep_cache->buffer, OFF_NODE_LINK(self, ci) + OFF_NODE_LINK_CHILD, target);
			dep_cache->modified = true;
		}

		fill = buffer_get_u32(target_cache->buffer, OFF_NODE_FILL);
		if(!isleaf(self, target_cache))
			for(i = 0; i <= fill; i++) {
				dep = buffer_get_rec(target_cache->buffer, OFF_NODE_LINK(self, i) + OFF_NODE_LINK_CHILD);
				dep_cache = cache_get(self, txn, dep);
				if(dep_cache == NULL) {
					target_cache->refcnt--;
					header_cache->refcnt--;
					return errno;
				}
				buffer_set_rec(dep_cache->buffer, OFF_NODE_PARENT, target);
				dep_cache->modified = true;
			}

		target_cache->modified = true;
		target_cache->refcnt--;
		free_node(self, txn, source);
		ret = bdb_btree_trim(self, txn);
		if(ret != 0) {
			header_cache->refcnt--;
			return ret;
		}
	}
	header_cache->refcnt--;
	return update_header(self, txn); /* TODO better update_header policy: just once for a single defrag() call; also in trim() method (see also comment at beginning of current trim()) */
}

/* TODO everywhere: on errno return, check (assert) if errno < 0 */
/* TODO datatype for index/size */
int bdb_btree_size(
		bdb_btree_t *self,
		DB_TXN *txn)
{
	uint32_t n = 0;
	uint32_t i;
	cache_t *root_cache;
	uint32_t root_fill;

	if(self->root == REC_NULL)
		return n;

	root_cache = cache_get(self, txn, self->root);
	if(root_cache == NULL) {
		assert(errno < 0);
		return errno;
	}
	root_fill = buffer_get_u32(root_cache->buffer, OFF_NODE_FILL);

	for(i = 0; i <= root_fill; i++)
		n += buffer_get_u32(root_cache->buffer, OFF_NODE_LINK(self, i) + OFF_NODE_LINK_COUNT);
	n += root_fill;
	return n;
}

int bdb_btree_insert(
		bdb_btree_t *self,
		DB_TXN *txn,
		void *element)
{
	db_recno_t node;
	int pos;
	bool found;
	int ret;

	found = find_upper(self, txn, element, &node, &pos);
	if(!found && errno != 0)
		return cache_cleanup(self, txn, errno);
	ret = to_insert_before(self, txn, &node, &pos);
	if(ret != 0)
		return cache_cleanup(self, txn, ret);
	else if((self->options & BTREE_OPT_MULTI_KEY) != 0 || !found)
		return cache_cleanup(self, txn, node_insert(self, txn, node, pos, element));
	else
		return cache_cleanup(self, txn, -EALREADY);
}

int bdb_btree_update(
		bdb_btree_t *self,
		DB_TXN *txn,
		int index,
		void *element)
{
	db_recno_t node;
	int pos;
	int size;
	void *stored;
	cache_t *node_cache;

	size = bdb_btree_size(self, txn);
	if(size < 0)
		return cache_cleanup(self, txn, size);
	else if(index < 0 || index >= size)
		return cache_cleanup(self, txn, -EOVERFLOW);

	if(!find_index(self, txn, index, &node, &pos)) {
		if(errno == 0)
			return cache_cleanup(self, txn, -ENOENT);
		else
			return cache_cleanup(self, txn, errno);
	}
	node_cache = cache_get(self, txn, node);
	if(node_cache == NULL)
		return cache_cleanup(self, txn, errno);
	stored = node_cache->buffer + OFF_NODE_ELEMENT(self, pos);

	if((self->options & OPT_NOCMP) == 0 && CMP_E(self, element, stored) != 0)
		return cache_cleanup(self, txn, -EINVAL);
	return cache_cleanup(self, txn, node_replace(self, txn, node, pos, element));
}

int bdb_btree_insert_at(
		bdb_btree_t *self,
		DB_TXN *txn,
		int index,
		void *element)
{
	db_recno_t node;
	int pos;
	int ret;
	int size;
	
	if((self->options & OPT_NOCMP) == 0) /* insert by index only if cmp is not present */
		return -EINVAL;

	size = bdb_btree_size(self, txn);
	if(size < 0)
		return cache_cleanup(self, txn, size);
	else if(index < 0 || index > size)
		return cache_cleanup(self, txn, -EOVERFLOW);

	if(!find_index(self, txn, index, &node, &pos)) {
		if(errno != 0) {
			assert(errno < 0);
			return cache_cleanup(self, txn, errno);
		}
	}
	ret = to_insert_before(self, txn, &node, &pos);
	if(ret != 0) {
		assert(ret < 0);
		return cache_cleanup(self, txn, ret);
	}

	return cache_cleanup(self, txn, node_insert(self, txn, node, pos, element));
}

int bdb_btree_remove(
		bdb_btree_t *self,
		DB_TXN *txn,
		void *element)
{
	int pos;
	db_recno_t node;

	assert(self->overflow_node == REC_NULL);

//	if(self->options & OPT_NOCMP) /* remove by key only if cmp is present */
//		return -EINVAL;

	if(!find_lower(self, txn, element, &node, &pos))
		return cache_cleanup(self, txn, -ENOENT);
	else
		return cache_cleanup(self, txn, node_remove(self, txn, node, pos));
}

int bdb_btree_remove_at(
		bdb_btree_t *self,
		DB_TXN *txn,
		int index)
{
	db_recno_t node;
	int pos;
	int size;
	
	size = bdb_btree_size(self, txn);
	if(size < 0)
		return cache_cleanup(self, txn, size);
	else if(index < 0 || index >= size)
		return cache_cleanup(self, txn, -EOVERFLOW);

	if(!find_index(self, txn, index, &node, &pos)) {
		if(errno == 0)
			errno = -ENOENT;
		return errno;
	}
	return cache_cleanup(self, txn, node_remove(self, txn, node, pos));
}

void *bdb_btree_get_at(
		bdb_btree_t *self,
		DB_TXN *txn,
		int index)
{
	db_recno_t node;
	cache_t *node_cache;
	int pos;
	int size;

	size = bdb_btree_size(self, txn);
	if(size < 0) {
		errno = size;
		return NULL;
	}

	if(index < 0 || index >= size) {
		errno = -EINVAL;
		return NULL;
	}

	if(!find_index(self, txn, index, &node, &pos)) {
		if(errno == 0)
			errno = -ENOENT;
		return NULL;
	}
	node_cache = cache_get(self, txn, node);
	if(node_cache == NULL)
		return NULL;
	return node_cache->buffer + OFF_NODE_ELEMENT(self, pos);
}

int bdb_btree_put_at(
		bdb_btree_t *self,
		DB_TXN *txn,
		int index,
		void *element)
{
	db_recno_t node;
	int pos;
	int ret;
	int size;
	
	if((self->options & OPT_NOCMP) == 0) /* insert by index only if cmp is not present */
		return -EINVAL;

	size = bdb_btree_size(self, txn);
	if(size < 0)
		return cache_cleanup(self, txn, size);
	else if(index < 0 || index > size)
		return cache_cleanup(self, txn, -EOVERFLOW);

	if(!find_index(self, txn, index, &node, &pos)) {
		if(errno != 0) {
			assert(errno < 0);
			return cache_cleanup(self, txn, errno);
		}
		/* append */
		ret = to_insert_before(self, txn, &node, &pos);
		if(ret != 0) {
			assert(ret < 0);
			return cache_cleanup(self, txn, ret);
		}
		return cache_cleanup(self, txn, node_insert(self, txn, node, pos, element));
	}
	else
		return cache_cleanup(self, txn, node_replace(self, txn, node, pos, element));
}

int bdb_btree_find_at(
		bdb_btree_t *self,
		DB_TXN *txn,
		int index,
		bdb_btree_it_t *it)
{
	db_recno_t node;
	int pos;
	cache_t *node_cache;

	if(find_index(self, txn, index, &node, &pos)) {
		if(it != NULL) {
			memset(it, 0, sizeof(*it));
			node_cache = cache_get(self, txn, node);
			if(node_cache == NULL)
				return cache_cleanup(self, txn, errno);
			it->element = node_cache->buffer + OFF_NODE_ELEMENT(self, pos);
			it->index = index;
			it->tree = self;
			it->node = node;
			it->pos = pos;
		}
		return index;
	}
	else if(errno != 0) {
		assert(errno < 0);
		return cache_cleanup(self, txn, errno);
	}
	else
		return cache_cleanup(self, txn, -ENOENT);
}

int bdb_btree_find_begin(
		bdb_btree_t *self,
		DB_TXN *txn,
		bdb_btree_it_t *it)
{
	db_recno_t child = self->root;
	db_recno_t node = child;
	cache_t *node_cache;

	while(child != REC_NULL) {
		node = child;
		node_cache = cache_get(self, txn, node);
		if(node_cache == NULL)
			return cache_cleanup(self, txn, errno);
		child = buffer_get_rec(node_cache->buffer, OFF_NODE_LINK(self, 0) + OFF_NODE_LINK_CHILD);
	}
	if(it != NULL) {
		memset(it, 0, sizeof(*it));
		if(node == REC_NULL)
			it->element = NULL;
		else
			it->element = node_cache->buffer + OFF_NODE_ELEMENT(self, 0);
		it->index = 0;
		it->tree = self;
		it->node = node;
		it->pos = 0;
	}
	return cache_cleanup(self, txn, 0);
}

int bdb_btree_find_end(
		bdb_btree_t *self,
		DB_TXN *txn,
		bdb_btree_it_t *it)
{
	db_recno_t node = self->root;
	db_recno_t child;
	cache_t *node_cache;
	int index = 0;
	int i;
	int node_fill;

	if(it != NULL) {
		memset(it, 0, sizeof(*it));
		it->element = NULL;
		it->index = 0;
		it->tree = self;
		it->node = REC_NULL;
		it->pos = 0;
	}
	if(node == REC_NULL)
		return 0;

	node_cache = cache_get(self, txn, node);
	if(node_cache == NULL)
		return cache_cleanup(self, txn, errno);
	node_fill = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL);
	index += node_fill;
	for(i = 0; i <= node_fill; i++)
		index += buffer_get_u32(node_cache->buffer, OFF_NODE_LINK(self, i) + OFF_NODE_LINK_COUNT);

	child = buffer_get_rec(node_cache->buffer, OFF_NODE_LINK(self, node_fill) + OFF_NODE_LINK_CHILD);
	while(child != REC_NULL) {
		node = child;
		node_cache = cache_get(self, txn, node);
		if(node_cache == NULL)
			return cache_cleanup(self, txn, errno);
		node_fill = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL);
		child = buffer_get_rec(node_cache->buffer, OFF_NODE_LINK(self, node_fill) + OFF_NODE_LINK_CHILD);
	}
	if(it != NULL) {
		it->index = index;
		it->node = node;
		it->pos = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL);
	}
	return index;
}

int bdb_btree_find_lower(
		bdb_btree_t *self,
		DB_TXN *txn,
		const void *key,
		bdb_btree_it_t *it)
{
	db_recno_t node;
	cache_t *node_cache;
	int node_fill;
	int pos;
	int index;

	if(self->options & OPT_NOCMP)
		return -EINVAL;

	find_lower(self, txn, key, &node, &pos);
	if(errno != 0)
		return cache_cleanup(self, txn, errno);
	index = to_index(self, txn, node, pos);
	if(index < 0)
		return cache_cleanup(self, txn, index);
	if(it != NULL) {
		memset(it, 0, sizeof(*it));
		it->tree = self;
		it->pos = pos;
		it->node = node;
		it->index = index;
		if(node == REC_NULL)
			it->element = NULL;
		else {
			node_cache = cache_get(self, txn, node);
			if(node_cache == NULL)
				return cache_cleanup(self, txn, errno);
			node_fill = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL);
			if(pos == node_fill)
				it->element = NULL;
			else
				it->element = node_cache->buffer + OFF_NODE_ELEMENT(self, pos);
		}
	}
	return cache_cleanup(self, txn, index);
}

int bdb_btree_find_upper(
		bdb_btree_t *self,
		DB_TXN *txn,
		const void *key,
		bdb_btree_it_t *it)
{
	API_FIND_LIM_IMPL(find_upper(self, txn, key, &node, &pos))
}

int bdb_btree_find_lower_set(
		bdb_btree_t *self,
		DB_TXN *txn,
		const void *key,
		int (*cmpfn)(const void *a, const void *b),
		bdb_btree_it_t *it)
{
	API_FIND_LIM_IMPL(find_lower_custom(self, txn, key, cmpfn, &node, &pos))
}

int bdb_btree_find_lower_set_user(
		bdb_btree_t *self,
		DB_TXN *txn,
		const void *key,
		int (*cmpfn)(const void *a, const void *b, void *data, void *cmpdata),
		void *cmpdata,
		bdb_btree_it_t *it)
{
	API_FIND_LIM_IMPL(find_lower_custom_user(self, txn, key, cmpfn, cmpdata, &node, &pos))
}

int bdb_btree_find_upper_set(
		bdb_btree_t *self,
		DB_TXN *txn,
		const void *key,
		int (*cmpfn)(const void *a, const void *b),
		bdb_btree_it_t *it)
{
	API_FIND_LIM_IMPL(find_upper_custom(self, txn, key, cmpfn, &node, &pos))
}

int bdb_btree_find_upper_set_user(
		bdb_btree_t *self,
		DB_TXN *txn,
		const void *key,
		int (*cmpfn)(const void *a, const void *b, void *data, void *cmpdata),
		void *cmpdata,
		bdb_btree_it_t *it)
{
	API_FIND_LIM_IMPL(find_upper_custom_user(self, txn, key, cmpfn, cmpdata, &node, &pos))
}

/* TODO this one does not work correctly, fix bugs */
int bdb_btree_iterate_next(
		bdb_btree_it_t *it,
		DB_TXN *txn)
{
	int pos = it->pos;
	db_recno_t node = it->node;
	cache_t *node_cache;
	bool has;
	int node_fill;

	has = to_next(it->tree, txn, &node, &pos);
	if(errno != 0) {
		assert(errno < 0);
		return cache_cleanup(it->tree, txn, errno);
	}
	if(!has)
		return cache_cleanup(it->tree, txn, -ENOENT);

	node_cache = cache_get(it->tree, txn, node);
	if(node_cache == NULL)
		return cache_cleanup(it->tree, txn, errno);
	node_fill = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL);
	it->index++;
	if(pos == node_fill)
		it->element = NULL;
	else
		it->element = node_cache->buffer + OFF_NODE_ELEMENT(it->tree, pos);
	it->pos = pos;
	it->node = node;
	return cache_cleanup(it->tree, txn, it->index);
}

int bdb_btree_iterate_prev(
		bdb_btree_it_t *it,
		DB_TXN *txn)
{
	int pos = it->pos;
	db_recno_t node = it->node;
	cache_t *node_cache;
	bool has;

	has = to_prev(it->tree, txn, &node, &pos);
	if(errno != 0) {
		assert(errno < 0);
		return cache_cleanup(it->tree, txn, errno);
	}
	if(!has)
		return cache_cleanup(it->tree, txn, -ENOENT);

	node_cache = cache_get(it->tree, txn, node);
	if(node_cache == NULL)
		return cache_cleanup(it->tree, txn, errno);
	it->element = node_cache->buffer + OFF_NODE_ELEMENT(it->tree, pos);
	it->index--;
	it->pos = pos;
	it->node = node;
	return cache_cleanup(it->tree, txn, it->index);

}

int bdb_btree_iterate_refresh(
		bdb_btree_it_t *it,
		DB_TXN *txn)
{
	cache_t *node_cache;

	node_cache = cache_get(it->tree, txn, it->node);
	if(node_cache == NULL)
		return cache_cleanup(it->tree, txn, errno);
	it->element = node_cache->buffer + OFF_NODE_ELEMENT(it->tree, it->pos);
	return 0;
}

void bdb_btree_dump(
		bdb_btree_t *self,
		DB_TXN *txn,
		void (*print)(const void *element))
{
	dump_tree(self, txn, print);
}

static void dump_node(
		int indent,
		bdb_btree_t *tree,
		DB_TXN *txn,
		db_recno_t node,
		void (*print)(const void *element))
{
	int i;
	int k;
	cache_t *node_cache;
	cache_t *child_cache;
	uint32_t node_fill;
	link_t link;

	node_cache = cache_get(tree, txn, node);
	if(node_cache == NULL) {
		printf("ERROR GETTING NODE %d\n", node);
		return;
	}
	node_cache->refcnt++;

	node_fill = buffer_get_u32(node_cache->buffer, OFF_NODE_FILL);
	for(i = 0; i < node_fill;/*MIN(node->fill, tree->order - 1);*/ i++) {
		printf("| ");
		print(node_cache->buffer + OFF_NODE_ELEMENT(tree, i));
		printf(" ");
	}
	for(i = node_fill; i < tree->order - 1; i++)
		printf("| --- ");
	if(tree->overflow_node == node) {
		printf("# ");
		print(tree->overflow_element);
		printf(" |\n");
	}
	else
		printf("|\n");
	for(i = 0; i <= node_fill; i++) {
		bool processed = false;

		buffer_to_link(&link, node_cache->buffer, OFF_NODE_LINK(tree, i));
		if(link.child != REC_NULL) {
			child_cache = cache_get(tree, txn, link.child);
			if(child_cache == NULL) {
				printf("ERROR GETTING CHILD NODE %d\n", link.child);
				node_cache->refcnt--;
				return;
			}
			if(buffer_get_u32(child_cache->buffer, OFF_NODE_FILL) > 0) {
				for(k = 0; k <= indent; k++)
					printf("  ");
				printf("[%3d %3d] ", link.offset, link.count);
				for(k = 0; k < i; k++)
					printf("-");
				printf("+");
				for(k = i + 1; k < tree->order; k++)
					printf("-");
				if(buffer_get_rec(child_cache->buffer, OFF_NODE_PARENT) != node)
					printf(" parent=INVALID");
				if(cimap_get(tree, node_cache, link.child) != i)
					printf(" child_index=INVALID (expected=%d, found=%d)", i, cimap_get(tree, node_cache, link.child));
				printf("  ");

				node_cache->refcnt--;
				dump_node(indent + 1, tree, txn, link.child, print);
				node_cache = cache_get(tree, txn, node);
				if(node_cache == NULL) {
					printf("ERROR GETTING NODE %d\n", node);
					node_cache->refcnt--;
					return;
				}
				node_cache->refcnt++;
				processed = true;
			}
		}
		if(!processed) {
			for(k = 0; k <= indent; k++)
				printf("  ");
			printf("[%3d %3d] ", link.offset, link.count);
			for(k = 0; k < i; k++)
				printf("-");
			printf("+");
			for(k = i + 1; k < tree->order; k++)
				printf("-");
			printf("\n");
		}
	}
	if(tree->overflow_node == node) {
		bool processed = false;
		buffer_to_link(&link, tree->overflow_link, 0);
		if(link.child != REC_NULL) {
			child_cache = cache_get(tree, txn, link.child);
			if(child_cache == NULL) {
				printf("ERROR GETTING OVERFLOW CHILD NODE %d\n", link.child);
				node_cache->refcnt--;
				return;
			}
			if(buffer_get_u32(child_cache->buffer, OFF_NODE_FILL) > 0) {
				for(k = 0; k <= indent; k++)
					printf("  ");
				printf("[%3d %3d] ", link.offset, link.count);
				for(k = 0; k < tree->order - 1; k++)
					printf(" ");
				printf("#");
				printf("  ");
				node_cache->refcnt--;
				dump_node(indent + 1, tree, txn, link.child, print);
				processed = true;
			}
		}
		if(!processed)
			node_cache->refcnt--;
	}
	else
		node_cache->refcnt--;
}

static void dump_tree(
		bdb_btree_t *tree,
		DB_TXN *txn,
		void (*print)(const void *element))
{
	if(tree->root == REC_NULL)
		return;
	dump_node(0, tree, txn, tree->root, print);
}

static void dump_cache(
		bdb_btree_t *tree,
		cache_t *cache)
{
	int i;

	printf("CACHE DUMP: index=%d refcnt=%d recno=%d modified=%c\n", (int)(cache - tree->cache), cache->refcnt, cache->recno, cache->modified ? 'Y' : 'N');
	for(i = 0; i < NODE_SIZE(tree); i++) {
		if(i % 16 == 0)
			printf("\n");
		printf("%02x ", (unsigned char)cache->buffer[i]);
	}
	printf("\n");
}

