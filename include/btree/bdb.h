#pragma once

#include <stdbool.h>
#include <db.h>

#include <btree/common.h>

typedef struct bdb_store bdb_store_t;
typedef struct bdb_btree bdb_btree_t;

/* may be copied; remains valid until next remove/insert operation */
typedef struct {
	void *element; /* valid until next operation; NOTE: including find/get; use bdb_btree_iterate_refresh() to refresh this field */
	int index;

	/* private */
	bdb_btree_t *tree;
	db_recno_t node;
	int pos;
} bdb_btree_it_t;

/* sets errno on NULL return */
bdb_store_t *bdb_store_create(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name);

/* sets errno on NULL return */
bdb_store_t *bdb_store_open(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name,
		int flags);

void bdb_store_destroy(
		bdb_store_t *self);

int bdb_store_flush(
		bdb_store_t *self,
		DB_TXN *txn);

/*int bdb_store_reopen(
		bdb_store_t *self);

int bdb_store_close(
		bdb_store_t *self);*/

/* get an entry; if no such entry exists, create a new one and return its id
 * 0 return: error; sets errno in that case; if errno == SUCCESS, 'len' was 0 */
db_recno_t bdb_store_get(
		bdb_store_t *self,
		DB_TXN *txn,
		const void *data,
		int len,
		bool incref); /* incref: specify whether refcount shall be increased by/set to 1 */

/* try to get an entry; same as _get(), but does not create a new entry if none is found
 * 0 return: error; sets errno in that case; if errno == SUCCESS, there is nothing wrong, but the entry simply does not exist */
db_recno_t bdb_store_try(
		bdb_store_t *self,
		DB_TXN *txn,
		const void *data,
		int len);

/* return size of entry; in case of an error, return 0 and set errno */
int bdb_store_size(
		bdb_store_t *self,
		DB_TXN *txn,
		db_recno_t entry);

int bdb_store_data(
		bdb_store_t *self,
		DB_TXN *txn,
		db_recno_t entry,
		void *data,
		int offset,
		int size);

int bdb_store_acquire(
		bdb_store_t *self,
		DB_TXN *txn,
		db_recno_t entry,
		uint32_t amount);

int bdb_store_release(
		bdb_store_t *self,
		DB_TXN *txn,
		db_recno_t entry,
		uint32_t amount);

int bdb_btree_exists(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name);

bdb_btree_t *bdb_btree_create(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name,
		int cache_buffers, /* -1: default value, 0: minimum value */
		int order,
		int element_size,
		int (*cmp)(const void *a, const void *b),
		int (*acquire)(void *a),
		void (*release)(void *a),
		uint32_t options);

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
		void *user);

bdb_btree_t *bdb_btree_open(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name,
		int cache_buffers, /* -1: default value, 0: minimum value */
		int (*cmp)(const void *a, const void *b),
		int (*acquire)(void *a),
		void (*release)(void *a),
		int flags);

bdb_btree_t *bdb_btree_open_user(
		DB_ENV *env,
		DB_TXN *txn,
		const char *name,
		int cache_buffers, /* -1: default value, 0: minimum value */
		int (*cmp)(const void *a, const void *b, void *user),
		int (*acquire)(void *a, void *user),
		void (*release)(void *a, void *user),
		int flags,
		void *user);

/* reload the tree and free the cache.
 * call this method after a transaction used for any method here has been aborted. */
int bdb_btree_reload(
		bdb_btree_t *self,
		DB_TXN *txn);

void bdb_btree_destroy(
		bdb_btree_t *self);

int bdb_btree_size(
		bdb_btree_t *self,
		DB_TXN *txn);

int bdb_btree_insert(
		bdb_btree_t *self,
		DB_TXN *txn,
		void *element);

int bdb_btree_insert_at(
		bdb_btree_t *self,
		DB_TXN *txn,
		int index,
		void *element);

int bdb_btree_remove(
		bdb_btree_t *self,
		DB_TXN *txn,
		void *element);

int bdb_btree_remove_at(
		bdb_btree_t *self,
		DB_TXN *txn,
		int index);

/* returned pointer guaranteed to be valid just until next
 * method call on same btree */
void *bdb_btree_get_at(
		bdb_btree_t *self,
		DB_TXN *txn,
		int index);

int bdb_btree_put_at(
		bdb_btree_t *self,
		DB_TXN *txn,
		int index,
		void *element);

int bdb_btree_flush_cache(
		bdb_btree_t *self,
		DB_TXN *txn);

int bdb_btree_flush_full(
		bdb_btree_t *self,
		DB_TXN *txn);

/* remove trailing unused records */
int bdb_btree_trim(
		bdb_btree_t *self,
		DB_TXN *txn);

/* remove all unused records */
int bdb_btree_defrag(
		bdb_btree_t *self,
		DB_TXN *txn);

void bdb_btree_dump(
		bdb_btree_t *self,
		DB_TXN *txn,
		void (*print)(const void *element));

int bdb_btree_find_at(
		bdb_btree_t *self,
		DB_TXN *txn,
		int index,
		bdb_btree_it_t *it);

int bdb_btree_find_begin(
		bdb_btree_t *self,
		DB_TXN *txn,
		bdb_btree_it_t *it);

int bdb_btree_find_end(
		bdb_btree_t *self,
		DB_TXN *txn,
		bdb_btree_it_t *it);

int bdb_btree_find_lower(
		bdb_btree_t *self,
		DB_TXN *txn,
		const void *key,
		bdb_btree_it_t *it);

int bdb_btree_find_upper(
		bdb_btree_t *self,
		DB_TXN *txn,
		const void *key,
		bdb_btree_it_t *it);

int bdb_btree_find_lower_set(
		bdb_btree_t *self,
		DB_TXN *txn,
		const void *key,
		int (*cmpfn)(const void *a, const void *b),
		bdb_btree_it_t *it);

int bdb_btree_find_lower_set_user(
		bdb_btree_t *self,
		DB_TXN *txn,
		const void *key,
		int (*cmpfn)(const void *a, const void *b, void *data, void *cmpdata),
		void *cmpdata,
		bdb_btree_it_t *it);

int bdb_btree_find_upper_set(
		bdb_btree_t *self,
		DB_TXN *txn,
		const void *key,
		int (*cmpfn)(const void *a, const void *b),
		bdb_btree_it_t *it);

int bdb_btree_find_upper_set_user(
		bdb_btree_t *self,
		DB_TXN *txn,
		const void *key,
		int (*cmpfn)(const void *a, const void *b, void *data, void *cmpdata),
		void *cmpdata,
		bdb_btree_it_t *it);

int bdb_btree_iterate_next(
		bdb_btree_it_t *it,
		DB_TXN *txn);

int bdb_btree_iterate_prev(
		bdb_btree_it_t *it,
		DB_TXN *txn);

int bdb_btree_iterate_refresh(
		bdb_btree_it_t *it,
		DB_TXN *txn);

