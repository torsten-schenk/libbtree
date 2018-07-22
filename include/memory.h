#ifndef _BTREE_MEMORY_H
#define _BTREE_MEMORY_H

#include <stdbool.h>
#include <stdio.h>

#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

/* TODO version 2 of btree:
 * - use size_t instead of int
 * - calbacks: hand over btree_data() instead of btree_t
 * - alignment alloc
 * - remove iterator: when removing a consecutive series of elements, improve performance and handyness with a remove iterator */
	
/* TODO all methods: return -1/NULL and use errno in case of error */

typedef struct btree btree_t;
typedef struct btree_node btree_node_t;
typedef int (*btree_cmp_t)(btree_t *btree, const void *a, const void *b, void *group);
typedef int (*btree_acquire_t)(btree_t *btree, void *element);
typedef void (*btree_release_t)(btree_t *btree, void *element);

/*
 * best practices when using keys (i.e. a compare function is given):
 * - make the values stored a struct:
 *     struct my_element {
 *       struct my_key;
 *       ... other members
 *     }
 * - implement the compare function as follows:
 *     int cmp(const void *a_, const void *b_) {
 *       const struct my_key *a = a_;
 *       const struct my_key *b = b_;
 *       ... compare a and b
 *     }
 * - this allows to hand over just a key struct to the lookup/modify/delete functions.
 * - additionally, if the key is an n-tuple (n > 1), arrange the members in my_key in
 *   descending significance. this allows the group functions to specify a compare function
 *   which only compares the most k (k <= n) significant members and therefore grouping together
 *   multiple elements
 * NOTE (as a conclusion): every argument named 'key' must be of a type which the corresponding
 * compare function can handle. the btree functions won't access any data on the pointer so
 * they do not need to be aware of the size. every argument named 'element' needs to contain
 * the whole structure, i.e. one of size 'element_size' as specified in 'btree_new'.
 */


/* may be copied; remains valid until next remove/insert operation */
typedef struct {
	void *element;
	int index;
	bool found; /* indicator, whether exact match has been found. undefined for find_end() and iterate_prev() */

	/* private */
	btree_t *tree;
	btree_node_t *node;
	int pos;
} btree_it_t;

/* sets errno in case NULL is returned;
 * creates a btree that stores values, so 'a' and 'b'
 * of cmp callback will point to the values within
 * the btree.
 * if element_size is set to -1, the btree
 * will store pointers instead of values.
 * this implies that pointers returned by lookup functions won't change
 * due to a delete or insert function.
 * it also implies that the pointer handed over to the insert function
 * must remain valid until the element is removed from the btree. */
btree_t *btree_new(
		int order,
		int element_size,
		btree_cmp_t cmp, /* when using an external element, it is guaranteed to be the second operand. */
		int options);

/* 'write' writes btree-specific serialization data to the desired output stream.
 * 'size' returns the serialized size of the given element.
 * 'serialize' writes the 'element' to the output stream. note that this function is expected to write exactly 'size'(element, user) bytes
 * */
int btree_write(
		btree_t *self,
		size_t (*size)(const void *element, void *user),
		int (*serialize)(const void *element, void *user),
		int (*write)(const void *si, size_t size, void *user), /* returns: 0 on success, custom error otherwise; when finished, called with si = NULL */
		void *user);

/* use this function if each element has a fixed serialized size */
int btree_write_fixed(
		btree_t *self,
		size_t element_size,
		int (*serialize)(const void *element, void *user),
		int (*write)(const void *si, size_t size, void *user), /* returns: 0 on success, custom error otherwise; when finished, called with si = NULL */
		void *user);

btree_t *btree_read(
		int (*read)(void *di, size_t size, void *user),
		void *user);

uint64_t btree_memory_total(
		btree_t *self);

uint64_t btree_memory_payload(
		btree_t *self);

void btree_set_data(
		btree_t *self,
		void *data);

void *btree_data(
		btree_t *self);

void btree_set_group_default(
		btree_t *self,
		void *group);

void *btree_group_default(
		btree_t *self);

void btree_sethook_subelement(
		btree_t *self,
		int (*size)(btree_t *btree, const void *element),
		void *(*sub)(btree_t *btree, void *element, int index));

void btree_sethook_refcount(
		btree_t *self,
		btree_acquire_t acquire,
		btree_release_t release);

int btree_clear(
		btree_t *self);

void btree_destroy(
		btree_t *self);

/* after calling this function, no further insertions/deletions are possible.
 * it is also ensured, that the pointers returned by btree_get() and other methods
 * will be valid until the btree instance is destroyed */
void btree_finalize(
		btree_t *self);

int btree_is_finalized(
		btree_t *self);

/*int btree_index_of(
		btree_t *self,
		const void *element);*/

bool btree_contains(
		btree_t *self,
		const void *key);

int btree_swap(
		btree_t *self,
		int index_a,
		int index_b);

/* find lower bound using custom compare function.
 * NOTE: the custom compare function may group together
 * multiple elements BUT foreach group g1 and g2
 * it must hold: g1 < g2 <=> all elements within g1 < all elements within g2 */
/*btree_group_lower(
		btree_t *self,
		const void *key,
		int (*cmp)(const void *a, const void *b));*/
/*btree_group_upper(
		btree_t *self,
		const void *key,
		int (*cmp)(const void *a, const void *b));*/

/* insert a new element. copies the given element data into the new slot. */
int btree_insert(
		btree_t *self,
		void *element);

int btree_insert_at(
		btree_t *self,
		int index,
		void *element);

/* insert/replace an element. same as btree_insert but replaces existing elements
 * in case of MULTI_KEY: if no element exists, a new one is inserted.
 * if at least one element already exists, the FIRST one will be replaced */
int btree_put(
		btree_t *self,
		void *element);

int btree_put_at(
		btree_t *self,
		int index,
		void *element);

/* removes an element from the tree.
 * in case of MULTI_KEY: remove just the FIRST one, even if multiple elements exist */
int btree_remove(
		btree_t *self,
		void *element);

int btree_remove_at(
		btree_t *self,
		int index);

int btree_remove_group(
		btree_t *self,
		const void *key,
		void *group);

int btree_remove_range(
		btree_t *self,
		int l,
		int u);

int btree_size(
		btree_t *self);

/* removes all occurences of an element from the tree.
 * returns number of elements removed */
/*int btree_remove_all(
		btree_t *self,
		void *element);*/

/* NOTE: the returned pointer is only guaranteed to be valid until
 * any of insert/delete functions have been called EXCEPT when btree_new_ptr was
 * used to create the btree.x
 * in case of MULTI_KEY: if multiple elements exist, the
 * FIRST one is returned (i.e. get() and put() operate on the same element) */
void *btree_get(
		btree_t *self,
		const void *key);

void *btree_get_at(
		btree_t *self,
		int index);

/* insert a new element. reserve a new slot at a position being fit for 'key'
 * BUT do not copy any data. The caller is responsible for filling the key
 * appropriately.
 * NOTE: the returned pointer is only guaranteed to be valid until
 * any of insert/delete functions have been called EXCEPT when btree_new_ptr was
 * used to create the btree.
 * sets errno in case NULL is returned */
/*void *btree_insert_key(
		btree_t *self,
		const void *key);*/

/* find functions return index or -ENOENT in case nothing was found.
 * 'it' may be NULL */
int btree_find_at(
		btree_t *self,
		int index,
		btree_it_t *it);

/* set iterator to first element in btree. if btree is empty, btree_find_end() is returned.
 * returns the index (always 0) */
int btree_find_begin(
		btree_t *self,
		btree_it_t *it);

/* set iterator to first imaginary element after last element. the returned index
 * also equals the number of elements in the btree. */
int btree_find_end(
		btree_t *self,
		btree_it_t *it);

/* return: iterator points to the first element being >= key and returns index.
 * if all elements are < key, btree_size() is returned. */
int btree_find_lower(
		btree_t *self,
		const void *key,
		btree_it_t *it);

int btree_find_upper(
		btree_t *self,
		const void *key,
		btree_it_t *it);

int btree_find_lower_cmp(
		btree_t *self,
		btree_cmp_t cmp,
		const void *key,
		btree_it_t *it);

int btree_find_upper_cmp(
		btree_t *self,
		btree_cmp_t cmp,
		const void *key,
		btree_it_t *it);

/* returns the first element a in tree for which holds a >= key using given cmp function */
int btree_find_lower_group(
		btree_t *self,
		const void *key,
		void *group,
		btree_it_t *it);

/* returns the last element a in tree for which holds a <= key using given cmp function */
int btree_find_upper_group(
		btree_t *self,
		const void *key,
		void *group,
		btree_it_t *it);

int btree_find_lower_group_cmp(
		btree_t *self,
		btree_cmp_t cmp,
		const void *key,
		void *group,
		btree_it_t *it);

int btree_find_upper_group_cmp(
		btree_t *self,
		btree_cmp_t cmp,
		const void *key,
		void *group,
		btree_it_t *it);

/* returns the first element a in tree for which holds a >= key using given cmp function */
int btree_find_lower_group_in(
		btree_t *self,
		int l,
		int u,
		const void *key,
		void *group,
		btree_it_t *it);

/* returns the last element a in tree for which holds a <= key using given cmp function */
int btree_find_upper_group_in(
		btree_t *self,
		int l,
		int u,
		const void *key,
		void *group,
		btree_it_t *it);

int btree_find_lower_group_in_cmp(
		btree_t *self,
		btree_cmp_t cmp,
		int l,
		int u,
		const void *key,
		void *group,
		btree_it_t *it);

/* returns the last element a in tree for which holds a <= key using given cmp function */
int btree_find_upper_group_in_cmp(
		btree_t *self,
		btree_cmp_t cmp,
		int l,
		int u,
		const void *key,
		void *group,
		btree_it_t *it);

/* call this function after the element (at least its key) has been modified.
 * this function checks, whether the element is valid at the iterator's position */
int btree_validate_modified(
		btree_it_t *it);

/* return -ENOENT when trying to process btree_find_end(). */
int btree_iterate_next(
		btree_it_t *it);

/* return -ENOENT when trying to process btree_find_begin(). */
int btree_iterate_prev(
		btree_it_t *it);

void btree_dump(
		btree_t *self,
		void (*print)(const void *element));

#ifdef __cplusplus
}
#endif

#endif

