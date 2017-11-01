#include <endian.h>
#include <stdint.h>
#include <assert.h>
#include <stdio.h>
#include <stdbool.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>

#include <btree/memory.h>

/* TODO possible memory usage optimization:
 * different size for leaf nodes and itermediate nodes: leafs don't need link offset/count */

enum {
	OPT_NOCMP = 0x01000000, /* no compare function given, use indices only */
	OPT_USE_POINTERS = 0x02000000, /* pointers are stored (element_size = -1 in ctor) */
	OPT_FINALIZED = 0x04000000 /* btree_finalize() has been called, no further insertions/deletions */
};

enum {
	WALK_END = 0,
	WALK_DESCEND,
	WALK_ASCEND
};

static const char *MAGIC = "btreeser";

#define MIN(X, Y) ((X) < (Y) ? (X) : (Y))
#define MAX(X, Y) ((X) > (Y) ? (X) : (Y))

/* set element to a pointer value */
#define SET_EP(TREE, E, V) \
	do { \
		if(TREE->options & OPT_USE_POINTERS) \
			*(const void**)(E) = V; \
		else \
			memcpy(E, V, TREE->element_size); \
	} while(false)

/* get element pointer value */
#define GET_E(TREE, E) \
	((TREE->options & OPT_USE_POINTERS) ? *(void**)(E) : (E))

static void dump_tree(
		btree_t *tree,
		void (*print)(const void *element));

static void dump_node(
		int indent,
		btree_t *tree,
		btree_node_t *node,
		void (*print)(const void *element));

typedef struct {
	int count;
	int offset;
	btree_node_t *child;
} btree_link_t;

#ifdef TESTING
/* testing: some structures are set up manually, so the usual
 * btree_clear()/btree_destroy() won't free those nodes.
 * to free them anyway, use a linked list of all allocated nodes
 * and use function test_nodes_destroy() in btree_common.h.
 * note that a possible free callback will not be invoked for
 * these nodes, just for the ones that are consistent with
 * the general btree structure (those which don't have sibling gaps). */
static btree_node_t *last_node_alloc = NULL;
#endif

typedef struct {
	btree_t *self;
	int error;
	void *user;
	int count;
	union {
		int (*write)(const void *si, size_t size, void *user);
		int (*read)(void *di, size_t size, void *user);
	} f;
	union {
		int (*serialize)(const void *element, void *user);
	} s;
	size_t fixed_size; /* each element has this size, if get_size == NULL */
	size_t (*get_size)(const void *element, void *user);
} io_context_t;

struct btree_node {
	btree_node_t *parent;
	int child_index;
	int fill; /* number of elements in node */
	btree_link_t *links; /* 'order' links */
	void *elements; /* 'order' - 1 elements */
#ifdef TESTING
	btree_node_t *prev_alloc;
	btree_node_t *next_alloc;
#endif
};

struct btree {
	int order;
	int element_size;
	int options;

	int (*hook_size)(btree_t *btree, const void *element); /* return size of a single element. if NULL, size is 1 */
	void *(*hook_sub)(btree_t *btree, void *element, int index); /* return sub element. note: 0 <= index < hook_size(element) */
	int (*hook_cmp)(btree_t *btree, const void *a, const void *b, void *group); /* compare function. 'data': only used by '_group' methods which specify an additional data argument */
	int (*hook_acquire)(btree_t *btree, void *a);
	void (*hook_release)(btree_t *btree, void *a);
	void *data;
	void *group_default;

	btree_node_t *root;
	btree_node_t *overflow_node;
	void *overflow_element;
	btree_link_t overflow_link;
/*	struct { used to track a slot during adjust(). feature enabled if node != NULL
		btree_node_t *node;
		int pos;
	} track;*/
};

static btree_t *alloc_tree(
		int element_size)
{
	void *alloc;
	btree_t *tree;

	alloc = calloc(1, sizeof(btree_t) + element_size);
	if(alloc == NULL)
		return NULL;

	tree = alloc;
	tree->overflow_element = alloc + sizeof(btree_t);
	return tree;
}

static btree_node_t *alloc_node(
		btree_t *tree)
{
	void *alloc;
	btree_node_t *node;

	alloc = calloc(1, sizeof(btree_node_t) + sizeof(btree_link_t) * tree->order + tree->element_size * (tree->order - 1));
	if(alloc == NULL)
		return NULL;
	node = alloc;
	node->links = alloc + sizeof(btree_node_t);
	node->elements = alloc + sizeof(btree_node_t) + sizeof(btree_link_t) * tree->order;

#ifdef TESTING
	if(last_node_alloc != NULL)
		last_node_alloc->next_alloc = node;
	node->prev_alloc = last_node_alloc;
	last_node_alloc = node;
#endif

	return node;
}

static void free_node(
		btree_node_t *node)
{
#ifdef TESTING
	if(node->next_alloc != NULL)
		node->next_alloc->prev_alloc = node->prev_alloc;
	if(node->prev_alloc == NULL)
		last_node_alloc = node->next_alloc;
	else
		node->prev_alloc->next_alloc = node->next_alloc;
#endif
	free(node);
}

static inline btree_node_t *left_sibling(
		btree_t *tree,
		btree_node_t *node)
{
	if(node->parent == NULL) /* root node has no siblings */
		return NULL;
	else if(node->child_index == 0) /* leftmost node does not have left sibling */
		return NULL;
	else
		return node->parent->links[node->child_index - 1].child;
}

static inline btree_node_t *right_sibling(
		btree_t *tree,
		btree_node_t *node)
{
	if(node->parent == NULL) /* root node has no siblings */
		return NULL;
	else if(node->child_index == node->parent->fill) /* rightmost node does not have right sibling */
		return NULL;
	else
		return node->parent->links[node->child_index + 1].child;
}

static inline bool overflowing(
		btree_t *tree,
		btree_node_t *node)
{
	return tree->overflow_node == node;
}

static inline bool near_overflowing(
		btree_t *tree,
		btree_node_t *node)
{
	return node->fill == tree->order - 1;
}

static inline bool underflowing(
		btree_t *tree,
		btree_node_t *node)
{
	return node->fill < tree->order / 2;
}

static inline bool near_underflowing(
		btree_t *tree,
		btree_node_t *node)
{
	return node->fill == tree->order / 2;
}

static inline bool isleaf(
		btree_node_t *node)
{
	return node->links[0].child == NULL;
}

static void out_data(
		io_context_t *ctx,
		const void *si,
		size_t n)
{
	if(ctx->error == 0)
		ctx->error = ctx->f.write(si, n, ctx->user);
}

static void out_u8(
		io_context_t *ctx,
		uint8_t v)
{
	out_data(ctx, &v, 1);
}

static void out_u32(
		io_context_t *ctx,
		uint32_t v)
{
	v = htobe32(v);
	out_data(ctx, &v, 4);
}

static void out_u64(
		io_context_t *ctx,
		uint64_t v)
{
	v = htobe64(v);
	out_data(ctx, &v, 8);
}

static void out_header(
		io_context_t *ctx)
{
	assert(strlen(MAGIC) == 8);
	out_data(ctx, MAGIC, 8);
	out_u32(ctx, 1); /* version */
	out_u32(ctx, ctx->self->order);
	out_u64(ctx, ctx->self->options);
	if(ctx->get_size == NULL)
		out_u64(ctx, ctx->fixed_size);
	else
		out_u64(ctx, 0);
}

static void out_eos(
		io_context_t *ctx)
{
	out_data(ctx, NULL, 0);
}

static void walk(
		btree_t *self,
		void (*enter)(btree_node_t *node, io_context_t *ctx),
		void (*leave)(btree_node_t *node, io_context_t *ctx),
		io_context_t *ctx)
{
	btree_node_t *cur = self->root;
	bool descend = true;
	int index;

	while(cur != NULL) {
		while(descend) {
			if(enter != NULL)
				enter(cur, ctx);
			if(isleaf(cur))
				descend = false;
			else
				cur = cur->links[0].child;
		}
		while(!descend) {
			if(leave != NULL)
				leave(cur, ctx);
			index = cur->child_index + 1;
			cur = cur->parent;
			if(cur == NULL)
				descend = true;
			else if(index <= cur->fill) {
				cur = cur->links[index].child;
				descend = true;
			}
		}
	}
}

static void out_node_enter(
		btree_node_t *node,
		io_context_t *ctx)
{
	int i;
	out_u8(ctx, WALK_DESCEND);
	out_u32(ctx, node->child_index);
	out_u32(ctx, node->fill);
	for(i = 0; i < node->fill; i++) {
		if(ctx->get_size != NULL) /* the size information for each field is only present, if get_size != NULL. otherwise ctx->fixed_size is used */
			out_u64(ctx, ctx->get_size(GET_E(ctx->self, node->elements + i * ctx->self->element_size), ctx->user));
		if(ctx->error == 0)
			ctx->error = ctx->s.serialize(GET_E(ctx->self, node->elements + i * ctx->self->element_size), ctx->user);
	}
}

static void out_node_leave(
		btree_node_t *node,
		io_context_t *ctx)
{
	out_u8(ctx, WALK_ASCEND);
}

static int newroot(
		btree_t *tree)
{
	btree_node_t *root = alloc_node(tree);
	if(root == NULL)
		return -ENOMEM;
	if(tree->root != NULL) {
		tree->root->parent = root;
		tree->root->child_index = 0;
		root->links[0].child = tree->root;
		if(tree->overflow_node == tree->root)
			root->links[0].count = tree->overflow_link.offset + tree->overflow_link.count;
		else
			root->links[0].count = tree->root->links[tree->root->fill].count + tree->root->links[tree->root->fill].offset;
	}
	tree->root = root;
	return 0;
}

/* does not split root node;
 * node must be overflowing */
static int split(
		btree_t *tree,
		btree_node_t *l)
{
	btree_node_t *p;
	btree_node_t *r;
	btree_link_t *rlink;
	int sidx = tree->order / 2;
	int i;
	int n;

	assert(l == tree->overflow_node);
	assert(l != tree->root);
	assert(l->fill == tree->order - 1);

	p = l->parent;
 	r = alloc_node(tree);
	if(r == NULL)
		return -ENOMEM;

	r->parent = p;
	r->child_index = l->child_index + 1;
	r->fill = l->fill - sidx;

	/* copy overflow data to back of right node */
	memcpy(r->elements + (r->fill - 1) * tree->element_size, tree->overflow_element, tree->element_size); /* move overflow element to last position of right node */
	memcpy(r->links + r->fill, &tree->overflow_link, sizeof(btree_link_t)); /* move overflow link to last position of right node */

	/* insert new right node into parent */
	if(r->child_index == tree->order) { /* new right node will be in overflow position */
		memcpy(tree->overflow_element, l->elements + sidx * tree->element_size, tree->element_size); /* move single element from left node to overflow */
		rlink = &tree->overflow_link;
		tree->overflow_node = p;
	}
	else {
		if(p->fill == tree->order - 1) { /* parent will overflow, move last element to overflow position */
			memcpy(tree->overflow_element, p->elements + (p->fill - 1) * tree->element_size, tree->element_size);
			memcpy(&tree->overflow_link, p->links + p->fill, sizeof(btree_link_t));
			if(tree->overflow_link.child != NULL)
				tree->overflow_link.child->child_index = tree->order;
			tree->overflow_node = p;
			p->fill--;
		}
		else { /* parent will not overflow, clear overflow position */
			memset(tree->overflow_element, 0, tree->element_size);
			memset(&tree->overflow_link, 0, sizeof(btree_link_t));
			tree->overflow_node = NULL;
		}
		memmove(p->elements + (l->child_index + 1) * tree->element_size, p->elements + l->child_index * tree->element_size, (p->fill - l->child_index) * tree->element_size); /* insert new element at l->child_index */
		memmove(p->links + l->child_index + 2, p->links + l->child_index + 1, (p->fill - l->child_index) * sizeof(btree_link_t)); /* insert new link at l->child_index + 1 */
		memcpy(p->elements + l->child_index * tree->element_size, l->elements + sidx * tree->element_size, tree->element_size); /* move single element from left node to parent */
		rlink = p->links + r->child_index;

		p->fill++;
	}
	rlink->child = r;

	memcpy(r->elements, l->elements + (sidx + 1) * tree->element_size, (r->fill - 1) * tree->element_size); /* move all remaining elements except overflow element from left node to right node */
	memcpy(r->links, l->links + sidx + 1, r->fill * sizeof(btree_link_t)); /* move links except overflow link from left node to right node */
	memset(l->elements + sidx * tree->element_size, 0, r->fill * tree->element_size); /* clear moved elements in left node */
	memset(l->links + sidx + 1, 0, r->fill * sizeof(btree_link_t)); /* clear moved links in left node */
	l->fill = sidx;
	for(i = l->child_index + 1; i <= p->fill; i++) /* update child indices in parent */
		if(p->links[i].child != NULL)
			p->links[i].child->child_index = i;
	for(i = 0; i <= r->fill; i++) /* update links in right node */
		if(r->links[i].child != NULL) {
			r->links[i].child->parent = r;
			r->links[i].child->child_index = i;
		}


	n = 0;
	for(i = 0; i <= r->fill; i++) {
		r->links[i].offset = n;
		n += r->links[i].count + 1;
	}
	n--;
	p->links[l->child_index].count -= n + 1; /* n elements go to right node, one element to parent node */
	rlink->count = n;
	rlink->offset = p->links[l->child_index].offset + p->links[l->child_index].count + 1;
	return 0;
}

static void concatenate(
		btree_t *tree,
		btree_node_t *l)
{
	btree_node_t *p;
	btree_node_t *r;
	int i;
	int n;

	assert(tree->overflow_node == NULL);
	assert(l->child_index < tree->order - 1);

	p = l->parent;
	r = p->links[l->child_index + 1].child;

	assert(l->fill + 1 + r->fill <= tree->order);

	if(l->fill + 1 + r->fill == tree->order) { /* left element will overflow */
		memcpy(tree->overflow_element, r->elements + (r->fill - 1) * tree->element_size, tree->element_size); /* move last element of right node into overflow position */
		memcpy(&tree->overflow_link, r->links + r->fill, sizeof(btree_link_t)); /* move last link of right node into overflow position */
		if(tree->overflow_link.child != NULL) {
			tree->overflow_link.child->parent = l;
			tree->overflow_link.child->child_index = tree->order;
		}
		tree->overflow_node = l;
		r->fill--;
	}
	memcpy(l->elements + l->fill * tree->element_size, p->elements + l->child_index * tree->element_size, tree->element_size); /* append element from parent to left node */
	memcpy(l->elements + (l->fill + 1) * tree->element_size, r->elements, r->fill * tree->element_size); /* append elements except overflow from right node to left node */
	memcpy(l->links + l->fill + 1, r->links, (r->fill + 1) * sizeof(btree_link_t)); /* append links except overflow from right node to left node */
	l->fill += 1 + r->fill;

	p->fill--;
	memmove(p->elements + l->child_index * tree->element_size, p->elements + (l->child_index + 1) * tree->element_size, (p->fill - l->child_index) * tree->element_size); /* delete element from parent */
	memmove(p->links + l->child_index + 1, p->links + l->child_index + 2, (p->fill - l->child_index) * sizeof(btree_link_t)); /* delete link from parent */
	memset(p->elements + p->fill * tree->element_size, 0, tree->element_size); /* clear last element from parent */
	memset(p->links + p->fill + 1, 0, sizeof(btree_link_t)); /* clear last link from parent */
	for(i = l->fill - r->fill; i <= l->fill; i++)
		if(l->links[i].child != NULL) {
			l->links[i].child->parent = l;
			l->links[i].child->child_index = i;
		}
	for(i = l->child_index + 1; i <= p->fill; i++)
		if(p->links[i].child != NULL)
			p->links[i].child->child_index = i;

	free_node(r);

	n = 0;
	for(i = 0; i <= l->fill; i++) {
		l->links[i].offset = n;
		n += l->links[i].count + 1;
	}
	n--;
	if(tree->overflow_node == l) {
		n++;
		tree->overflow_link.offset = n;
		n += tree->overflow_link.count;
	}
	p->links[l->child_index].count = n;
}

/* move last element of node 'l' to parent;
 * move element at corresponding parent position to beginning of right sibling
 * note: right sibling must not be full */
static void lr_redistribute(
		btree_t *tree,
		btree_node_t *l)
{
	btree_node_t *p;
	btree_node_t *r;
	int i;
	int n;

	assert(l == tree->overflow_node || tree->overflow_node == NULL);
	assert(l != tree->root);
	assert(l->child_index < tree->order - 1);

	p = l->parent;
	r = p->links[l->child_index + 1].child;

	assert(r->fill < tree->order - 1);

	memmove(r->elements + tree->element_size, r->elements, r->fill * tree->element_size); /* insert new first element at right node */
	memmove(r->links + 1, r->links, (r->fill + 1) * sizeof(btree_link_t)); /* insert new link at right node */
	memcpy(r->elements, p->elements + l->child_index * tree->element_size, tree->element_size); /* move element from parent to first position at right node */
	if(l == tree->overflow_node) {
		memcpy(p->elements + l->child_index * tree->element_size, tree->overflow_element, tree->element_size); /* move overflow element from left node to parent */
		memcpy(r->links, &tree->overflow_link, sizeof(btree_link_t)); /* move overflow link from left node to first link of right node */
		memset(tree->overflow_element, 0, tree->element_size); /* clear overflow element */
		memset(&tree->overflow_link, 0, sizeof(btree_link_t)); /* clear overflow link */
		tree->overflow_node = NULL;
	}
	else {
		memcpy(p->elements + l->child_index * tree->element_size, l->elements + (l->fill - 1) * tree->element_size, tree->element_size); /* move last element from left node to parent */
		memcpy(r->links, l->links + l->fill, sizeof(btree_link_t)); /* move last link from left node to first link of right node */
		memset(l->elements + (l->fill - 1) * tree->element_size, 0, tree->element_size); /* clear last element from left node */
		memset(l->links + l->fill, 0, sizeof(btree_link_t)); /* clear last link of left node */
		l->fill--;
	}
	r->fill++;

	if(r->links[0].child != NULL)
		r->links[0].child->parent = r;
	for(i = 0; i <= r->fill; i++)
		if(r->links[i].child != NULL)
			r->links[i].child->child_index = i;

	n = r->links[0].count + 1; /* number of elements moved from left to right... */
	p->links[l->child_index].count -= n; /* ...which are missing on left node now */
	p->links[r->child_index].count += n; /* ...are present on right node now */
	p->links[r->child_index].offset -= n; /* ...which shift the offset of the right node */
	r->links[0].offset = 0;
	for(i = 1; i <= r->fill; i++)
		r->links[i].offset += n; /* ...and the offset of all consecutive links on right node */
}

/* move first element of node 'r' to parent;
 * move element at corresponding parent position to first free of left sibling
 * note: left sibling must not be full */
static void rl_redistribute(
		btree_t *tree,
		btree_node_t *r)
{
	btree_node_t *p;
	btree_node_t *l;
	int i;
	int n;

	assert(r == tree->overflow_node || tree->overflow_node == NULL);
	assert(r != tree->root);
	assert(r->child_index > 0);

	p = r->parent;
	l = p->links[r->child_index - 1].child;

	assert(l->fill < tree->order - 1);

	memcpy(l->elements + l->fill * tree->element_size, p->elements + l->child_index * tree->element_size, tree->element_size); /* move element from parent to last position at left node */
	memcpy(p->elements + l->child_index * tree->element_size, r->elements, tree->element_size); /* move first element from right node to parent */
	memcpy(l->links + l->fill + 1, r->links, sizeof(btree_link_t)); /* move first link from right node to left node */
	memmove(r->elements, r->elements + tree->element_size, (r->fill - 1) * tree->element_size); /* delete first element at right node */
	memmove(r->links, r->links + 1, r->fill * sizeof(btree_link_t)); /* delete first link at right node */
	l->fill++;
	if(tree->overflow_node == r) {
		memmove(r->elements + (r->fill - 1) * tree->element_size, tree->overflow_element, tree->element_size);
		memmove(r->links + r->fill, &tree->overflow_link, sizeof(btree_link_t));
		memset(tree->overflow_element, 0, tree->element_size); /* clear overflow element */
		memset(&tree->overflow_link, 0, sizeof(btree_link_t)); /* clear overflow link */
		tree->overflow_node = NULL;
	}
	else {
		memset(r->elements + (r->fill - 1) * tree->element_size, 0, tree->element_size); /* clear last element from right node */
		memset(r->links + r->fill, 0, sizeof(btree_link_t)); /* clear last link from right node */
		r->fill--;
	}

	for(i = 0; i <= r->fill; i++)
		if(r->links[i].child != NULL)
			r->links[i].child->child_index = i;

	n = l->links[l->fill].count + 1;
	p->links[l->child_index].count += n;
	p->links[r->child_index].count -= n;
	p->links[r->child_index].offset += n;
	if(l->fill == 0)
		l->links[0].offset = 0;
	else
		l->links[l->fill].offset = l->links[l->fill - 1].offset + l->links[l->fill - 1].count + 1;
	if(l->links[l->fill].child != NULL) {
		l->links[l->fill].child->parent = l;
		l->links[l->fill].child->child_index = l->fill;
	}
	for(i = 0; i <= r->fill; i++)
		r->links[i].offset -= n;
}

static int adjust(
		btree_t *tree,
		btree_node_t *node)
{
	int ret = 0;
	btree_node_t *left;
	btree_node_t *right;
	if(overflowing(tree, node)) {
		left = left_sibling(tree, node);
		right = right_sibling(tree, node);
		if(right != NULL && !near_overflowing(tree, right)) /* test: overflow_1 */
			lr_redistribute(tree, node);
		else if(left != NULL && !near_overflowing(tree, left)) /* test: overflow_2 */
			rl_redistribute(tree, node);
		else if(node->parent == NULL) { /* test: overflow_3 */
			ret = newroot(tree);
			if(ret == 0)
				ret = split(tree, node);
		}
		else { /* test: overflow_4 */
			ret = split(tree, node);
			if(ret == 0)
				ret = adjust(tree, node->parent);
		}
	}
	else if(underflowing(tree, node)) {
		left = left_sibling(tree, node);
		right = right_sibling(tree, node);
		if(left != NULL && !near_underflowing(tree, left)) /* test: underflow_1 */
			lr_redistribute(tree, left);
		else if(right != NULL && !near_underflowing(tree, right)) /* test: underflow_2 */
			rl_redistribute(tree, right);
		else if(node->parent == NULL) {
			if(node->fill == 0) { /* test: underflow_3 */
				tree->root = node->links[0].child;
				tree->root->parent = NULL;
				free_node(node);
			}
		}
		else if(right != NULL) { /* test: underflow_4 */
			concatenate(tree, node);
			adjust(tree, node->parent);
		}
		else { /* 'left' must exist */
			concatenate(tree, left);
			adjust(tree, left->parent);
		}
	}
	return ret;
}

static void update_count(
		btree_node_t *node,
		int amount)
{
	int i;
	int ci;
	while(node->parent != NULL) {
		ci = node->child_index;
		node = node->parent;
		node->links[ci].count += amount;
		for(i = ci + 1; i <= node->fill; i++)
			node->links[i].offset += amount;
	}
}

static int node_insert(
		btree_t *tree,
		btree_node_t *node,
		int pos,
		void *element)
{
	int ret;

	if(tree->root == NULL) {
		ret = newroot(tree);
		if(ret != 0)
			return ret;
	}
	if(node == NULL) {
		node = tree->root;
		pos = 0;
	}
	if(pos == tree->order - 1) { /* put new element into overflow position */
		SET_EP(tree, tree->overflow_element, element);
		tree->overflow_node = node;
	}
	else {
		if(node->fill == tree->order - 1) { /* node will overflow, move last element to overflow position */
			memcpy(tree->overflow_element, node->elements + (node->fill - 1) * tree->element_size, tree->element_size);
			tree->overflow_node = node;
			node->fill--;
		}
		memmove(node->elements + (pos + 1) * tree->element_size, node->elements + pos * tree->element_size, (node->fill - pos) * tree->element_size);
		SET_EP(tree, node->elements + pos * tree->element_size, element);
		node->fill++;
	}
	if(tree->overflow_node == node)
		tree->overflow_link.offset = node->fill + 1;
	else
		node->links[node->fill].offset = node->fill;

	update_count(node, 1);
	ret = adjust(tree, node);
	if(ret != 0)
		return ret;
	if(tree->hook_acquire != NULL)
		tree->hook_acquire(tree, element);
	return 0;
}

static int node_replace(
		btree_t *tree,
		btree_node_t *node,
		int pos,
		void *element)
{
	if(tree->hook_release != NULL)
		tree->hook_release(tree, GET_E(tree, node->elements + pos * tree->element_size));
	SET_EP(tree, node->elements + pos * tree->element_size, element);
	if(tree->hook_acquire != NULL)
		tree->hook_acquire(tree, element);
	return 0;
}

/* TODO casual segfault when removing items from an even-order btree */
static int node_remove(
		btree_t *tree,
		btree_node_t *node,
		int pos)
{
	btree_node_t *cur;

	if(tree->hook_release != NULL)
		tree->hook_release(tree, GET_E(tree, node->elements + pos * tree->element_size));
	if(isleaf(node)) { /* node where the element is contained within is a leaf, simply remove it */
		node->fill--;
		memmove(node->elements + pos * tree->element_size, node->elements + (pos + 1) * tree->element_size, (node->fill - pos) * tree->element_size);
		if(node == tree->root && node->fill == 0) {
			free_node(node);
			tree->root = NULL;
			return 0;
		}
	}
	else { /* node where the element is contained within is not a leaf, search cur leaf of right subtree */
		cur = node->links[pos + 1].child;
		while(!isleaf(cur))
			cur = cur->links[0].child;
		memcpy(node->elements + pos * tree->element_size, cur->elements, tree->element_size); /* move first element to position of deleted element */
		cur->fill--;
		memmove(cur->elements, cur->elements + tree->element_size, cur->fill * tree->element_size); /* delete moved element */
		node = cur;
	}
	update_count(node, -1);
	return adjust(tree, node);
}

/* converts a node and a position to a position on a leaf, where
 * a new element can be inserted, so that the resulting element
 * will be located immediately before the given element */
static void to_insert_before(
		btree_t *tree,
		btree_node_t **node,
		int *pos)
{
	if(*node == NULL) { /* rightmost position in tree; find_lower returns NULL is all elements are less, therefore insertion would take place at rightmost slot */
		if(tree->root == NULL) /* no root node yet, *node remains NULL */
			return;
		*node = tree->root;
		*pos = tree->root->fill;
	}
	while(!isleaf(*node)) {
		*node = (*node)->links[*pos].child;
		*pos = (*node)->fill;
	}
}

/* converts a node and a position to a position on a leaf, where
 * a new element can be inserted, so that the resulting element
 * will be located immediately after the given element */
/*static void to_insert_after(
		btree_t *tree,
		btree_node_t **node,
		int *pos)
{
	if(*node == NULL) {
		if(tree->root == NULL)
			return;
		*node = tree->root;
		*pos = -1;
	}
	while(!isleaf(*node)) {
		*node = (*node)->links[*pos + 1].child;
		*pos = -1;
	}
	(*pos)++;
}*/

static bool find_lower(
		btree_t *tree,
		const void *key,
		btree_node_t **node,
		int *pos,
		void *group)
{
	int u;
	int l;
	int m;
	int cmp;
	btree_node_t *node_candidate = NULL;
	int pos_candidate = 0;
	bool found = false;
	btree_node_t *cur = tree->root;
	btree_node_t *prev = NULL;

	while(cur != NULL) {
		u = cur->fill - 1;
		l = 0;
		prev = cur;
		while(l <= u) {
			m = l + (u - l) / 2;
			cmp = tree->hook_cmp(tree, GET_E(tree, cur->elements + m * tree->element_size), key, group);
			if(cmp >= 0) {
				node_candidate = cur;
				pos_candidate = m;
				u = m - 1;
				if(cmp == 0)
					found = true;
			}
			else
				l = m + 1;
		}
		cur = cur->links[l].child;
	}

	if(node_candidate == NULL && prev != NULL) { /* all element keys less than requested key, select imaginary element after end (rightmost leaf node) */
		node_candidate = prev;
		pos_candidate = prev->fill;
	}
	if(node != NULL)
		*node = node_candidate;
	if(pos != NULL)
		*pos = pos_candidate;
	return found;
}

static bool find_upper(
		btree_t *tree,
		const void *key,
		btree_node_t **node,
		int *pos,
		void *group)
{
	int u;
	int l;
	int m;
	int cmp;
	btree_node_t *node_candidate = NULL;
	int pos_candidate = 0;
	bool found = false;
	btree_node_t *cur = tree->root;
	btree_node_t *prev = NULL;

	while(cur != NULL) {
		u = cur->fill - 1;
		l = 0;
		prev = cur;
		while(l <= u) {
			m = l + (u - l) / 2;
			cmp = tree->hook_cmp(tree, GET_E(tree, cur->elements + m * tree->element_size), key, group);
			if(cmp > 0) {
				node_candidate = cur;
				pos_candidate = m;
				u = m - 1;
			}
			else {
				if(cmp == 0)
					found = true;
				l = m + 1;
			}
		}
		cur = cur->links[l].child;
	}

	if(node_candidate == NULL && prev != NULL) { /* all element keys less than requested key, select imaginary element after end (rightmost leaf node) */
		node_candidate = prev;
		pos_candidate = prev->fill;
	}
	if(node != NULL)
		*node = node_candidate;
	if(pos != NULL)
		*pos = pos_candidate;
	return found;
}

/* returns whether the given index has been found. if false:
 *   - 'node' == NULL: given index greater than size
 *   - 'node' != NULL: given index == size, can append at node->elements[pos] (note: 'pos' may be the overflow position) */
static bool find_index(
		btree_t *tree,
		int index,
		btree_node_t **node,
		int *pos)
{
	int u;
	int l;
	int m;
	btree_node_t *cur = tree->root;
	int offset = 0;
	int o;
	int c;

	while(cur != NULL) {
		u = cur->fill;
		l = 0;
		while(l <= u) {
			m = l + (u - l) / 2;
			c = cur->links[m].count;
			o = offset + cur->links[m].offset;
			if(o + c == index) {
				if(m == cur->fill && !isleaf(cur)) {
					cur = cur->links[m].child;
					offset = o;
					break;
				}
				else {
					if(node != NULL)
						*node = cur;
					if(pos != NULL)
						*pos = m;
					return m < cur->fill;
				}
			}
			else if(o > index)
				u = m - 1;
			else if(o + c < index)
				l = m + 1;
			else {
				cur = cur->links[m].child;
				offset = o;
				break;
			}
		}
		if(l > u)
			break;
	}
	if(node != NULL)
		*node = NULL;
	if(pos != NULL)
		*pos = 0;
	return false;
}

static int to_index(
		btree_node_t *node,
		int pos)
{
	int index;

	if(node == NULL)
		return 0;
	index = node->links[pos].count;
	while(node != NULL) {
		index += node->links[pos].offset;
		pos = node->child_index;
		node = node->parent;
	}
	return index;
}

static bool to_next(
		btree_node_t **node_,
		int *pos_)
{
	btree_node_t *node = *node_;
	int pos = *pos_;

	if(pos == node->fill)
		return false;
	pos++;
	/* descend */
	while(node->links[pos].child != NULL) {
		node = node->links[pos].child;
		pos = 0;
	}
	/* ascend */
	while(pos == node->fill) {
		pos = node->child_index;
		node = node->parent;
		if(node == NULL) { /* end reached */
			(*pos_)++; /* if we are here, *node_ is rightmost leaf. increase pos_ so that it points to imaginary element after end. */
			return true;
		}
	}
	*node_ = node;
	*pos_ = pos;
	return true;
}

static bool to_prev(
		btree_node_t **node_,
		int *pos_)
{
	btree_node_t *node = *node_;
	int pos = *pos_;

	/* descend tree */
	while(node->links[pos].child != NULL) {
		node = node->links[pos].child;
		pos = node->fill;
	}
	/* ascend tree */
	while(pos == 0) {
		pos = node->child_index;
		node = node->parent;
		if(node == NULL)
			return false;
	}
	pos--;
	*node_ = node;
	*pos_ = pos;
	return true;
}

/* check, whether the specified element can be inserted/set at given index */
static bool validate_at(
		btree_t *tree,
		const void *element,
		btree_node_t *node,
		int pos,
		bool replace)
{
	btree_node_t *other_node;
	int other_pos;
	bool found;

	/* TODO debug. if !node, a segfault happens (i.e. when validating against an empty tree). */
	return true;


	other_node = node;
	other_pos = pos;
	found = to_prev(&other_node, &other_pos);
	if(found && tree->hook_cmp(tree, GET_E(tree, other_node->elements + other_pos * tree->element_size), element, tree->group_default) > 0) /* element before must be <= element to insert */
		return false;
	if(replace) {
		other_node = node;
		other_pos = pos;
		found = to_next(&other_node, &other_pos);
	}
	else {
		other_node = node;
		other_pos = pos;
		found = pos < node->fill;
	}
	if(found && tree->hook_cmp(tree, GET_E(tree, other_node->elements + other_pos * tree->element_size), element, tree->group_default) < 0) /* element after must be >= element to insert */
		return false;
	return true;
}

btree_t *btree_new(
		int order,
		int element_size,
		int (*cmp)(btree_t *btree, const void *a, const void *b, void *group),
		int options)
{
	btree_t *self;

	if(order < 3) {
		errno = EINVAL;
		return NULL;
	}
	else if((options & BTREE_OPT_RESERVED) != 0) {
		errno = EINVAL;
		return NULL;
	}

	if(element_size < 0)
		self = alloc_tree(sizeof(void*));
	else
		self = alloc_tree(element_size);
	if(self == NULL) {
		errno = ENOMEM;
		return NULL;
	}

	self->options = options;
	self->order = order;
	self->hook_cmp = cmp;
	if(element_size < 0) {
		self->element_size = sizeof(void*);
		self->options |= OPT_USE_POINTERS;
	}
	else
		self->element_size = element_size;
	if(cmp == NULL)
		self->options |= OPT_NOCMP;

	return self;
}

int btree_write(
		btree_t *self,
		size_t (*size)(const void *element, void *user),
		int (*serialize)(const void *element, void *user),
		int (*write)(const void *si, size_t size, void *user),
		void *user)
{
	io_context_t ctx;

	if((self->options & OPT_USE_POINTERS) != 0)
		return -EINVAL;

	memset(&ctx, 0, sizeof(ctx));
	ctx.self = self;
	ctx.get_size = size;
	ctx.user = user;
	ctx.f.write = write;
	ctx.s.serialize = serialize;

	out_header(&ctx);
	walk(self, out_node_enter, out_node_leave, &ctx);
	out_u8(&ctx, WALK_END);

	out_eos(&ctx);
	return ctx.error;
}

int btree_write_fixed(
		btree_t *self,
		size_t element_size,
		int (*serialize)(const void *element, void *user),
		int (*write)(const void *si, size_t size, void *user), /* returns: 0 on success, custom error otherwise; when finished, called with si = NULL */
		void *user)
{
	io_context_t ctx;

	if((self->options & OPT_USE_POINTERS) != 0)
		return -EINVAL;
	else if(element_size == 0)
		return -EINVAL;

	memset(&ctx, 0, sizeof(ctx));
	ctx.self = self;
	ctx.fixed_size = element_size;
	ctx.user = user;
	ctx.f.write = write;
	ctx.s.serialize = serialize;

	out_header(&ctx);
	walk(self, out_node_enter, out_node_leave, &ctx);
	out_u8(&ctx, WALK_END);

	out_eos(&ctx);
	return ctx.error;
}

btree_t *btree_read(
		int (*read)(void *di, size_t size, void *user),
		void *user)
{
	return NULL;
}

uint64_t btree_memory_total(
		btree_t *self)
{
	uint64_t bytes = sizeof(btree_t) + self->element_size; /* see alloc_tree() */
	btree_node_t *cur;
	int n_nodes = 0;
	int pow = 0;
	for(cur = self->root; cur != NULL; cur = cur->links[0].child) {
		if(pow == 0)
			pow = 1;
		else
			pow *= self->order;
		n_nodes += pow;
	}
	bytes += n_nodes * (sizeof(btree_node_t) + sizeof(btree_link_t) * self->order + self->element_size * (self->order - 1)); /* see alloc_node() */
	return bytes;
}

uint64_t btree_memory_payload(
		btree_t *self)
{
	if(self->root == NULL)
		return 0;
	return self->element_size * (self->root->links[self->root->fill].offset + self->root->links[self->root->fill].count);
}

void btree_set_data(
		btree_t *self,
		void *data)
{
	self->data = data;
}

void *btree_data(
		btree_t *self)
{
	return self->data;
}

void btree_sethook_subelement(
		btree_t *self,
		int (*size)(btree_t *btree, const void *element),
		void *(*sub)(btree_t *btree, void *element, int index))
{
	self->hook_size = size;
	self->hook_sub = sub;
}

void btree_sethook_refcount(
		btree_t *self,
		int (*acquire)(btree_t *btree, void *element),
		void (*release)(btree_t *btree, void *element))
{
	self->hook_release = release;
	self->hook_acquire = acquire;
}

void btree_set_group_default(
		btree_t *self,
		void *group)
{
	self->group_default = group;
}

int btree_clear(
		btree_t *self)
{
	btree_node_t *prev;
	btree_node_t *cur = self->root;
	int child_index = 0;
	int i;

	if((self->options & OPT_FINALIZED) != 0)
		return -EINVAL;

	while(cur != NULL) {
		while(child_index <= cur->fill && cur->links[child_index].child != NULL) {
			cur = cur->links[child_index].child;
			child_index = 0;
		}

		if(self->hook_release != NULL)
			for(i = 0; i < cur->fill; i++)
				self->hook_release(self, GET_E(self, cur->elements + i * self->element_size));
		prev = cur;
		child_index = cur->child_index;
		cur = cur->parent;
		if(cur != NULL)
			cur->links[child_index].child = NULL;
		child_index++;
		free_node(prev);
	}
	self->root = NULL;
	return 0;
}

void btree_finalize(
		btree_t *self)
{
	self->options |= OPT_FINALIZED;
}

int btree_is_finalized(
		btree_t *self)
{
	return (self->options & OPT_FINALIZED) != 0;
}

void btree_destroy(
		btree_t *self)
{
	btree_clear(self);
	free(self);
}

int btree_size(
		btree_t *self)
{
/*	int n = 0;
	int i;*/
	if(self->root == NULL)
		return 0;
	return self->root->links[self->root->fill].offset + self->root->links[self->root->fill].count;
/*	for(i = 0; i <= self->root->fill; i++)
		n += self->root->links[i].count;
	n += self->root->fill;
	return n;*/
}

int btree_swap(
		btree_t *self,
		int index_a,
		int index_b)
{
	btree_node_t *node_a;
	btree_node_t *node_b;
	int pos_a;
	int pos_b;

	if((self->options & OPT_FINALIZED) != 0)
		return -EINVAL;
	else if((self->options & OPT_NOCMP) == 0 && (self->options & BTREE_OPT_ALLOW_INDEX) == 0) /* insert by index only if cmp is not used */
		return -EINVAL;
	else if(index_a < 0 || index_a >= btree_size(self))
		return -EOVERFLOW;
	else if(index_b < 0 || index_b >= btree_size(self))
		return -EOVERFLOW;
	else if(index_a == index_b)
		return 0;

	find_index(self, index_a, &node_a, &pos_a);
	find_index(self, index_b, &node_b, &pos_b);
	if((self->options & OPT_NOCMP) == 0 && self->hook_cmp(self, GET_E(self, node_a->elements + pos_a * self->element_size), GET_E(self, node_b->elements + pos_b * self->element_size), self->group_default) != 0)
		return -EINVAL;
	memcpy(self->overflow_element, node_a->elements + pos_a * self->element_size, self->element_size);
	memcpy(node_a->elements + pos_a * self->element_size, node_b->elements + pos_b * self->element_size, self->element_size);
	memcpy(node_b->elements + pos_b * self->element_size, self->overflow_element, self->element_size);
	memset(self->overflow_element, 0, self->element_size);
	return 0;
}

int btree_insert(
		btree_t *self,
		void *element)
{
	int pos;
	btree_node_t *cur;
	bool found;

	assert(self->overflow_node == NULL);

	if((self->options & OPT_FINALIZED) != 0)
		return -EINVAL;
	else if((self->options & OPT_NOCMP) != 0) /* insert by key only if cmp is present */
		return -EINVAL;

	if((self->options & BTREE_OPT_INSERT_LOWER) != 0)
		found = find_lower(self, element, &cur, &pos, self->group_default);
	else
		found = find_upper(self, element, &cur, &pos, self->group_default);
	to_insert_before(self, &cur, &pos);
	if((self->options & BTREE_OPT_MULTI_KEY) != 0 || !found)
		return node_insert(self, cur, pos, element);
	else
		return -EALREADY;
}

int btree_insert_at(
		btree_t *self,
		int index,
		void *element)
{
	int pos;
	btree_node_t *cur;

	assert(self->overflow_node == NULL);

	if((self->options & OPT_FINALIZED) != 0)
		return -EINVAL;
	else if((self->options & OPT_NOCMP) == 0 && (self->options & BTREE_OPT_ALLOW_INDEX) == 0) /* insert by index only if cmp is not used */
		return -EINVAL;
	else if(index < 0 || index > btree_size(self))
		return -EOVERFLOW;

	find_index(self, index, &cur, &pos);
	if((self->options & OPT_NOCMP) == 0 && !validate_at(self, element, cur, pos, false))
		return -EINVAL;
	to_insert_before(self, &cur, &pos);
	return node_insert(self, cur, pos, element);
}

int btree_put(
		btree_t *self,
		void *element)
{
	int pos;
	btree_node_t *cur;
	bool found;

	assert(self->overflow_node == NULL);

	if((self->options & OPT_FINALIZED) != 0)
		return -EINVAL;
	else if((self->options & OPT_NOCMP) != 0) /* insert by key only if cmp is used */
		return -EINVAL;

	found = find_lower(self, element, &cur, &pos, self->group_default);
	if(found)
		return node_replace(self, cur, pos, element);
	else {
		to_insert_before(self, &cur, &pos);
		return node_insert(self, cur, pos, element);
	}
}

int btree_put_at(
		btree_t *self,
		int index,
		void *element)
{
	int pos;
	btree_node_t *cur;
	bool found;

	assert(self->overflow_node == NULL);

	if((self->options & OPT_FINALIZED) != 0)
		return -EINVAL;
	else if((self->options & OPT_NOCMP) == 0 && (self->options & BTREE_OPT_ALLOW_INDEX) == 0) /* put by index only if cmp is not used */
		return -EINVAL;
	else if(index < 0 || index > btree_size(self))
		return -EOVERFLOW;

	found = find_index(self, index, &cur, &pos);
	if(found) {
		if((self->options & OPT_NOCMP) == 0 && !validate_at(self, element, cur, pos, true))
			return -EINVAL;
		return node_replace(self, cur, pos, element);
	}
	else {
		if((self->options & OPT_NOCMP) == 0 && !validate_at(self, element, cur, pos, false))
			return -EINVAL;
		to_insert_before(self, &cur, &pos);
		return node_insert(self, cur, pos, element);
	}
}

/*void *btree_insert_key(
		btree_t *self,
		const void *key)
{
	int pos;
	btree_node_t *cur;

	assert(self->overflow_node == NULL);

	if(self->cmp == NULL)
		return -EINVAL;

	if(find_key(self, key, &cur, &pos))
		return -EALREADY;
	self->track.node = cur;
	self->track.pos = pos;
	node_insert(self, cur, pos, NULL);

	memset(&self->track, 0, sizeof(self->track));
}*/

bool btree_contains(
		btree_t *self,
		const void *key)
{
	return find_lower(self, key, NULL, NULL, self->group_default);
}

void *btree_get(
		btree_t *self,
		const void *key)
{
	int pos;
	btree_node_t *node;
	if(find_lower(self, key, &node, &pos, self->group_default))
		return GET_E(self, node->elements + pos * self->element_size);
	else
		return NULL;
}

void *btree_get_at(
		btree_t *self,
		int index)
{
	btree_node_t *node;
	int pos;

	if(index < 0) {
		errno = -EOVERFLOW;
		return NULL;
	}
	if(find_index(self, index, &node, &pos))
		return GET_E(self, node->elements + pos * self->element_size);
	else {
		errno = -EOVERFLOW;
		return NULL;
	}
}

int btree_remove(
		btree_t *self,
		void *element)
{
	int pos;
	btree_node_t *cur;

	assert(self->overflow_node == NULL);

	if((self->options & OPT_FINALIZED) != 0)
		return -EINVAL;
	else if(self->options & OPT_NOCMP) /* remove by key only if cmp is present */
		return -EINVAL;

	if(!find_lower(self, element, &cur, &pos, self->group_default))
		return -ENOENT;
	else
		return node_remove(self, cur, pos);
}

int btree_remove_at(
		btree_t *self,
		int index)
{
	btree_node_t *node;
	int pos;

	assert(self->overflow_node == NULL);

	if((self->options & OPT_FINALIZED) != 0)
		return -EINVAL;
	else if(!find_index(self, index, &node, &pos))
		return -ENOENT;
	else
		return node_remove(self, node, pos);
}

int btree_find_at(
		btree_t *self,
		int index,
		btree_it_t *it)
{
	btree_node_t *node;
	int pos;

	if(find_index(self, index, &node, &pos)) {
		if(it != NULL) {
			memset(it, 0, sizeof(*it));
			it->element = GET_E(self, node->elements + pos * self->element_size);
			it->index = index;
			it->tree = self;
			it->node = node;
			it->pos = pos;
			it->found = true;
		}
		return index;
	}
	else {
		if(it != NULL)
			it->found = false;
		return -ENOENT;
	}
}

int btree_find_begin(
		btree_t *self,
		btree_it_t *it)
{
	btree_node_t *child = self->root;
	btree_node_t *node = child;

	if(it != NULL) {
		while(child != NULL) {
			node = child;
			child = node->links[0].child;
		}
		memset(it, 0, sizeof(*it));
		if(node == NULL)
			it->element = NULL;
		else
			it->element = GET_E(self, node->elements);
		it->index = 0;
		it->tree = self;
		it->node = node;
		it->pos = 0;
		it->found = node != NULL;
	}
	return 0;
}

int btree_find_end(
		btree_t *self,
		btree_it_t *it)
{
	btree_node_t *node = self->root;
	int index = 0;
	int i;

	if(it != NULL) {
		memset(it, 0, sizeof(*it));
		it->element = NULL;
		it->index = 0;
		it->tree = self;
		it->node = NULL;
		it->pos = 0;
	}
	if(node == NULL)
		return 0;
	
	/* number of elements (i.e. resulting index + 1) can be retrieved
	 * from root node alone */
	index += node->fill;
	for(i = 0; i <= node->fill; i++)
		index += node->links[i].count;
	
	if(it != NULL) {
		while(node->links[node->fill].child != NULL)
			node = node->links[node->fill].child;
		it->index = index;
		it->node = node;
		it->pos = node->fill;
	}
	return index;
}

/*int btree_find_first(
		btree_t *self,
		const void *key,
		btree_it_t *it)
{
	btree_node_t *node;
	int pos;
	int index;

	if(self->options & OPT_NOCMP)
		return -EINVAL;

	if(!find_lower(self, key, &node, &pos))
		return -ENOENT;
	index = to_index(node, pos);
	if(it != NULL) {
		it->tree = self;
		it->index = index;
		it->pos = pos;
		it->node = node;
		it->element = GET_E(self, node->elements + pos * self->element_size);
	}
	return index;
}

int btree_find_last(
		btree_t *self,
		const void *key,
		btree_it_t *it)
{
	if(self->options & OPT_NOCMP)
		return -EINVAL;
	btree_node_t *node;
	int pos;
	int index;

	if(self->options & OPT_NOCMP)
		return -EINVAL;

	if(!find_upper(self, key, &node, &pos))
		return -ENOENT;
	to_prev(&node, &pos);
	index = to_index(node, pos);
	if(it != NULL) {
		it->tree = self;
		it->index = index;
		it->pos = pos;
		it->node = node;
		it->element = GET_E(self, node->elements + pos * self->element_size);
	}
	return index;
}*/

int btree_find_lower(
		btree_t *self,
		const void *key,
		btree_it_t *it)
{
	btree_node_t *node;
	int pos;
	int index;
	bool found;

	if(self->options & OPT_NOCMP)
		return -EINVAL;

	found = find_lower(self, key, &node, &pos, self->group_default);
	index = to_index(node, pos);
	if(it != NULL) {
		memset(it, 0, sizeof(*it));
		it->tree = self;
		it->pos = pos;
		it->node = node;
		if(node == NULL || pos == node->fill)
			it->element = NULL;
		else
			it->element = GET_E(self, node->elements + pos * self->element_size);
		it->index = index;
		it->found = found;
	}
	return index;
}

int btree_find_upper(
		btree_t *self,
		const void *key,
		btree_it_t *it)
{
	btree_node_t *node;
	int pos;
	int index;
	bool found;

	if(self->options & OPT_NOCMP)
		return -EINVAL;

	found = find_upper(self, key, &node, &pos, self->group_default);
	index = to_index(node, pos);
	if(it != NULL) {
		memset(it, 0, sizeof(*it));
		it->tree = self;
		it->pos = pos;
		it->node = node;
		if(node == NULL || pos == node->fill)
			it->element = NULL;
		else {
			assert(index < btree_size(self));
			it->element = GET_E(self, node->elements + pos * self->element_size);
		}
		it->index = index;
		it->found = found;
	}
	return index;
}

int btree_find_lower_group(
		btree_t *self,
		const void *key,
		void *group,
		btree_it_t *it)
{
	btree_node_t *node;
	int pos;
	int index;
	bool found;

	if(self->options & OPT_NOCMP)
		return -EINVAL;

	found = find_lower(self, key, &node, &pos, group);
	index = to_index(node, pos);
	if(it != NULL) {
		memset(it, 0, sizeof(*it));
		it->tree = self;
		it->pos = pos;
		it->node = node;
		if(node == NULL)
			it->element = NULL;
		else
			it->element = GET_E(self, node->elements + pos * self->element_size);
		it->index = index;
		it->found = found;
	}
	return index;
}

int btree_find_upper_group(
		btree_t *self,
		const void *key,
		void *group,
		btree_it_t *it)
{
	btree_node_t *node;
	int pos;
	int index;
	bool found;

	if(self->options & OPT_NOCMP)
		return -EINVAL;

	found = find_upper(self, key, &node, &pos, group);
	index = to_index(node, pos);
	if(it != NULL) {
		memset(it, 0, sizeof(*it));
		it->tree = self;
		it->pos = pos;
		it->node = node;
		if(node == NULL || pos == node->fill)
			it->element = NULL;
		else {
			assert(index < btree_size(self));
			it->element = GET_E(self, node->elements + pos * self->element_size);
		}
		it->index = index;
		it->found = found;
	}
	return index;
}

int btree_validate_modified(
		btree_it_t *it)
{
	return validate_at(it->tree, it->element, it->node, it->pos, true) ? 0 : -EINVAL;
}

int btree_iterate_next(
		btree_it_t *it)
{
	int pos = it->pos;
	btree_node_t *node = it->node;

	if(!to_next(&node, &pos))
		return -ENOENT;

	it->index++;
	if(pos == node->fill)
		it->element = NULL;
	else
		it->element = GET_E(it->tree, node->elements + pos * it->tree->element_size);
	it->pos = pos;
	it->node = node;
	it->found = it->element != NULL;

	return it->index;
}

int btree_iterate_prev(
		btree_it_t *it)
{
	int pos = it->pos;
	btree_node_t *node = it->node;

	if(!to_prev(&node, &pos))
		return -ENOENT;

	it->element = GET_E(it->tree, node->elements + pos * it->tree->element_size);
	it->index--;
	it->pos = pos;
	it->node = node;
	return it->index;
}

void btree_dump(
		btree_t *self,
		void (*print)(const void *element))
{
	dump_tree(self, print);
}

static void dump_node(
		int indent,
		btree_t *tree,
		btree_node_t *node,
		void (*print)(const void *element))
{
	int i;
	int k;
	for(i = 0; i < node->fill;/*MIN(node->fill, tree->order - 1);*/ i++) {
		printf("| ");
		print(GET_E(tree, node->elements + i * tree->element_size));
		printf(" ");
	}
	for(i = node->fill; i < tree->order - 1; i++)
		printf("| --- ");
	if(tree->overflow_node == node) {
		printf("# ");
		print(GET_E(tree, tree->overflow_element));
		printf(" |\n");
	}
	else
		printf("|\n");
	for(i = 0; i <= node->fill; i++) {
		if(node->links[i].child != NULL && node->links[i].child->fill > 0) {
			for(k = 0; k <= indent; k++)
				printf("  ");
			printf("[%3d %3d] ", node->links[i].offset, node->links[i].count);
			for(k = 0; k < i; k++)
				printf("-");
			printf("+");
			for(k = i + 1; k < tree->order; k++)
				printf("-");
			if(node->links[i].child->parent != node)
				printf(" parent=INVALID");
			if(node->links[i].child->child_index != i)
				printf(" child_index=INVALID");
			printf("  ");
			dump_node(indent + 1, tree, node->links[i].child, print);
		}
		else {
			for(k = 0; k <= indent; k++)
				printf("  ");
			printf("[%3d %3d] ", node->links[i].offset, node->links[i].count);
			for(k = 0; k < i; k++)
				printf("-");
			printf("+");
			for(k = i + 1; k < tree->order; k++)
				printf("-");
			printf("\n");
		}
	}
	if(tree->overflow_node == node) {
		if(tree->overflow_link.child != NULL && tree->overflow_link.child->fill > 0) {
			for(k = 0; k <= indent; k++)
				printf("  ");
			printf("[%3d %3d] ", tree->overflow_link.offset, tree->overflow_link.count);
			for(k = 0; k < tree->order - 1; k++)
				printf(" ");
			printf("#");
			if(tree->overflow_link.child->parent != node)
				printf(" parent=INVALID");
			if(tree->overflow_link.child->child_index != tree->order)
				printf(" child_index=INVALID");
			printf("  ");
			dump_node(indent + 1, tree, tree->overflow_link.child, print);
		}
	}
}

static void dump_tree(
		btree_t *tree,
		void (*print)(const void *element))
{
	if(tree->root == NULL)
		return;
	dump_node(0, tree, tree->root, print);
}

#ifdef TESTING
#include "../test/btree.h"
#include "btree_gla.gen.h"
#endif

