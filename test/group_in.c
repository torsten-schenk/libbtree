#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>
#include <btree/memory.h>

#define ARRAY_SIZE(A) (sizeof(A) / sizeof(*(A)))

typedef struct {
	int a;
	int b;
	int c;
} entry_t;

static const entry_t entries[] = {
	{ 1, 1, 1 },
	{ 1, 1, 2 },
	{ 1, 1, 3 },
	{ 1, 2, 1 },
	{ 1, 2, 2 },
	{ 1, 2, 3 },
	{ 1, 3, 1 },
	{ 1, 3, 2 },
	{ 1, 3, 3 },
	{ 1, 4, 1 },
	{ 2, 1, 1 },
	{ 2, 1, 2 },
	{ 2, 1, 3 },
	{ 2, 2, 1 },
	{ 2, 2, 2 },
	{ 2, 2, 3 },
	{ 2, 3, 1 },
	{ 2, 3, 2 },
	{ 2, 3, 3 },
	{ 3, 1, 1 },
	{ 3, 1, 2 },
	{ 3, 1, 3 },
	{ 3, 2, 1 },
	{ 3, 2, 2 },
	{ 3, 2, 3 },
	{ 3, 3, 1 },
	{ 3, 3, 2 },
	{ 3, 3, 3 }
};

typedef struct {
	int left;
	int right;
} group_t;

#define GROUP_A ((void*)1)
#define GROUP_B ((void*)2)

static int cmp_entry(
		btree_t *btree,
		const entry_t *a,
		const entry_t *b,
		group_t *group)
{
	if(group->left < 1) {
		if(a->a < b->a)
			return -1;
		else if(a->a > b->a)
			return 1;
	}
	if(group->right < 2)
		return 0;
	if(group->left < 2) {
		if(a->b < b->b)
			return -1;
		else if(a->b > b->b)
			return 1;
	}
	if(group->right < 3)
		return 0;

	if(a->c < b->c)
		return -1;
	else if(a->c > b->c)
		return 1;
	else
		return 0;
}

enum {
	A_MAX = 50, B_MAX = 100, C_MAX = 400
};

enum {
	N_TEST = 10000
};

static int test()
{
	entry_t entry;
	entry_t *result;
	btree_it_t it;
	btree_t *tree = btree_new(3, sizeof(entry_t), (btree_cmp_t)cmp_entry, 0);
	int i;
	int l;
	int u;
	group_t group_default;
	group_t group_test;
	group_t group;
	int size = 0;

	int a;
	int b;
	int c;
	int an;
	int bn;
	int cn;
	int sample;

	group_default.left = 0;
	group_default.right = 3;
	btree_set_group_default(tree, &group_default);

	an = rand() % A_MAX;
	for(a = 0; a < an; a++) {
		entry.a = a;
		bn = rand() % B_MAX;
		for(b = 0; b < bn; b++) {
			entry.b = b;
			cn = rand() % C_MAX;
			for(c = 0; c < cn; c++) {
				size++;
				entry.c = c;
				if(btree_insert(tree, &entry) < 0) {
					printf("ERROR INSERTING\n");
					return 1;
				}
			}
		}
	}

	assert(btree_size(tree) == size);
	if(btree_size(tree) == 0) {
		printf("empty tree\n");
		btree_destroy(tree);
		return 0;
	}
	sample = rand() % btree_size(tree);
	result = btree_get_at(tree, sample);

	a = result->a;
	b = result->b;
	c = result->c;

	group.left = 0;
	group.right = 1;
	entry.a = a;
	btree_find_lower_group(tree, &entry, &group, &it);
	l = it.index;
	btree_find_upper_group(tree, &entry, &group, &it);
	u = it.index;
	printf("SIZE: %d  RANGE: %d %d\n", btree_size(tree), l, u);

	group_test.left = 0;
	group_test.right = 2;
	group.left = 1;
	group.right = 2;
	entry.b = b;

	btree_find_lower_group_in(tree, l, u, &entry, &group, &it);
	printf("LOWER: %d\n", it.index);
	assert(it.index == btree_find_lower_group(tree, &entry, &group_test, NULL));
	assert(it.found);

	btree_find_upper_group_in(tree, l, u, &entry, &group, &it);
	printf("UPPER: %d\n", it.index);
	assert(it.index == btree_find_upper_group(tree, &entry, &group_test, NULL));

	entry.b = B_MAX;
	btree_find_lower_group_in(tree, l, u, &entry, &group, &it);
	printf("LOWER 2: %d\n", it.index);
	assert(it.index == u);
	assert(!it.found);

	btree_find_upper_group_in(tree, l, u, &entry, &group, &it);
	printf("UPPER 2: %d\n", it.index);
	assert(it.index == u);

	entry.b = -1;
	btree_find_lower_group_in(tree, l, u, &entry, &group, &it);
	printf("LOWER 3: %d\n", it.index);
	assert(it.index == l);
	assert(!it.found);

	btree_find_upper_group_in(tree, l, u, &entry, &group, &it);
	printf("UPPER 3: %d\n", it.index);
	assert(it.index == l);

	btree_destroy(tree);
	return 0;
}

int main()
{
	int i;
	for(i = 0; i < N_TEST; i++)
		test();
/*	result = it.element;
	assert(result->a == a);
	assert(result->b == b);
	if(it.index > l) {
		btree_iterate_prev(&it);
		result = it.element;
		assert(result->b < b);
	}*/
	return 0;
}

