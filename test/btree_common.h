#pragma once

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(X) (sizeof(X) / sizeof(*(X)))
#endif

static bool test_node_equal(
		btree_t *ta,
		btree_t *tb,
		btree_node_t *a,
		btree_node_t *b)
{
	int i;
	int va;
	int vb;
	int order = ta->order;
	btree_node_t *ca;
	btree_node_t *cb;

	if(ta->order != tb->order) {
		printf("  - trees not equal: different order\n");
		return false;
	}
	else if(a->fill != b->fill) {
		printf("  - trees not equal: fill size of both nodes differ (%d != %d)\n", a->fill, b->fill);
		return false;
	}
	else if((ta->overflow_node == a && tb->overflow_node != b) || (ta->overflow_node != a && tb->overflow_node == b)) {
		printf("  - trees not equal: different node overflow state\n");
		return false;
	}
	for(i = 0; i < a->fill; i++) {
		va = *((int*)a->elements + i);
		vb = *((int*)b->elements + i);
		if(va != vb) {
			printf("  - trees not equal: element at %d, %d != %d\n", i, va, vb);
			return false;
		}
	}
	for(i = 0; i < order; i++) {
		ca = a->links[i].child;
		cb = b->links[i].child;
		if(ca == NULL && cb == NULL)
			continue;
		else if(ca == NULL || cb == NULL) {
			printf("  - trees not equal: one of both has a child at %d\n", i);
			return false;
		}
		else if(!test_node_equal(ta, tb, ca, cb))
			return false;
	}
	if(ta->overflow_node == a) {
		ca = ta->overflow_link.child;
		cb = tb->overflow_link.child;
		va = *(int*)ta->overflow_element;
		vb = *(int*)tb->overflow_element;
		if(va != vb) {
			printf("  - trees not equal: different overflow element, %d != %d\n", va, vb);
			return false;
		}
		if((ca == NULL || cb == NULL) && ca != cb) {
			printf("  - trees not equal: one of both has a child at overflow position\n");
			return false;
		}
		else if(!test_node_equal(ta, tb, ca, cb))
			return false;
	}
	return true;
}

static bool test_tree_equal(
		btree_t *a,
		btree_t *b)
{
	if(a->order != b->order) {
		printf("  - trees not equal: different order\n");
		return false;
	}
	else if(a->root == NULL && b->root == NULL)
		return true;
	else if(a->root == NULL || b->root == NULL) {
		if(a->root != NULL && a->root->fill == 0)
			return true;
		else if(b->root != NULL && b->root->fill == 0)
			return true;
		else {
			printf("  - trees not equal: one of both trees is empty\n");
			return false;
		}
	}
	else
		return test_node_equal(a, b, a->root, b->root);
}

static bool test_node_consistent(
		btree_t *tree,
		btree_node_t *node,
		int *count)
{
	int i;
	btree_node_t *child;
	int chcount;
	bool is_leaf = true;
	int child_gap = -1;

	if(tree->overflow_node == node && tree->overflow_link.child != NULL)
		is_leaf = false;
	for(i = node->fill + 1; i < tree->order; i++) {
		child = node->links[i].child;
		if(child != NULL) {
//			printf("%d    %d %d %d\n", node->fill, *((int*)node->elements + 0), *((int*)node->elements + 1), *((int*)node->elements + 2));
			printf("  - inconsistency: no child expected at %d, %d expected\n", i, node->fill + 1);
			return false;
		}
	}
	for(i = 0; i <= node->fill; i++) {
		child = node->links[i].child;
		if(child == NULL) {
			if(child_gap == -1)
				child_gap = i;
		}
		else
			is_leaf = false;
	}
	if(!is_leaf) {
		if(child_gap != -1) {
//			printf("%d    %d %d\n", node->fill, *((int*)node->elements + 0), *((int*)node->elements + 1));
			printf("  - inconsistency: missing child at %d, %d expected%s\n", child_gap, node->fill + 1, tree->overflow_node == node ? " (plus additional overflow link)" : "");
			return false;
		}
		*count = 0;
		for(i = 0; i <= node->fill; i++) {
			child = node->links[i].child;
			if(child->parent != node) {
				printf("  - inconsistency: invalid parent\n");
				return false;
			}
			else if(child->child_index != i) {
				printf("  - inconsistency: invalid child index (found: %d, expected: %d)\n", child->child_index, i);
				return false;
			}
			else if(!test_node_consistent(tree, child, &chcount))
				return false;
			else if(node->links[i].count != chcount) {
//				printf("%d    %d %d\n", node->fill, *((int*)node->elements + 0), *((int*)node->elements + 1));
				printf("  - inconsistency: invalid link count, found=%d, expected=%d\n", node->links[i].count, chcount);
				return false;
			}
			else if(node->links[i].offset != *count) {
				printf("  - inconsistency: invalid link offset, found=%d, expected=%d\n", node->links[i].offset, *count);
				return false;
			}
			*count += chcount;
			if(i < node->fill)
				(*count)++;
		}
		if(tree->overflow_node == node) {
			(*count)++;
			child = tree->overflow_link.child;
			if(node->fill != tree->order - 1) {
				printf("  - inconsistency: node not full but overflow present\n");
				return false;
			}
			if(child != NULL)
				is_leaf = false;
			else {
				printf("  - inconsistency: missing overflow child\n");
				return false;
			}
			if(child->parent != node) {
				printf("  - inconsistency: invalid parent for overflow child\n");
				return false;
			}
			else if(child->child_index != tree->order) {
				printf("  - inconsistency: invalid child index for overflow child, %d expected, %d found\n", tree->order, child->child_index);
				return false;
			}
			else if(!test_node_consistent(tree, child, &chcount))
				return false;
			else if(tree->overflow_link.count != chcount) {
				printf("  - inconsistency: invalid child count on overflow link, found=%d, expected=%d\n", tree->overflow_link.count, chcount);
				return false;
			}
			else if(tree->overflow_link.offset != *count) {
				printf("  - inconsistency: invalid link offset, found=%d, expected=%d\n", tree->overflow_link.offset, *count);
				return false;
			}
			*count += chcount;
		}
	}
	else {
		*count = node->fill;
		if(tree->overflow_node == node)
			(*count)++;
		for(i = 0; i <= node->fill; i++)
			if(node->links[i].offset != i) {
				printf("  - inconsistency: invalid link offset in leaf, found=%d, expected=%d\n", node->links[i].offset, i);
				return false;
			}
			else if(node->links[i].count != 0) {
				printf("  - inconsistency: invalid link count on leaf, found=%d, expected=0\n", node->links[i].count);
				return false;
			}
		if(tree->overflow_node == node) {
			if(tree->overflow_link.offset != node->fill + 1) {
				printf("  - inconsistency: invalid overflow link offset in leaf, found=%d, expected=%d\n", tree->overflow_link.offset, node->fill + 1);
				return false;
			}
			else if(tree->overflow_link.count != 0) {
				printf("  - inconsistency: invalid overflow link count on leaf, found=%d, expected=0\n", node->links[i].count);
				return false;
			}
		}
	}
	return true;
}

static bool test_tree_consistent(
		btree_t *tree)
{
	int count;
	if(tree->root == NULL)
		return true;
	else if(tree->root->parent != NULL)
		return false;
	else
		return test_node_consistent(tree, tree->root, &count);
}

static int test_cmp_div10(
		const void *a_,
		const void *b_)
{
	int a = *(const int*)a_ / 10;
	int b = *(const int*)b_ / 10;
	if(a < b)
		return -1;
	else if(a > b)
		return 1;
	else
		return 0;
}

static int test_cmp_int(
		const void *a_,
		const void *b_)
{
	int a = *(const int*)a_;
	int b = *(const int*)b_;
	if(a < b)
		return -1;
	else if(a > b)
		return 1;
	else
		return 0;
}

static void test_print_int(
		const void *a_)
{
	const int *a = a_;
	printf("%d", *a);
}

static void test_nodes_destroy()
{
	while(last_node_alloc != NULL)
		free_node(last_node_alloc);
}

