#include <string.h>

#include "common.h"
#include "btree_common.h"

/******************************** TEST 1 **********************************/

static int test1_init()
{
	return 0;
}

static int test1_cleanup()
{
	return 0;
}

/******************************** TEST 2 **********************************/

static btree_t *test2_btree0;
static btree_t *test2_btree3;
static btree_t *test2_btree11;

static int test2_init()
{
	int value;
	int ret;

	test2_btree0 = btree_new(5, sizeof(int), test_cmp_int, NULL, NULL, BTREE_OPT_DEFAULT);
	if(test2_btree0 == NULL)
		return -ENOMEM;
	test2_btree3 = btree_new(5, sizeof(int), test_cmp_int, NULL, NULL, BTREE_OPT_DEFAULT);
	if(test2_btree3 == NULL)
		return -ENOMEM;
	test2_btree11 = btree_new(5, sizeof(int), test_cmp_int, NULL, NULL, BTREE_OPT_DEFAULT);
	if(test2_btree11 == NULL)
		return -ENOMEM;

	for(value = 10; value <= 12; value++) {
		ret = btree_insert(test2_btree3, &value);
		if(ret < 0)
			return ret;
	}

	for(value = 20; value <= 30; value++) {
		ret = btree_insert(test2_btree11, &value);
		if(ret < 0)
			return ret;
	}
	/* btree_dump(test2_btree3, test_print_int); */
	/* btree_dump(test2_btree11, test_print_int); */
	return 0;
}

static int test2_cleanup()
{
	btree_destroy(test2_btree0);
	btree_destroy(test2_btree3);
	btree_destroy(test2_btree11);
	return 0;
}

static void test2_find_begin_end0()
{
	int ret;
	btree_it_t it;

	ret = btree_find_begin(test2_btree0, NULL);
	CU_ASSERT_EQUAL(ret, 0);
	ret = btree_find_end(test2_btree0, NULL);
	CU_ASSERT_EQUAL(ret, 0);

	ret = btree_find_begin(test2_btree0, &it);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(it.index, 0);
	CU_ASSERT_EQUAL(it.tree, test2_btree0);
	CU_ASSERT_EQUAL(it.node, NULL);
	CU_ASSERT_EQUAL(it.pos, 0);

	ret = btree_find_end(test2_btree0, &it);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(it.index, 0);
	CU_ASSERT_EQUAL(it.tree, test2_btree0);
	CU_ASSERT_EQUAL(it.node, NULL);
	CU_ASSERT_EQUAL(it.pos, 0);
}

static void test2_find_begin_end3()
{
	int ret;
	btree_it_t it;

	ret = btree_find_begin(test2_btree3, NULL);
	CU_ASSERT_EQUAL(ret, 0);
	ret = btree_find_end(test2_btree3, NULL);
	CU_ASSERT_EQUAL(ret, 3);

	ret = btree_find_begin(test2_btree3, &it);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(it.index, 0);
	CU_ASSERT_EQUAL(it.tree, test2_btree3);
	CU_ASSERT_EQUAL(it.node, test2_btree3->root);
	CU_ASSERT_EQUAL(it.pos, 0);

	ret = btree_find_end(test2_btree3, &it);
	CU_ASSERT_EQUAL(ret, 3);
	CU_ASSERT_EQUAL(it.index, 3);
	CU_ASSERT_EQUAL(it.tree, test2_btree3);
	CU_ASSERT_EQUAL(it.node, test2_btree3->root);
	CU_ASSERT_EQUAL(it.pos, 3);
}

static void test2_find_begin_end11()
{
	int ret;
	btree_it_t it;

	ret = btree_find_begin(test2_btree11, NULL);
	CU_ASSERT_EQUAL(ret, 0);
	ret = btree_find_end(test2_btree11, NULL);
	CU_ASSERT_EQUAL(ret, 11);

	ret = btree_find_begin(test2_btree11, &it);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(it.index, 0);
	CU_ASSERT_EQUAL(it.tree, test2_btree11);
	CU_ASSERT_EQUAL(it.node, test2_btree11->root->links[0].child);
	CU_ASSERT_EQUAL(it.pos, 0);

	ret = btree_find_end(test2_btree11, &it);
	CU_ASSERT_EQUAL(ret, 11);
	CU_ASSERT_EQUAL(it.index, 11);
	CU_ASSERT_EQUAL(it.tree, test2_btree11);
	CU_ASSERT_EQUAL(it.node, test2_btree11->root->links[2].child);
	CU_ASSERT_EQUAL(it.pos, 3);
}

static void test2_find_lower_upper0()
{
	int ret;
	btree_it_t it;
	int v = 10;

	ret = btree_find_lower(test2_btree0, &v, NULL);
	CU_ASSERT_EQUAL(ret, 0);
	ret = btree_find_upper(test2_btree0, &v, NULL);
	CU_ASSERT_EQUAL(ret, 0);

	ret = btree_find_lower(test2_btree0, &v, &it);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(it.index, 0);
	CU_ASSERT_EQUAL(it.tree, test2_btree0);
	CU_ASSERT_EQUAL(it.node, NULL);
	CU_ASSERT_EQUAL(it.pos, 0);

	ret = btree_find_upper(test2_btree0, &v, &it);
	CU_ASSERT_EQUAL(ret, 0);
	CU_ASSERT_EQUAL(it.index, 0);
	CU_ASSERT_EQUAL(it.tree, test2_btree0);
	CU_ASSERT_EQUAL(it.node, NULL);
	CU_ASSERT_EQUAL(it.pos, 0);
}

/******************************** TEST 3 **********************************/

#define TEST3_ITERATE_N 1000

static void test3_iterate()
{
	btree_t *tree;
	int i;
	int k;
	int ret;
	int tmp;
	int turn;
	btree_it_t it;
	btree_it_t begin;
	btree_it_t end;
	btree_it_t tmpit;
	int order[TEST3_ITERATE_N];

	for(i = 0; i < TEST3_ITERATE_N; i++)
		order[i] = i;
	for(turn = 0; turn < 100; turn++) {
		int failed_prepend = 0;
		int failed_element_value = 0;
		int failed_element_pointer = 0;
		int failed_element_value_reverse = 0;
		int failed_element_pointer_reverse = 0;
		int failed_find_at = 0;

		for(i = 0; i < TEST3_ITERATE_N; i++) {
			k = rand() % TEST3_ITERATE_N;
			tmp = order[i];
			order[i] = order[k];
			order[k] = tmp;
		}

		tree = btree_new(3, sizeof(int), test_cmp_int, NULL, NULL, BTREE_OPT_DEFAULT);
		CU_ASSERT_PTR_NOT_NULL_FATAL(tree);
		for(i = 0; i < TEST3_ITERATE_N; i++) {
			ret = btree_insert(tree, order + i);
			if(ret != 0)
				failed_prepend++;
		}
		CU_ASSERT_EQUAL(failed_prepend, 0);
		CU_ASSERT_TRUE(test_tree_consistent(tree));

		CU_ASSERT_EQUAL(btree_size(tree), TEST3_ITERATE_N);

		ret = btree_find_end(tree, &end);
		CU_ASSERT_EQUAL_FATAL(ret, TEST3_ITERATE_N);
		ret = btree_find_begin(tree, &begin);
		CU_ASSERT_EQUAL_FATAL(ret, 0);

		it = begin;
		for(it = begin; it.index < end.index; btree_iterate_next(&it)) {
			if(*(int*)it.element != it.index)
				failed_element_value++;
			if(it.element != btree_get_at(tree, it.index))
				failed_element_pointer++;
			btree_find_at(tree, it.index, &tmpit);
			if(memcmp(&tmpit, &it, sizeof(btree_it_t)) != 0)
				failed_find_at++;
		}
		CU_ASSERT_EQUAL(memcmp(&end, &it, sizeof(btree_it_t)), 0);
		CU_ASSERT_EQUAL(failed_find_at, 0);

		it = end;
		while(btree_iterate_prev(&it) != -ENOENT) {
			btree_iterate_prev(&it);
			if(*(int*)it.element != it.index)
				failed_element_value_reverse++;
			if(it.element != btree_get_at(tree, it.index))
				failed_element_pointer_reverse++;
		}
		CU_ASSERT_EQUAL(memcmp(&begin, &it, sizeof(btree_it_t)), 0);

		CU_ASSERT_EQUAL(failed_element_value, 0);
		CU_ASSERT_EQUAL(failed_element_pointer, 0);
		CU_ASSERT_EQUAL(failed_element_value_reverse, 0);
		CU_ASSERT_EQUAL(failed_element_pointer_reverse, 0);

		btree_destroy(tree);
	}
}

#define TEST3_MAX_PER_TIER 100
#define TEST3_N_RANDOM_INSERT 300

static void test3_random_insert()
{
	btree_t *tree;
	int i;
	int k;
	int n[10];
	int order[10][TEST3_MAX_PER_TIER];
	int v;
	int turn;
	int ret;
	int sum;
	btree_it_t begin;
	btree_it_t end;
	btree_it_t it;
	btree_it_t tmpit;
	int prev_tier;
	int tier;

	for(turn = 0; turn < 100; turn++) {
		int failed_prepend = 0;
		int failed_tier_order = 0;
		int failed_element_order = 0;
		int failed_tier_index = 0;
		int failed_tier_element_order = 0;
		int failed_lower_value = 0;
		int failed_lower_index = 0;
		int failed_upper_value = 0;
		int failed_upper_index = 0;
		int failed_find_at = 0;

		tree = btree_new(3, sizeof(int), test_cmp_div10, NULL, NULL, BTREE_OPT_MULTI_KEY);
		CU_ASSERT_PTR_NOT_NULL_FATAL(tree);
		for(i = 0; i < 10; i++)
			n[i] = 0;
		for(i = 0; i < TEST3_N_RANDOM_INSERT; i++) {
			do {
				v = rand() % 100;
				tier = v / 10;
			} while(n[tier] == TEST3_MAX_PER_TIER);
			order[tier][n[tier]++] = v;
			ret = btree_insert(tree, &v);
			if(ret != 0)
				failed_prepend++;
		}
		CU_ASSERT_EQUAL_FATAL(failed_prepend, 0);
		CU_ASSERT_TRUE(test_tree_consistent(tree));

		ret = btree_find_begin(tree, &begin);
		CU_ASSERT_EQUAL(ret, 0);
		ret = btree_find_end(tree, &end);
		CU_ASSERT_EQUAL(ret, TEST3_N_RANDOM_INSERT);

		prev_tier = -1;
		for(it = begin; it.index < end.index; btree_iterate_next(&it)) {
			v = *(int*)it.element;
			tier = v / 10;
			if(prev_tier != tier) {
				if(tier <= prev_tier)
					failed_tier_order++;
				if(prev_tier >= 0)
					if(i != n[prev_tier])
						failed_element_order++;
				i = 0;
				prev_tier = tier;
			}
			btree_find_at(tree, it.index, &tmpit);
			if(memcmp(&tmpit, &it, sizeof(btree_it_t)) != 0)
				failed_find_at++;
			if(i >= n[tier])
				failed_tier_index++;
			if(v != order[tier][i])
				failed_tier_element_order++;
			i++;
		}

		for(i = 0; i < 10; i++)
			if(n[i] > 0) {
				sum = 0;
				for(k = 0; k < i; k++)
					sum += n[k];
				v = i * 10;
				ret = btree_find_lower(tree, &v, &it);
				if(it.index != sum)
					failed_lower_index++;
				if(*(int*)it.element != order[i][0])
					failed_lower_value++;
				if(it.index > 0) {
					btree_iterate_prev(&it);
					if(*(int*)it.element >= v)
						failed_lower_value++;
				}

				btree_find_upper(tree, &v, &it);
				if(it.index != sum + n[i])
					failed_upper_index++;
				btree_iterate_prev(&it);
				if(*(int*)it.element != order[i][n[i] - 1])
					failed_upper_value++;
/*				ret = btree_find_first(tree, &v, &it);
				CU_ASSERT_TRUE(ret >= 0);
				CU_ASSERT_EQUAL(*(int*)it.element, order[i][0]);

				ret = btree_find_last(tree, &v, &it);
				CU_ASSERT_TRUE(ret >= 0);
				CU_ASSERT_EQUAL(*(int*)it.element, order[i][n[i] - 1]);*/
			}
		CU_ASSERT_EQUAL(failed_tier_order, 0);
		CU_ASSERT_EQUAL(failed_element_order, 0);
		CU_ASSERT_EQUAL(failed_tier_index, 0);
		CU_ASSERT_EQUAL(failed_tier_element_order, 0);
		CU_ASSERT_EQUAL(failed_lower_value, 0);
		CU_ASSERT_EQUAL(failed_lower_index, 0);
		CU_ASSERT_EQUAL(failed_upper_value, 0);
		CU_ASSERT_EQUAL(failed_upper_index, 0);
		CU_ASSERT_EQUAL(failed_find_at, 0);

		btree_destroy(tree);
	}
}

static void test3_random_remove()
{
	btree_t *tree;
	int i;
	int k;
	int tmp;
	int turn;
	int order[TEST3_ITERATE_N];

	for(i = 0; i < TEST3_ITERATE_N; i++)
		order[i] = i;
	for(turn = 0; turn < 100; turn++) {
		int failed_consistency = 0;

		for(i = 0; i < TEST3_ITERATE_N; i++) {
			k = rand() % TEST3_ITERATE_N;
			tmp = order[i];
			order[i] = order[k];
			order[k] = tmp;
		}

		tree = btree_new(3, sizeof(int), test_cmp_int, NULL, NULL, BTREE_OPT_DEFAULT);
		CU_ASSERT_PTR_NOT_NULL_FATAL(tree);
		for(i = 0; i < TEST3_ITERATE_N; i++)
			btree_insert(tree, order + i);

		while(btree_size(tree) > 0) {
			k = rand() % btree_size(tree);
			btree_remove_at(tree, k);
			if(!test_tree_consistent(tree))
				failed_consistency++;
		}
		CU_ASSERT_EQUAL(failed_consistency, 0);

		btree_destroy(tree);
	}
}

/******************************** INITIALIZE **********************************/

int btreetest_btree()
{
	CU_pSuite suite;
	CU_pTest test;

	BEGIN_SUITE("BTree Structure", test1_init, test1_cleanup);
	END_SUITE;
	BEGIN_SUITE("BTree Find Functions", test2_init, test2_cleanup);
		ADD_TEST("begin/end in empty tree", test2_find_begin_end0);
		ADD_TEST("begin/end in single-node tree", test2_find_begin_end3);
		ADD_TEST("begin/end in multi-node tree", test2_find_begin_end11);
		ADD_TEST("lower/upper in empty tree", test2_find_lower_upper0);
	END_SUITE;
	BEGIN_SUITE("BTree Random Data", NULL, NULL);
		ADD_TEST("iteration", test3_iterate);
		ADD_TEST("random insertion", test3_random_insert);
		ADD_TEST("random removal", test3_random_remove);
	END_SUITE;

	return 0;
}

