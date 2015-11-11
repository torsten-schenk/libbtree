#include <stdbool.h>
#include <CUnit/Basic.h>

#include <btree/memory.h>

int btreetest_btree();
int btreetest_gla_btree();

int main(
		int argn,
		const char *const *argv)
{
	int ret;

	ret = CU_initialize_registry();
	if(ret != CUE_SUCCESS) {
		printf("Error initializing CUnit.\n");
		CU_cleanup_registry();
		return ret;
	}

	ret = btreetest_btree();
	if(ret != 0) {
		CU_cleanup_registry();
		return ret;
	}
	ret = btreetest_gla_btree();
	if(ret != 0) {
		CU_cleanup_registry();
		return ret;
	}

	CU_basic_set_mode(CU_BRM_VERBOSE);
	CU_basic_run_tests();
	ret = CU_get_error();
	CU_cleanup_registry();
	return ret;
}

