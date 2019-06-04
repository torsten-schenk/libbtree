#include <string.h>
#include <assert.h>
#include <limits>
#include <sys/time.h>
#include <btree/memory.h>
#ifndef NO_QMAP
#include <QMap>
#endif
#include <map>

#include "helper.h"

#define BTREE_ORDER 15
#define RUNS 5
#define PARTIAL_AVG (RUNS / 5)
#define ELEMS 1000000

static_assert(RUNS % PARTIAL_AVG == 0);

#define AVLTREE_PREFIX avl
#define AVLTREE_FN static inline
#define AVLTREE_DATA avlentry_t
#define AVLTREE_FIELD node
#define AVLTREE_USE_PARENT 1
#define AVLTREE_USE_AVL 1

typedef struct avlentry avlentry_t;

struct avlentry {
	#define BINTREE_CONFIG AVLTREE
	#include <bintree-hdr.h>
	int key;
	int value;
};

#define BINTREE_CONFIG AVLTREE
#include <bintree-impl.h>

typedef struct {
	int key;
	int value;
} entry_t;

static uint64_t msec_map[RUNS];
static uint64_t msec_btree[RUNS];
static uint64_t msec_avltree[RUNS];
#ifndef NO_QMAP
static uint64_t msec_qmap[RUNS];
static QMap<int, int> qmap;
#endif
static std::map<int, int> map;
static btree_t *btree;
static avlentry_t *avltree;
static avlentry_t avldata[ELEMS];
static int values[ELEMS];

static inline void avl_clear()
{
	memset(avldata, 0, sizeof(avldata));
	avltree = NULL;
}

static int cmp_entry(
		btree_t *tree,
		const entry_t *a,
		const entry_t *b,
		void *group)
{
	if(a->key < b->key)
		return -1;
	else if(a->key > b->key)
		return 1;
	else
		return 0;
}

static int cmp_int(const void *a_, const void *b_)
{
	const uint64_t *a = (const uint64_t*)a_;
	const uint64_t *b = (const uint64_t*)b_;
	if(*a < *b)
		return -1;
	else if(*a > *b)
		return 1;
	else
		return 0;
}

static int qcmp_avlkey(const avlentry_t *a, const void *b_)
{
	int b = *(const int*)b_;
	if(a->key < b)
		return -1;
	else if(a->key > b)
		return 1;
	else
		return 0;
}

static void calc_stats(uint64_t *samples, uint64_t &min, uint64_t &max, uint64_t &avg, uint64_t &median)
{
	uint64_t partavg = 0;
	min = std::numeric_limits<uint64_t>::max();
	max = std::numeric_limits<uint64_t>::min();
	avg = 0;

	qsort(samples, RUNS, sizeof(uint64_t), cmp_int);

	for(size_t i = 0; i < RUNS; i++) {
		if(samples[i] < min)
			min = samples[i];
		if(samples[i] > max)
			max = samples[i];
		partavg += samples[i];
		if(i % PARTIAL_AVG == PARTIAL_AVG - 1) {
			avg += partavg / PARTIAL_AVG;
			partavg = 0;
		}
	}
	avg /= RUNS / PARTIAL_AVG;
	median = samples[RUNS / 2];
}

static void print_stats(const char *bench)
{
	uint64_t min;
	uint64_t max;
	uint64_t avg;
	uint64_t median;
	printf("%s:\n", bench);
#ifndef NO_QMAP
	calc_stats(msec_qmap, min, max, avg, median);
	printf("  qmap : min=%4llu     max=%4llu     avg=%4llu     median=%4llu\n", min, max, avg, median);
#endif
	calc_stats(msec_map, min, max, avg, median);
	printf("  std::map : min=%4llu     max=%4llu     avg=%4llu     median=%4llu\n", min, max, avg, median);
	calc_stats(msec_btree, min, max, avg, median);
	printf("  btree: min=%4llu     max=%4llu     avg=%4llu     median=%4llu\n", min, max, avg, median);
	calc_stats(msec_avltree, min, max, avg, median);
	printf("  avltree: min=%4llu     max=%4llu     avg=%4llu     median=%4llu\n", min, max, avg, median);
	printf("\n");
}

static void print_stats_md(const char *bench)
{
	uint64_t min;
	uint64_t max;
	uint64_t avg;
	uint64_t median;
	printf("### %s\n", bench);
	printf("| item | min | max | avg | median |\n");
	printf("|----------|----:|----:|----:|----:|\n");
#ifndef NO_QMAP
	calc_stats(msec_qmap, min, max, avg, median);
	printf("| **qmap 4.8** | %llu | %llu | %llu | %llu |\n", min, max, avg, median);
#endif
	calc_stats(msec_map, min, max, avg, median);
	printf("| **std::map** | %llu | %llu | %llu | %llu |\n", min, max, avg, median);
	calc_stats(msec_btree, min, max, avg, median);
	printf("| **btree** | %llu | %llu | %llu | %llu |\n", min, max, avg, median);
	calc_stats(msec_avltree, min, max, avg, median);
	printf("| **avltree** | %llu | %llu | %llu | %llu |\n", min, max, avg, median);
	printf("\n");
}

static void bench_read()
{
	struct timeval start;
	struct timeval end;
	size_t i;
	size_t k;
	avlentry_t *avlentry;
	entry_t entry;
	entry_t *pentry;

	map.clear();
#ifndef NO_QMAP
	qmap.clear();
#endif
	btree_clear(btree);
	avl_clear();
	for(k = 0; k < ELEMS; k++) { // prepare both for access benchmarks
		entry.key = k;
		entry.value = k;
		btree_insert(btree, &entry);
		avlentry = avldata + k;
		avlentry->key = k;
		avlentry->value = k;
		avl_insert(&avltree, NULL, avlentry);
#ifndef NO_QMAP
		qmap[k] = k;
#endif
		map[k] = k;
	}

	for(i = 0; i < RUNS; i++) {
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			entry.key = sequence_rnd[k];
			pentry = (entry_t*)btree_get(btree, &entry);
			values[k] = pentry->value;
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			avl_query(avltree, sequence_rnd + k, &avlentry, NULL, qcmp_avlkey);
			values[k] = avlentry->value;
		}
		gettimeofday(&end, NULL);
		msec_avltree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			values[k] = map[sequence_rnd[k]];
		}
		gettimeofday(&end, NULL);
		msec_map[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

#ifndef NO_QMAP
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			values[k] = qmap[sequence_rnd[k]];
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);
#endif
	}
	print_stats_md("random access");

	for(i = 0; i < RUNS; i++) {
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			entry.key = sequence_first[k];
			pentry = (entry_t*)btree_get(btree, &entry);
			values[k] = pentry->value;
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			avl_query(avltree, sequence_first + k, &avlentry, NULL, qcmp_avlkey);
			values[k] = avlentry->value;
		}
		gettimeofday(&end, NULL);
		msec_avltree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			values[k] = map[sequence_first[k]];
		}
		gettimeofday(&end, NULL);
		msec_map[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

#ifndef NO_QMAP
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			values[k] = qmap[sequence_first[k]];
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);
#endif
	}
	print_stats_md("first access");

	for(i = 0; i < RUNS; i++) {
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			entry.key = sequence_last[k];
			pentry = (entry_t*)btree_get(btree, &entry);
			values[k] = pentry->value;
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			avl_query(avltree, sequence_last + k, &avlentry, NULL, qcmp_avlkey);
			values[k] = avlentry->value;
		}
		gettimeofday(&end, NULL);
		msec_avltree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			values[k] = map[sequence_last[k]];
		}
		gettimeofday(&end, NULL);
		msec_map[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

#ifndef NO_QMAP
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			values[k] = qmap[sequence_last[k]];
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);
#endif
	}
	print_stats_md("last access");

	for(i = 0; i < RUNS; i++) {
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			entry.key = sequence_middle[k];
			pentry = (entry_t*)btree_get(btree, &entry);
			values[k] = pentry->value;
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			avl_query(avltree, sequence_middle + k, &avlentry, NULL, qcmp_avlkey);
			values[k] = avlentry->value;
		}
		gettimeofday(&end, NULL);
		msec_avltree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			values[k] = map[sequence_middle[k]];
		}
		gettimeofday(&end, NULL);
		msec_map[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

#ifndef NO_QMAP
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			values[k] = qmap[sequence_middle[k]];
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);
#endif
	}
	print_stats_md("middle access");
}

static void bench_insert()
{
	struct timeval start;
	struct timeval end;
	size_t i;
	size_t k;
	entry_t entry;
	entry_t *pentry;
	avlentry_t *avlentry;
	avlentry_t *avlpivot;

	map.clear();
#ifndef NO_QMAP
	qmap.clear();
#endif
	btree_clear(btree);
	avl_clear();
	for(i = 0; i < RUNS; i++) {
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			entry.key = sequence_fwd[k];
			entry.value = sequence_fwd[k];
			btree_insert(btree, &entry);
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		btree_clear(btree);

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			avlentry = avldata + k;
			avlentry->key = sequence_fwd[k];
			avlentry->value = sequence_fwd[k];
			avl_query(avltree, sequence_fwd + k, &avlpivot, NULL, qcmp_avlkey);
			avl_insert(&avltree, avlpivot, avlentry);
		}
		gettimeofday(&end, NULL);
		msec_avltree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		avl_clear();

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			map.insert(std::make_pair(sequence_fwd[k], sequence_fwd[k]));
		}
		gettimeofday(&end, NULL);
		msec_map[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		map.clear();

#ifndef NO_QMAP
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			qmap.insert(sequence_fwd[k], sequence_fwd[k]);
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		qmap.clear();
#endif
	}
	print_stats_md("append elements");

	for(i = 0; i < RUNS; i++) {
		gettimeofday(&start, NULL);
		for(k = ELEMS; k > 0; k--) {
			entry.key = sequence_rev[k];
			entry.value = sequence_rev[k];
			btree_insert(btree, &entry);
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		btree_clear(btree);

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			avlentry = avldata + k;
			avlentry->key = sequence_rev[k];
			avlentry->value = sequence_rev[k];
			avl_query(avltree, sequence_rev + k, &avlpivot, NULL, qcmp_avlkey);
			avl_insert(&avltree, avlpivot, avlentry);
		}
		gettimeofday(&end, NULL);
		msec_avltree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		avl_clear();

		gettimeofday(&start, NULL);
		for(k = ELEMS; k > 0; k--) {
			map.insert(std::make_pair(sequence_rev[k], sequence_rev[k]));
		}
		gettimeofday(&end, NULL);
		msec_map[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		map.clear();

#ifndef NO_QMAP
		gettimeofday(&start, NULL);
		for(k = ELEMS; k > 0; k--) {
			qmap.insert(sequence_rev[k], sequence_rev[k]);
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		qmap.clear();
#endif
	}
	print_stats_md("prepend elements");

	for(i = 0; i < RUNS; i++) {
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			entry.key = sequence_rnd[k];
			entry.value = sequence_rnd[k];
			btree_insert(btree, &entry);
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		btree_clear(btree);

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			avlentry = avldata + k;
			avlentry->key = sequence_rnd[k];
			avlentry->value = sequence_rnd[k];
			avl_query(avltree, sequence_rnd + k, &avlpivot, NULL, qcmp_avlkey);
			avl_insert(&avltree, avlpivot, avlentry);
		}
		gettimeofday(&end, NULL);
		msec_avltree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		avl_clear();

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			map.insert(std::make_pair(sequence_rnd[k], sequence_rnd[k]));
		}
		gettimeofday(&end, NULL);
		msec_map[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		map.clear();

#ifndef NO_QMAP
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			qmap.insert(sequence_rnd[k], sequence_rnd[k]);
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		qmap.clear();
#endif
	}
	print_stats_md("random insert elements");
}

static void bench_remove()
{
	struct timeval start;
	struct timeval end;
	size_t i;
	size_t k;
	entry_t entry;
	entry_t *pentry;
	avlentry_t *avlentry;

	map.clear();
#ifndef NO_QMAP
	qmap.clear();
#endif
	btree_clear(btree);
	for(i = 0; i < RUNS; i++) {
		avl_clear();
		for(k = 0; k < ELEMS; k++) {
			entry.key = k;
			entry.value = k;
			btree_insert(btree, &entry);
			avlentry = avldata + k;
			avlentry->key = k;
			avlentry->value = k;
			avl_insert(&avltree, NULL, avlentry);
#ifndef NO_QMAP
			qmap[k] = k;
#endif
			map[k] = k;
		}
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			entry.key = sequence_rnd[k];
			btree_remove(btree, &entry);
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			avl_query(avltree, sequence_rnd + k, &avlentry, NULL, qcmp_avlkey);
			avl_remove(&avltree, avlentry);
		}
		gettimeofday(&end, NULL);
		msec_avltree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			map.erase(map.find(sequence_rnd[k]));
		}
		gettimeofday(&end, NULL);
		msec_map[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;

#ifndef NO_QMAP
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			qmap.erase(qmap.find(sequence_rnd[k]));
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		assert(qmap.size() == 0);
#endif
		assert(map.size() == 0);
		assert(btree_size(btree) == 0);
	}
	print_stats_md("random remove elements");

	for(i = 0; i < RUNS; i++) {
		avl_clear();
		for(k = 0; k < ELEMS; k++) {
			entry.key = k;
			entry.value = k;
			btree_insert(btree, &entry);
			avlentry = avldata + k;
			avlentry->key = k;
			avlentry->value = k;
			avl_insert(&avltree, NULL, avlentry);
#ifndef NO_QMAP
			qmap[k] = k;
#endif
			map[k] = k;
		}
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			entry.key = sequence_fwd[k];
			btree_remove(btree, &entry);
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			avl_query(avltree, sequence_fwd + k, &avlentry, NULL, qcmp_avlkey);
			avl_remove(&avltree, avlentry);
		}
		gettimeofday(&end, NULL);
		msec_avltree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			map.erase(map.find(sequence_fwd[k]));
		}
		gettimeofday(&end, NULL);
		msec_map[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;

#ifndef NO_QMAP
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			qmap.erase(qmap.find(sequence_fwd[k]));
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		assert(qmap.size() == 0);
#endif
		assert(map.size() == 0);
		assert(btree_size(btree) == 0);
	}
	print_stats_md("remove first element");

	for(i = 0; i < RUNS; i++) {
		avl_clear();
		for(k = 0; k < ELEMS; k++) {
			entry.key = k;
			entry.value = k;
			btree_insert(btree, &entry);
			avlentry = avldata + k;
			avlentry->key = k;
			avlentry->value = k;
			avl_insert(&avltree, NULL, avlentry);
#ifndef NO_QMAP
			qmap[k] = k;
#endif
			map[k] = k;
		}
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			entry.key = sequence_rev[k];
			btree_remove(btree, &entry);
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			avl_query(avltree, sequence_rev + k, &avlentry, NULL, qcmp_avlkey);
			avl_remove(&avltree, avlentry);
		}
		gettimeofday(&end, NULL);
		msec_avltree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			map.erase(map.find(sequence_rev[k]));
		}
		gettimeofday(&end, NULL);
		msec_map[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;

#ifndef NO_QMAP
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			qmap.erase(qmap.find(sequence_rev[k]));
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		assert(qmap.size() == 0);
#endif
		assert(map.size() == 0);
		assert(btree_size(btree) == 0);
	}
	print_stats_md("remove last element");
}

static void bench_iterate()
{
	struct timeval start;
	struct timeval end;
	size_t i;
	size_t k;
	entry_t entry;
	entry_t *pentry;
	btree_it_t it;
	avlentry_t *avlentry;

	map.clear();
#ifndef NO_QMAP
	qmap.clear();
#endif
	btree_clear(btree);
	avl_clear();
	for(k = 0; k < ELEMS; k++) { // prepare both for access benchmarks
		entry.key = k;
		entry.value = k;
		btree_insert(btree, &entry);
		avlentry = avldata + k;
		avlentry->key = k;
		avlentry->value = k;
		avl_insert(&avltree, NULL, avlentry);
#ifndef NO_QMAP
		qmap[k] = k;
#endif
		map[k] = k;
	}

	for(i = 0; i < RUNS; i++) {
		gettimeofday(&start, NULL);
		for(btree_find_begin(btree, &it); it.element; btree_iterate_next(&it)) {
			values[it.index] = ((entry_t*)it.element)->value;
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

		gettimeofday(&start, NULL);
		k = 0;
		for(avlentry_t *cur = avl_first(avltree); cur; cur = avl_next(cur)) {
			values[k++] = cur->value;
		}
		gettimeofday(&end, NULL);
		msec_avltree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

		gettimeofday(&start, NULL);
		k = 0;
		for(auto it = map.cbegin(); it != map.cend(); ++it) {
			values[k++] = it->second;
		}
		gettimeofday(&end, NULL);
		msec_map[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

#ifndef NO_QMAP
		gettimeofday(&start, NULL);
		k = 0;
		for(auto it = qmap.begin(); it != qmap.end(); ++it) {
			values[k++] = *it;
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);
#endif
	}
	print_stats_md("forward iteration");

	for(i = 0; i < RUNS; i++) {
		gettimeofday(&start, NULL);
		for(btree_find_end(btree, &it); it.index;) {
			btree_iterate_prev(&it);
			values[it.index] = ((entry_t*)it.element)->value;
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

		gettimeofday(&start, NULL);
		k = 0;
		for(avlentry_t *cur = avl_last(avltree); cur; cur = avl_prev(cur)) {
			values[k++] = cur->value;
		}
		gettimeofday(&end, NULL);
		msec_avltree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

		gettimeofday(&start, NULL);
		k = 0;
		for(auto it = map.crbegin(); it != map.crend(); ++it) {
			values[k++] = it->second;
		}
		gettimeofday(&end, NULL);
		msec_map[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);

#ifndef NO_QMAP
		gettimeofday(&start, NULL);
		k = 0;
		for(auto it = qmap.end(); it != qmap.begin();) {
			--it;
			values[k++] = *it;
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		consume(values);
#endif
	}
	print_stats_md("reverse iteration");
}

int main()
{
	btree = btree_new(BTREE_ORDER, sizeof(entry_t), (btree_cmp_t)cmp_entry, 0);

	mkseq(ELEMS);
	
	bench_remove();

	bench_insert();

	bench_read();

	bench_iterate();

	return 0;
}

