#include <limits>
#include <sys/time.h>
#include <btree/memory.h>
#include <QMap>

#define BTREE_ORDER 15
#define RUNS 5
#define PARTIAL_AVG (RUNS / 5)
#define ELEMS 1000000
#define SHUFFLE ELEMS

static_assert(RUNS % PARTIAL_AVG == 0);

typedef struct {
	int key;
	int value;
} entry_t;

static uint64_t msec_qmap[RUNS];
static uint64_t msec_btree[RUNS];

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

static void shuffle(size_t *sequence)
{
	srand(0);
	for(size_t i = 0; i < ELEMS; i++)
		sequence[i] = i;
	for(size_t i = 0; i < SHUFFLE; i++) {
		size_t a = rand() % ELEMS;
		size_t b = rand() % ELEMS;
		size_t tmp = sequence[a];
		sequence[a] = sequence[b];
		sequence[b] = tmp;
	}
}

static void calc_stats(const uint64_t *samples, uint64_t &min, uint64_t &max, uint64_t &avg)
{
	uint64_t partavg = 0;
	min = std::numeric_limits<uint64_t>::max();
	max = std::numeric_limits<uint64_t>::min();
	avg = 0;

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
}

static void print_stats(const char *bench)
{
	uint64_t min;
	uint64_t max;
	uint64_t avg;
	uint64_t median;
	printf("%s:\n", bench);
	calc_stats(msec_qmap, min, max, avg);
	printf("  qmap : min=%4llu     max=%4llu     avg=%4llu\n", min, max, avg);
	calc_stats(msec_btree, min, max, avg);
	printf("  btree: min=%4llu     max=%4llu     avg=%4llu\n", min, max, avg);
	printf("\n");
}

int main()
{
	struct timeval start;
	struct timeval end;
	size_t i;
	size_t k;
	QMap<int, int> qmap;
	entry_t entry;
	entry_t *pentry;
	btree_t *btree = btree_new(BTREE_ORDER, sizeof(entry_t), (btree_cmp_t)cmp_entry, 0);
	size_t *sequence = (size_t*)malloc(sizeof(size_t) * ELEMS);
	shuffle(sequence);

append:
	for(i = 0; i < RUNS; i++) {
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			entry.key = k;
			entry.value = k;
			btree_insert(btree, &entry);
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		btree_clear(btree);

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			qmap[k] = k;
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		qmap.clear();
	}

	print_stats("append elements");

prepend:
	for(i = 0; i < RUNS; i++) {
		gettimeofday(&start, NULL);
		for(k = ELEMS; k > 0; k--) {
			entry.key = k;
			entry.value = k;
			btree_insert(btree, &entry);
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		btree_clear(btree);

		gettimeofday(&start, NULL);
		for(k = ELEMS; k > 0; k--) {
			qmap[k] = k;
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		qmap.clear();
	}

	print_stats("prepend elements");

insert:
	for(i = 0; i < RUNS; i++) {
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			entry.key = sequence[k];
			entry.value = sequence[k];
			btree_insert(btree, &entry);
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		btree_clear(btree);

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			qmap[sequence[k]] = sequence[k];
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		qmap.clear();
	}

	print_stats("random insert elements");

access:
	for(i = 0; i < RUNS; i++) {
		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			entry.key = sequence[k];
			btree_get(btree, &entry);
		}
		gettimeofday(&end, NULL);
		msec_btree[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		btree_clear(btree);

		gettimeofday(&start, NULL);
		for(k = 0; k < ELEMS; k++) {
			qmap[sequence[k]];
		}
		gettimeofday(&end, NULL);
		msec_qmap[i] = (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
		qmap.clear();
	}

	print_stats("random access");

	return 0;
}

