#include <stdio.h>
#include <stdlib.h>

#include "helper.h"

size_t *sequence_rnd;
size_t *sequence_fwd;
size_t *sequence_rev;
size_t *sequence_first;
size_t *sequence_last;
size_t *sequence_middle;

void mkseq(size_t n)
{
	sequence_rnd = (size_t*)malloc(sizeof(size_t) * n);
	sequence_fwd = (size_t*)malloc(sizeof(size_t) * n);
	sequence_rev = (size_t*)malloc(sizeof(size_t) * n);
	sequence_first = (size_t*)malloc(sizeof(size_t) * n);
	sequence_last = (size_t*)malloc(sizeof(size_t) * n);
	sequence_middle = (size_t*)malloc(sizeof(size_t) * n);

	srand(0);
	for(size_t i = 0; i < n; i++) {
		sequence_rnd[i] = i;
		sequence_fwd[i] = i;
		sequence_rev[i] = n - i - 1;
		sequence_first[i] = 0;
		sequence_last[i] = n - 1;
		sequence_middle[i] = n / 2;
	}
	for(size_t i = 0; i < 10 * n; i++) {
		size_t a = rand() % n;
		size_t b = rand() % n;
		size_t tmp = sequence_rnd[a];
		sequence_rnd[a] = sequence_rnd[b];
		sequence_rnd[b] = tmp;
	}
}

void consume(int *dummy) {}

