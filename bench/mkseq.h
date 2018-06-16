#ifdef __cplusplus
extern "C" {
#endif
extern size_t *sequence_rnd;
extern size_t *sequence_fwd;
extern size_t *sequence_rev;
extern size_t *sequence_first;
extern size_t *sequence_last;
extern size_t *sequence_middle;

void mkseq(size_t n);
#ifdef __cplusplus
}
#endif

