#include <unordered_map>

void construct_hist_int(int64_t *arr, Datum *hist, int len, int elem_num);
void construct_hist_int_special(std::unordered_map<int64_t, int> m, Datum *hist);