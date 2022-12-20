#include <set>

bool evaluate_func(Datum *c_hash, Datum *q_hash, int k, int opt_k, int opt_l);
std::set<int> query(char *s, char *q_hash, int len, int len2, int opt_k);