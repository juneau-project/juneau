#include "fnv.h"
#include <stdio.h>
#include <inttypes.h>

int main() {
  // char buf[] = "hello";
  Fnv32_t hash_val;

  int i = 0;


  hash_val = fnv_32a_buf(&i, sizeof(i), FNV1_32A_INIT);
  printf("%" PRIu32 "\n", hash_val);

  i = 1;

  hash_val = fnv_32a_buf(&i, sizeof(i), FNV1_32A_INIT);
  printf("%" PRIu32 "\n", hash_val);

  i = 2;

  hash_val = fnv_32a_buf(&i, sizeof(i), FNV1_32A_INIT);
  printf("%" PRIu32 "\n", hash_val);
  // hash_val = fnv_32a_buf(ptr, sizeof(*ptr), hash_val);
  // hash_val = fnv_32a_buf(ptr, sizeof(*ptr), FNV1_32A_INIT);
  // hash_val = fnv_64a_str("more data", FNV1A_64_INIT);
  // hash_val = fnv_64a_str("clearly", hash_val);

  return 0;
}

