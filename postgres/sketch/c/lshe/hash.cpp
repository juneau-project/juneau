extern "C"
{
#include "postgres.h"
}

#include <algorithm>
#include <iterator>
#include <cstdint>
#include <inttypes.h>

#include "sig.h"
#include "hash.h"
#include <iostream>

using namespace std;

void hash_func(Datum *hs, char *ch_arr, int len, int num_hash)
{
  int hash_value_size = 8;
  u_int64_t *signature = sig(ch_arr, len, num_hash);

  for (int i = 0; i < num_hash; i++)
  {
    uint8_t *buf = (uint8_t *)&signature[i];

    copy(buf, buf + hash_value_size, hs + i * hash_value_size);
  }
}

void update_hash_func(Datum *hs, Datum *prev_hash, char *ch_arr, int len, int num_hash)
{
  int hash_value_size = 8;
  u_int64_t *signature = sig(ch_arr, len, num_hash);

  for (int i = 0; i < num_hash; i++)
  {
    u_int64_t new_num = signature[i];

    // get the previous signature from the 8 uint8_t
    u_int64_t old_num;

    uint8_t prev_signature[8];
    int idx = 0;
    for (int j = i * hash_value_size; j < (i + 1) * hash_value_size; j++)
    {
      prev_signature[idx] = DatumGetInt16(prev_hash[j]);
      idx++;
    }
    memcpy(&old_num, prev_signature, hash_value_size);

    u_int64_t smaller_num = min(new_num, old_num);

    // store the smaller of the new_num and old_num
    uint8_t *buf = (uint8_t *)&smaller_num;
    copy(buf, buf + hash_value_size, hs + i * hash_value_size);
  }
}