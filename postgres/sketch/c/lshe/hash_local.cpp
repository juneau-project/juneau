#include <algorithm>
#include <iterator>
#include <cstdint>
#include <inttypes.h>

#include "sig.h"
#include "hash_local.h"
#include <iostream>

using namespace std;

// change int back to Datum
// local testing: change Datum to int
void hash_func(int *hs, char *ch_arr, int num_hash)
{
  int hash_value_size = 8;
  u_int64_t *signature = sig(ch_arr, num_hash);

  for (int i = 0; i < num_hash; i++)
  {
    uint8_t *buf = (uint8_t *)&signature[i];

    copy(buf, buf + hash_value_size, hs + i * hash_value_size);
  }
}

// void update_hash_func(int *hs, int *prev_hash, char *ch_arr, int num_hash)
// {
//   int hash_value_size = 8;
//   u_int64_t *signature = sig(ch_arr, num_hash);

//   for (int i = 0; i < num_hash; i++)
//   {
//     u_int64_t new_num = signature[i];

//     // get the previous signature from the 8 uint8_t
//     u_int64_t old_num;

//     uint8_t prev_signature[8];
//     int idx = 0;
//     for (int j = i * hash_value_size; j < (i + 1) * hash_value_size; j++)
//     {
//       prev_signature[idx] = DatumGetInt16(prev_hash[j]);
//       idx++;
//     }
//     memcpy(&old_num, prev_signature, hash_value_size);

//     u_int64_t smaller_num = min(new_num, old_num);

//     // store the smaller of the new_num and old_num
//     uint8_t *buf = (uint8_t *)&smaller_num;
//     copy(buf, buf + hash_value_size, hs + i * hash_value_size);
//   }
// }

// int main()
// {
//   u_int64_t u64[] = {8446744073709551615, 46744073709551615};
//   for (int i = 0; i < 2; i++)
//   {
//     uint8_t *buf = (uint8_t *)&u64[i];
//     for (int j = 0; j < 8; j++)
//     {
//       cout << unsigned(buf[j]) << ",";
//     }
//     cout << endl;
//   }

//   uint8_t u8[] = {255, 255, 23, 118, 251, 220, 56, 117, 255, 255, 207, 196, 124, 17, 166, 0};
//   u_int64_t u64_1;
//   u_int64_t u64_2;

//   copy(u8, u8 + 8, &u64_1);
//   // memcpy(&u64_1, u8, 8);
//   printf("%" PRId64 "\n", u64_1);
//   // memcpy(&u64_1, u8 + 8, 8);
//   // printf("%" PRId64 "\n", u64_1);
//   // copy(u8, u8 + 8, &u64_1);
//   // copy(u8+8, u8 + 16, &u64_2);

//   // printf("%" PRId64 "\n", u64_2);
//   // cout << unsigned(u64_1) << endl;
//   // cout << unsigned(u64_2) << endl;

//   return 0;
// }

int main()
{
  char months[] = "male#sep#female";
  int hash_value_size = 8;
  int num_hash = 128;
  int length = hash_value_size * num_hash;
  int arr[length];
  hash_func(arr, months, num_hash);

  // int *buf = convert_to_byte_array(2729518115542292064);
  // for (int i = 0; i < 8; i++) {
  //   cout << buf[i] << endl;
  // }

  // for (int i = 0; i < 8; i++) {
  //   cout << buf[i];
  // }
  // cout << buf << endl;

  cout << "fuck you" << endl;
  for (int i = 0; i < length; i++) {
    cout << arr[i] << ',';
  }

  cout << endl;

  return 0;
}