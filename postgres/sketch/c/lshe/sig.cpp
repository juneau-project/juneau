extern "C"
{
#include "fnv/fnv.h"
#include "postgres.h"
}

#include <string>
#include <iostream>
#include "sig.h"
#include <climits>

using namespace std;

int HashValueSize = 8;

u_int64_t hv(char *val, char *b1, char *b2, int idx)
{
  u_int64_t hv1;
  hv1 = fnv_64a_buf(b1, HashValueSize, FNV1A_64_INIT);
  hv1 = fnv_64a_str(val, hv1) + hv1;

  u_int64_t hv2;
  hv2 = fnv_64a_buf(b2, HashValueSize, FNV1A_64_INIT);
  hv2 = fnv_64a_str(val, hv2) + hv2;

  return (u_int64_t)(hv1 + idx * hv2);
}

u_int64_t *sig(char *ch_arr, int len, int num_hash)
{
  // srand(100);
  char b1[8] = {'i', 'x', 'x', 'r', 'r', 't', 'k', 'h'};
  char b2[8] = {'z', 'j', 'w', 'j', 'x', 'x', 'j', 'a'};
  //   for (int i = 0; i < HashValueSize; i++)
  //   {
  //     b1[i] = 'a' + rand() % 26;
  //     cout << b1[i];
  //   }

  //   cout << endl;
  //   cout << "------" << endl;

  //   for (int i = 0; i < HashValueSize; i++)
  //   {
  //     b2[i] = 'a' + rand() % 26;
  // cout << b2[i];
  //   }
  //   cout << endl;
  //   cout << endl;

  string s = string(ch_arr, ch_arr + len);
  size_t pos = 0;
  string token;
  string delimiter = "#sep#";

  // elog(WARNING, "%s", s.c_str());

  static u_int64_t *minimums;
  minimums = (u_int64_t *)malloc(num_hash * sizeof(u_int64_t));

  for (int i = 0; i < num_hash; i++)
  {
    minimums[i] = ULLONG_MAX;
  }

  while ((pos = s.find(delimiter)) != std::string::npos)
  {
    token = s.substr(0, pos);

    // cout << token << endl;
    // elog(WARNING, "%s", token.c_str());

    for (int i = 0; i < num_hash; i++)
    {
      minimums[i] = min(minimums[i], hv(&token[0], b1, b2, i));
    }

    s.erase(0, pos + delimiter.length());
  }

  // cout << s << endl;
  // elog(WARNING, "%s", s.c_str());
  for (int i = 0; i < num_hash; i++)
  {
    minimums[i] = min(minimums[i], hv(&s[0], b1, b2, i));
    cout << minimums[i] << ",";
  }

  return minimums;
}

// int main()
// {
//   char months[] = "";
//   sig(months, 0, 128);
//   return 0;
// }
