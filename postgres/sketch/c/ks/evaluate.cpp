#include <math.h>
#include <cstdint>
#include <iostream>

#include "evaluate.h"

using namespace std;

// return the number of elements in the list that is <= key
// need to -1 to get the index of the largest element <= key
int bs_int(int64_t *array, int key, int low, int high)
{
  int count = 0;

  while (low <= high)
  {
    int mid = (high + low) / 2;

    if (array[mid] <= key)
    {
      count = mid + 1;
      low = mid + 1;
    }
    else
    {
      high = mid - 1;
    }
  }

  return count;
}

double dist_int(int64_t *h1, int len1, int64_t *h2, int len2)
{
  double max_dis = 0.0;

  for (int i = 0; i < len1 / 2; i++)
  {
    int h1_sm_eq_count = h1[i + len1 / 2];

    int h2_count_idx = bs_int(h2, h1[i], 0, len2 / 2 - 1);
    int h2_sm_eq_count;
    if (h2_count_idx == 0)
    {
      h2_sm_eq_count = 0;
    }
    else
    {
      h2_sm_eq_count = h2[h2_count_idx - 1 + len2 / 2];
    }

    // cout << "h1: # <= " << h1[i] << " " << h1_sm_eq_count << endl;
    // cout << "h2: # <= " << h1[i] << " " << h2_sm_eq_count << endl;

    // formula adapted from https://en.wikipedia.org/wiki/Empirical_distribution_function
    double dist = fabs((double)h1_sm_eq_count / (double)h1[len1 - 1] - (double)h2_sm_eq_count / (double)h2[len2 - 1]);

    if (dist > max_dis)
    {
      max_dis = dist;
    }

    // cout << "-----" << endl;
  }

  return max_dis;
}

// length is the length of the histogram array (e1, e2, ..., en, o1, o2, ..., on)
// elem_num is the number of unique elements in the column = len/2
// total_num is the total number of elements in the column
// t is the threshold
double ks_evaluate_int(int64_t *h1, int len1, int64_t *h2, int len2)
{
  int elem_num1 = len1 / 2, elem_num2 = len2 / 2;

  double max_dist = dist_int(h1, len1, h2, len2);
  double max_dist2 = dist_int(h2, len2, h1, len1);

  if (max_dist2 > max_dist)
  {
    max_dist = max_dist2;
  }

  double exponent = -2 * elem_num1 * elem_num2 * pow(max_dist, 2) / (elem_num1 + elem_num2);

  // if (exp(exponent) >= t)
  // {
  //   return true;
  // }

  // return false;

  double score = exp(exponent);
  score = round(score * 1000.0) / 1000.0;

  return score;
}

// int main()
// {
//   // test everything
//   // int64_t h1[] = {1, 2, 3, 5, 4, 8, 12, 16};
//   // int64_t h2[] = {1, 2, 3, 4, 5, 4, 8, 12, 16, 20};

//   // bool b = ks_evaluate_int(h1, 8, h2, 10, 0.8);
//   // cout << b << endl;

//   // test bsearch
//   // int64_t h1[] = {1, 2, 3, 5};
//   // // lo = 0, hi = size - 1
//   // int idx = bs_int(h1, 30, 0, 3); // --> 5
//   // cout << idx << endl;

//   // cout << interval * floor(a/interval) << endl;

//   int digits = 4;
//   int64_t interval = int64_t(round(pow(10, digits - 3)));
//   cout << interval  << endl;
//   cout << "done" << endl;

//   return 0;
// }