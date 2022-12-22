#include <stdio.h>
#include <iostream>
#include <math.h>
#include <float.h>

#include "probability.h"

using namespace std;

long double falsePositive(int x, int q, int l, int k, double t)
{
  return 1.0 - pow(1.0 - pow(t / (1.0 + (long double)x / (long double)q - t), (long double)k), (long double)l);
}

long double falseNegative(int x, int q, int l, int k, double t)
{
  return 1.0 - (1.0 - pow(1.0 - pow(t / (1.0 + (long double)x / (long double)q - t), (long double)k), (long double)l));
}

long double integralFP(int x, int q, int l, int k, double a, double b, double precision)
{
  long double area = 0.0;

  for (long double i = a; i < b; i += precision)
  {
    area += falsePositive(x, q, l, k, i + 0.5 * precision) * precision;
  }

  return area;
}

long double integralFN(int x, int q, int l, int k, double a, double b, double precision)
{
  long double area = 0.0;

  for (double i = a; i < b; i += precision)
  {
    area += falseNegative(x, q, l, k, i + 0.5 * precision) * precision;
  }

  return area;
}

long double prob_false_negative(int x, int q, int l, int k, double t, double precision)
{
  long double xq = (long double)x / (long double)q;

  if (xq >= 1.0)
  {
    return integralFN(x, q, l, k, t, 1.0, precision);
  }

  if (xq >= t)
  {
    return integralFN(x, q, l, k, t, xq, precision);
  }
  else
  {
    return 0.0;
  }
}

long double prob_false_positive(int x, int q, int l, int k, double t, double precision)
{
  long double xq = (long double)x / (long double)q;

  if (xq >= 1.0) {
    return integralFP(x, q, l, k, 0.0, t, precision);
  }

  if (xq >= t)
  {
    return integralFP(x, q, l, k, 0.0, t, precision);
  }
  else
  {
    return integralFP(x, q, l, k, 0.0, xq, precision);
  }
}

// maxK is the number of hash functions in each band
// numHash is the total number of hash functions used
// x is the domain size
// q is the query size
// t is the threshold value
// optK and optL are the pointers to store the optimal k and l values
void optimal_kl(int k_arg, int l_arg, int x, int q, double t, int *opt_k, int *opt_l)
{
  double min_error = DBL_MAX;
  double integration_precision = 0.01;

  for (int l = 1; l <= l_arg; l++)
  {
    for (int k = 1; k <= k_arg; k++)
    {
      double curr_fp = prob_false_positive(x, q, l, k, t, integration_precision);
      double curr_fn = prob_false_negative(x, q, l, k, t, integration_precision);
      double curr_err = curr_fp + curr_fn;

      if (min_error > curr_err)
      {
        min_error = curr_err;
        *opt_k = k;
        *opt_l = l;
      }
    }
  }
}

// int main() {
//   int k = 4;
//   long l = 64;
//   long x = 150;
//   long q = 250;
//   double t = 0.9;

//   int opt_k = -1, opt_l = -1;

//   optimal_kl(k, l, x, q, t, &opt_k, &opt_l);

//   cout << "final k: " << opt_k << endl;
//   cout << "final l: " << opt_l << endl;

//   return 0;
// }