extern "C"
{
#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include <catalog/pg_type.h>

  PG_MODULE_MAGIC;

  PG_FUNCTION_INFO_V1(pg_hist_int);
  PG_FUNCTION_INFO_V1(pg_hist_float);
  PG_FUNCTION_INFO_V1(pg_ks_evaluate);
}

#include <unordered_set>
#include <cstdint>
#include <cmath>
#include <string>
#include "math.h"

#include "hist.h"
#include "evaluate.h"

using namespace std;

int rand_range(int low, int high)
{
  return rand() % (high - low + 1) + low;
}

Datum pg_hist_int(PG_FUNCTION_ARGS)
{
  // deconstruct the numerical array
  ArrayType *vals;
  Oid vals_type;
  int16 vals_type_width;
  bool vals_type_by_value;
  char vals_type_alignment_code;
  Datum *arr_content;
  bool *vals_null_flags;
  int arr_len;
  vals = PG_GETARG_ARRAYTYPE_P(0);
  vals_type = ARR_ELEMTYPE(vals);
  arr_len = (ARR_DIMS(vals))[0];
  get_typlenbyvalalign(vals_type, &vals_type_width, &vals_type_by_value, &vals_type_alignment_code);
  deconstruct_array(vals, vals_type, vals_type_width, vals_type_by_value, vals_type_alignment_code,
                    &arr_content, &vals_null_flags, &arr_len);

  int num_digits = PG_GETARG_INT32(1);

  // --------------------------------
  // PREPROCESSING

  // set to check if there are more than 1000 unique elements
  unordered_set<int64_t> unique_s;
  // (sampled) int array
  int64_t *arr_content_int;

  // only do sampling if arr_len > 10,0000
  // int sample_size = 10000;
  // if (arr_len > sample_size)
  // {
  //   arr_content_int = (int64_t *)palloc(sizeof(int64_t) * sample_size);

  //   // make sure no repeated rand_idx
  //   unordered_set<int> sample_set;
  //   for (int i = 0; i < sample_size; i++)
  //   {
  //     int rand_idx = rand_range(0, arr_len - 1);

  //     while (sample_set.count(rand_idx) == 1)
  //     {
  //       rand_idx = rand_range(0, arr_len - 1);
  //     }

  //     sample_set.insert(rand_idx);
  //     arr_content_int[i] = DatumGetInt64(arr_content[rand_idx]);
  //     unique_s.insert(DatumGetInt64(arr_content[rand_idx]));
  //   }
  //   arr_len = sample_size;
  // }
  // else
  // {
  //   arr_content_int = (int64_t *)palloc(sizeof(int64_t) * arr_len);

  //   // insert into array
  //   for (int i = 0; i < arr_len; i++)
  //   {
  //     arr_content_int[i] = DatumGetInt64(arr_content[i]);
  //     unique_s.insert(DatumGetInt64(arr_content[i]));
  //   }
  // }

  arr_content_int = (int64_t *)palloc(sizeof(int64_t) * arr_len);

  // insert into array
  for (int i = 0; i < arr_len; i++)
  {
    arr_content_int[i] = DatumGetInt64(arr_content[i]);
    unique_s.insert(DatumGetInt64(arr_content[i]));
  }

  // --------------------------------------------------------
  // logic below should remain unchanged
  bool special = false;
  if (unique_s.size() > 1000)
  {
    special = true;
  }

  // initialize histogram array
  int16 type_width;
  bool type_by_value;
  char type_alignment_code;
  Datum *hist;
  ArrayType *hist_arr;
  int hist_len;

  // create histogram
  if (special)
  {
    // unordered_map should initialize non-existing keys to 0
    // https://stackoverflow.com/questions/59192236/does-stdunordered-map-operator-do-zero-initialization-for-non-exisiting-key
    unordered_map<int64_t, int> m;

    if (num_digits == 0)
    {
      // default is to keep the first 4 digits
      num_digits = 4;
    }

    int64_t ten_pow = int64_t(round(pow(10, num_digits)));

    for (int i = 0; i < arr_len; i++)
    {
      if (arr_content_int[i] > ten_pow)
      {
        int digits = int(log10(arr_content_int[i]) + 1);
        m[floor(arr_content_int[i] / pow(10, digits - num_digits)) * pow(10, digits - num_digits)]++;
      }
      else
      {
        m[arr_content_int[i]]++;
      }
    }

    hist_len = 2 * m.size();
    hist = (Datum *)palloc(sizeof(Datum) * hist_len);

    for (int i = 0; i < 2 * m.size(); i++)
    {
      hist[i] = 0;
    }

    construct_hist_int_special(m, hist);
  }
  else
  {
    hist_len = 2 * unique_s.size();
    hist = (Datum *)palloc(sizeof(Datum) * hist_len);

    // initialize array to be {0}
    for (int i = 0; i < hist_len; i++)
    {
      hist[i] = 0;
    }

    construct_hist_int(arr_content_int, hist, arr_len, unique_s.size());
  }

  get_typlenbyvalalign(INT8OID, &type_width, &type_by_value, &type_alignment_code);
  hist_arr = construct_array(hist, hist_len, INT8OID, type_width, type_by_value, type_alignment_code);

  PG_RETURN_ARRAYTYPE_P(hist_arr);
}

Datum pg_hist_float(PG_FUNCTION_ARGS)
{
  // deconstruct the numerical array
  ArrayType *vals;
  Oid vals_type;
  int16 vals_type_width;
  bool vals_type_by_value;
  char vals_type_alignment_code;
  Datum *arr_content;
  bool *vals_null_flags;
  int arr_len;
  vals = PG_GETARG_ARRAYTYPE_P(0);
  vals_type = ARR_ELEMTYPE(vals);
  arr_len = (ARR_DIMS(vals))[0];
  get_typlenbyvalalign(vals_type, &vals_type_width, &vals_type_by_value, &vals_type_alignment_code);
  deconstruct_array(vals, vals_type, vals_type_width, vals_type_by_value, vals_type_alignment_code,
                    &arr_content, &vals_null_flags, &arr_len);

  int num_digits = PG_GETARG_INT32(1);

  // --------------------------------
  // PREPROCESSING

  // set to check if there are more than 1000 unique elements
  unordered_set<int64_t> unique_s;
  // (sampled) int array
  int64_t *arr_content_int;

  // only do sampling if arr_len > 10,0000
  int sample_size = 100000;
  if (arr_len > sample_size)
  {
    arr_content_int = (int64_t *)palloc(sizeof(int64_t) * sample_size);

    // make sure no repeated rand_idx
    unordered_set<int> sample_set;
    for (int i = 0; i < sample_size; i++)
    {
      int rand_idx = rand_range(0, arr_len - 1);

      while (sample_set.count(rand_idx) == 1)
      {
        rand_idx = rand_range(0, arr_len - 1);
      }

      sample_set.insert(rand_idx);

      int64_t val = int64_t(round(DatumGetFloat8(arr_content[rand_idx])));
      arr_content_int[i] = val;
      unique_s.insert(val);
    }
    arr_len = sample_size;
  }
  else
  {
    arr_content_int = (int64_t *)palloc(sizeof(int64_t) * arr_len);

    // insert into array
    for (int i = 0; i < arr_len; i++)
    {
      int64_t val = int64_t(round(DatumGetFloat8(arr_content[i])));
      arr_content_int[i] = val;
      unique_s.insert(val);
    }
  }

  // --------------------------------------------------------
  // logic below should remain unchanged
  bool special = false;
  if (unique_s.size() > 1000)
  {
    special = true;
  }

  // initialize histogram array
  int16 type_width;
  bool type_by_value;
  char type_alignment_code;
  Datum *hist;
  ArrayType *hist_arr;
  int hist_len;

  // create histogram
  if (special)
  {
    // unordered_map should initialize non-existing keys to 0
    // https://stackoverflow.com/questions/59192236/does-stdunordered-map-operator-do-zero-initialization-for-non-exisiting-key
    unordered_map<int64_t, int> m;

    if (num_digits == 0)
    {
      // default is to keep the first 3 digits
      num_digits = 3;
    }

    int64_t ten_pow = int64_t(round(pow(10, num_digits)));

    for (int i = 0; i < arr_len; i++)
    {
      if (arr_content_int[i] > ten_pow)
      {
        int digits = int(log10(arr_content_int[i]) + 1);
        m[floor(arr_content_int[i] / pow(10, digits - num_digits)) * pow(10, digits - num_digits)]++;
      }
      else
      {
        m[arr_content_int[i]]++;
      }
    }

    hist_len = 2 * m.size();
    hist = (Datum *)palloc(sizeof(Datum) * hist_len);

    for (int i = 0; i < 2 * m.size(); i++)
    {
      hist[i] = 0;
    }

    construct_hist_int_special(m, hist);
  }
  else
  {
    hist_len = 2 * unique_s.size();
    hist = (Datum *)palloc(sizeof(Datum) * hist_len);

    // initialize array to be {0}
    for (int i = 0; i < hist_len; i++)
    {
      hist[i] = 0;
    }

    construct_hist_int(arr_content_int, hist, arr_len, unique_s.size());
  }

  get_typlenbyvalalign(INT8OID, &type_width, &type_by_value, &type_alignment_code);
  hist_arr = construct_array(hist, hist_len, INT8OID, type_width, type_by_value, type_alignment_code);

  PG_RETURN_ARRAYTYPE_P(hist_arr);
}

Datum pg_ks_evaluate(PG_FUNCTION_ARGS)
{
  // deconstruct h1, h2
  ArrayType *a1, *a2;
  Oid arrayElementType1, arrayElementType2;
  int16 arrayElementTypeWidth1, arrayElementTypeWidth2;
  bool arrayElementTypeByValue1, arrayElementTypeByValue2;
  char arrayElementTypeAlignmentCode1, arrayElementTypeAlignmentCode2;
  Datum *arr_content1, *arr_content2;
  bool *arrayNullFlags1, *arrayNullFlags2;
  int len1, len2;
  a1 = PG_GETARG_ARRAYTYPE_P(0);
  a2 = PG_GETARG_ARRAYTYPE_P(1);

  len1 = (ARR_DIMS(a1))[0];
  len2 = (ARR_DIMS(a2))[0];
  arrayElementType1 = ARR_ELEMTYPE(a1);
  get_typlenbyvalalign(arrayElementType1, &arrayElementTypeWidth1, &arrayElementTypeByValue1, &arrayElementTypeAlignmentCode1);

  arrayElementType2 = ARR_ELEMTYPE(a2);
  get_typlenbyvalalign(arrayElementType2, &arrayElementTypeWidth2, &arrayElementTypeByValue2, &arrayElementTypeAlignmentCode2);

  deconstruct_array(a1, arrayElementType1, arrayElementTypeWidth1, arrayElementTypeByValue1, arrayElementTypeAlignmentCode1,
                    &arr_content1, &arrayNullFlags1, &len1);
  deconstruct_array(a2, arrayElementType2, arrayElementTypeWidth2, arrayElementTypeByValue2, arrayElementTypeAlignmentCode2,
                    &arr_content2, &arrayNullFlags2, &len2);

  // double t = PG_GETARG_FLOAT8(2);

  // copy to int64_t arrays
  int64_t *h1, *h2;
  h1 = (int64_t *)palloc(sizeof(int64_t) * len1);
  h2 = (int64_t *)palloc(sizeof(int64_t) * len2);

  for (int i = 0; i < len1; i++)
  {
    h1[i] = DatumGetInt64(arr_content1[i]);
  }

  for (int i = 0; i < len2; i++)
  {
    h2[i] = DatumGetInt64(arr_content2[i]);
  }

  // check if score >= threshold
  // PG_RETURN_FLOAT8(ks_evaluate_int(h1, len1, h2, len2, t));
  PG_RETURN_FLOAT8(ks_evaluate_int(h1, len1, h2, len2));
}