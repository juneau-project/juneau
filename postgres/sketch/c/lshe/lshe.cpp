extern "C"
{
#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include <catalog/pg_type.h>

  PG_MODULE_MAGIC;

  PG_FUNCTION_INFO_V1(pg_test);
  PG_FUNCTION_INFO_V1(pg_hash);
  PG_FUNCTION_INFO_V1(pg_update_hash);
  PG_FUNCTION_INFO_V1(pg_evaluate);
  PG_FUNCTION_INFO_V1(pg_evaluate_old);
  PG_FUNCTION_INFO_V1(pg_optimal_kl);
}

#include <cstdint>
#include <algorithm>
#include <iterator>
#include <set>

#include "hash.h"
#include "evaluate.h"
#include "probability.h"

using namespace std;

Datum pg_hash(PG_FUNCTION_ARGS)
{
  VarChar *arg = (VarChar *)PG_GETARG_VARCHAR_P(0);
  // int len = PG_GETARG_INT32(1);
  int len = VARSIZE(arg) - VARHDRSZ;
  // elog(WARNING, "%d", len);
  char *str_arg = (char *)VARDATA(arg);
  char *str;
  str = (char *)malloc(sizeof(char) * len);
  copy(str_arg, str_arg + len, str);

  // can use memcpy
  // for (int i = 0; i < len; i++) {
  //   elog(WARNING, "%c", str_arg[i]);
  //   str[i] = str_arg[i];
  //   elog(WARNING, "%c", str[i]);
  // }

  int num_hash = PG_GETARG_INT32(1);

  // construct hash_array
  int16 type_width;
  bool type_by_value;
  char type_alignment_code;
  Datum *arr_content;
  ArrayType *arr;

  int hash_value_size = 8;
  int length = hash_value_size * num_hash;

  arr_content = (Datum *)palloc(sizeof(Datum) * length);
  hash_func(arr_content, str, len, num_hash);

  get_typlenbyvalalign(INT2OID, &type_width, &type_by_value, &type_alignment_code);
  arr = construct_array(arr_content, length, INT2OID, type_width, type_by_value, type_alignment_code);

  PG_RETURN_ARRAYTYPE_P(arr);
}

Datum pg_update_hash(PG_FUNCTION_ARGS)
{
  // deconstruct prev_hash
  ArrayType *vals;
  Oid vals_type;
  int16 vals_type_width;
  bool vals_type_by_value;
  char vals_type_alignment_code;
  Datum *prev_hash;
  bool *vals_null_flags;
  int vals_length;
  vals = PG_GETARG_ARRAYTYPE_P(0);
  vals_type = ARR_ELEMTYPE(vals);
  vals_length = (ARR_DIMS(vals))[0];
  get_typlenbyvalalign(vals_type, &vals_type_width, &vals_type_by_value, &vals_type_alignment_code);
  deconstruct_array(vals, vals_type, vals_type_width, vals_type_by_value, vals_type_alignment_code,
                    &prev_hash, &vals_null_flags, &vals_length);

  VarChar *arg = (VarChar *)PG_GETARG_VARCHAR_P(1);
  // int len = PG_GETARG_INT32(1);
  int len = VARSIZE(arg) - VARHDRSZ;
  // elog(WARNING, "%d", len);
  char *str_arg = (char *)VARDATA(arg);
  char *str;
  str = (char *)malloc(sizeof(char) * len);
  copy(str_arg, str_arg + len, str);

  int num_hash = PG_GETARG_INT32(2);

  // construct hash_array
  int16 type_width;
  bool type_by_value;
  char type_alignment_code;
  Datum *arr_content;
  ArrayType *arr;

  int hash_value_size = 8;
  int length = hash_value_size * num_hash;

  arr_content = (Datum *)palloc(sizeof(Datum) * length);
  update_hash_func(arr_content, prev_hash, str, len, num_hash);

  get_typlenbyvalalign(INT2OID, &type_width, &type_by_value, &type_alignment_code);
  arr = construct_array(arr_content, length, INT2OID, type_width, type_by_value, type_alignment_code);

  PG_RETURN_ARRAYTYPE_P(arr);
}

Datum pg_evaluate_old(PG_FUNCTION_ARGS)
{
  ArrayType *a1, *a2;
  Oid arrayElementType1, arrayElementType2;
  int16 arrayElementTypeWidth1, arrayElementTypeWidth2;
  bool arrayElementTypeByValue1, arrayElementTypeByValue2;
  char arrayElementTypeAlignmentCode1, arrayElementTypeAlignmentCode2;
  Datum *arrayContent1, *arrayContent2;
  bool *arrayNullFlags1, *arrayNullFlags2;
  int length1, length2;
  a1 = PG_GETARG_ARRAYTYPE_P(0);
  a2 = PG_GETARG_ARRAYTYPE_P(1);

  length1 = (ARR_DIMS(a1))[0];
  length2 = (ARR_DIMS(a2))[0];
  arrayElementType1 = ARR_ELEMTYPE(a1);
  get_typlenbyvalalign(arrayElementType1, &arrayElementTypeWidth1, &arrayElementTypeByValue1, &arrayElementTypeAlignmentCode1);

  arrayElementType2 = ARR_ELEMTYPE(a2);
  get_typlenbyvalalign(arrayElementType2, &arrayElementTypeWidth2, &arrayElementTypeByValue2, &arrayElementTypeAlignmentCode2);

  // deconstruct c_hash_array
  deconstruct_array(a1, arrayElementType1, arrayElementTypeWidth1, arrayElementTypeByValue1, arrayElementTypeAlignmentCode1,
                    &arrayContent1, &arrayNullFlags1, &length1);

  // deconstruct q_hash_array
  deconstruct_array(a2, arrayElementType2, arrayElementTypeWidth2, arrayElementTypeByValue2, arrayElementTypeAlignmentCode2,
                    &arrayContent2, &arrayNullFlags2, &length2);

  int k = PG_GETARG_INT32(2);
  int opt_k = PG_GETARG_INT32(3);
  int opt_l = PG_GETARG_INT32(4);
  // elog(WARNING, "k: %d", k);
  // elog(WARNING, "opt_k: %d", opt_k);
  // elog(WARNING, "opt_l: %d", opt_l);

  PG_RETURN_BOOL(evaluate_func(arrayContent1, arrayContent2, k, opt_k, opt_l));
}

Datum pg_evaluate(PG_FUNCTION_ARGS)
{
  VarChar *arg = (VarChar *)PG_GETARG_VARCHAR_P(0);
  // int len = PG_GETARG_INT32(1);
  int len = VARSIZE(arg) - VARHDRSZ;
  // elog(WARNING, "%d", len);
  char *str_arg = (char *)VARDATA(arg);
  char *s;
  s = (char *)malloc(sizeof(char) * len);
  copy(str_arg, str_arg + len, s);

  VarChar *arg2 = (VarChar *)PG_GETARG_VARCHAR_P(1);
  int len2 = VARSIZE(arg2) - VARHDRSZ;
  // elog(WARNING, "%d", len);
  char *str_arg2 = (char *)VARDATA(arg2);
  char *q_hash;
  q_hash = (char *)malloc(sizeof(char) * len2);
  copy(str_arg2, str_arg2 + len2, q_hash);

  int opt_k = PG_GETARG_INT32(2);

  int16 type_width;
  bool type_by_value;
  char type_alignment_code;
  Datum *arr_content;
  ArrayType *arr;

  int length = 1;

  // arr_content = (Datum *)palloc(sizeof(Datum) * length);

  set<int> result_set = query(s, q_hash, len, len2, opt_k);

  arr_content = (Datum *)palloc(sizeof(Datum) * result_set.size());
  int idxx = 0;

  set<int>::iterator it;
  for (it = result_set.begin(); it != result_set.end(); it++) {
    arr_content[idxx] = *it;
    idxx = idxx + 1;
  }

  get_typlenbyvalalign(INT2OID, &type_width, &type_by_value, &type_alignment_code);
  arr = construct_array(arr_content, result_set.size(), INT2OID, type_width, type_by_value, type_alignment_code);

  PG_RETURN_ARRAYTYPE_P(arr);
}

Datum pg_optimal_kl(PG_FUNCTION_ARGS)
{
  int k = PG_GETARG_INT32(0);
  int l = PG_GETARG_INT32(1);
  int x = PG_GETARG_INT32(2);
  int q = PG_GETARG_INT32(3);
  double t = PG_GETARG_FLOAT8(4);

  int opt_k = -1, opt_l = -1;

  optimal_kl(k, l, x, q, t, &opt_k, &opt_l);

  int16 type_width;
  bool type_by_value;
  char type_alignment_code;
  get_typlenbyvalalign(INT4OID, &type_width, &type_by_value, &type_alignment_code);

  // array to hold values for optK and optL
  Datum *arr_content;
  int length = 2;
  arr_content = (Datum *)palloc(sizeof(Datum) * length);
  arr_content[0] = opt_k;
  arr_content[1] = opt_l;

  ArrayType *arr;
  arr = construct_array(arr_content, length, INT4OID, type_width, type_by_value, type_alignment_code);

  PG_RETURN_ARRAYTYPE_P(arr);
}

// Datum pg_sig(PG_FUNCTION_ARGS)
// {

//   // construct signature array
//   int16 type_width;
//   bool type_by_value;
//   char type_alignment_code;

//   Datum *signature = palloc0(sizeof(Datum) * num_hash);
//   sig(signature, str, num_hash);

//   get_typlenbyvalalign(INT8OID, &type_width, &type_by_value, &type_alignment_code);
//   array = construct_array(signature, num_hash, INT8OID, type_width, type_by_value, type_alignment_code);

//   PG_RETURN_ARRAYTYPE_P(array);
// }