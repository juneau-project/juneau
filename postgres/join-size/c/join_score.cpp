extern "C"
{
#include "postgres.h"
#include "fmgr.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include <catalog/pg_type.h>

  PG_MODULE_MAGIC;

  PG_FUNCTION_INFO_V1(pg_join_score_str);
  PG_FUNCTION_INFO_V1(pg_join_score_int);
}

#include "score.h"

using namespace std;

Datum pg_join_score_str(PG_FUNCTION_ARGS)
{
  VarChar *arg1 = (VarChar *)PG_GETARG_VARCHAR_P(0);
  int len1 = VARSIZE(arg1) - VARHDRSZ;
  char *str_arg1 = (char *)VARDATA(arg1);
  // char *str1;
  // str1 = (char *)malloc(sizeof(char) * len1);
  // copy(str_arg1, str_arg1 + len1, str1);
  int e_num1 = PG_GETARG_INT32(1);

  VarChar *arg2 = (VarChar *)PG_GETARG_VARCHAR_P(2);
  int len2 = VARSIZE(arg2) - VARHDRSZ;
  char *str_arg2 = (char *)VARDATA(arg2);
  int e_num2 = PG_GETARG_INT32(3);
  // char *str2;
  // str2 = (char *)malloc(sizeof(char) * len2);
  // copy(str_arg2, str_arg2 + len2, str2);

  PG_RETURN_FLOAT8(score_str(str_arg1, len1, e_num1, str_arg2, len2, e_num2));
}

Datum pg_join_score_int(PG_FUNCTION_ARGS)
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
 
  PG_RETURN_FLOAT8(score_int(arr_content1, len1, arr_content2, len2));
}