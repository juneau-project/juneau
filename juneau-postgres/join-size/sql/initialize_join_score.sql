SET search_path = utils;
DROP TABLE IF EXISTS join_score_table;
DROP TABLE IF EXISTS join_score_table2;

CREATE TABLE join_score_table(
  "key" text,
  "score" double precision
);

CREATE TABLE join_score_table2(
  "key" text,
  "score" double precision
);

CREATE OR REPLACE FUNCTION c_join_score_str(s1 text, e_num1 integer, s2 text, e_num2 integer) RETURNS double precision
  AS '/juneau_funcs/join-size/c/join_score', 'pg_join_score_str'
  LANGUAGE C;

CREATE OR REPLACE FUNCTION c_join_score_int(arr1 bigint[], arr2 bigint[]) RETURNS double precision
  AS '/juneau_funcs/join-size/c/join_score', 'pg_join_score_int'
  LANGUAGE C;