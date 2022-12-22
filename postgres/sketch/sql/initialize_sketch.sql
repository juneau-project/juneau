CREATE SCHEMA IF NOT EXISTS sketch;

SET search_path=sketch;
DROP TABLE IF EXISTS sketched_table;
-- lshe
DROP TABLE IF EXISTS hash_table;
DROP TABLE IF EXISTS partition_table;
DROP TABLE IF EXISTS optkl_table;
DROP TABLE IF EXISTS hist_int_table;
DROP TABLE IF EXISTS hist_float_table;
DROP TABLE IF EXISTS profile_hash_table;
DROP TABLE IF EXISTS profile_partition_table;
DROP TABLE IF EXISTS profile_hist_int_table;
DROP TABLE IF EXISTS profile_hist_float_table;
-- transpose
-- DROP TABLE IF EXISTS utils.transpose_table;

-----------
-- create tables
CREATE TABLE sketched_table(
  "_schema" text,
  "_tbl" text
);

-- corpus
CREATE TABLE hash_table(
  "key" text,
  size integer,
  "hashKey" int2[],
  part integer
);

CREATE TABLE hash_table2(
  "key" text,
  size integer,
  "hashKey" int2[],
  part integer
);

CREATE TABLE partition_table(
  part integer,
  "upper" integer
);

CREATE TABLE optkl_table(
  "key" text,
  optk integer,
	optl integer
);

CREATE TABLE hist_int_table(
  "key" text,
  "hist" bigint[]
);

CREATE TABLE hist_float_table(
  "key" text,
  "hist" bigint[]
);

CREATE TABLE hist_int_table2(
  "key" text,
  "hist" bigint[]
);

CREATE TABLE hist_float_table2(
  "key" text,
  "hist" bigint[]
);

-- profiles
CREATE TABLE profile_hash_table(
  "key" text primary key,
  size integer,
  "hashKey" int2[],
  part integer
);

CREATE TABLE profile_partition_table(
  part integer,
  "upper" integer
);

CREATE TABLE profile_hist_int_table(
  "key" text primary key,
  "hist" bigint[]
);

CREATE TABLE profile_hist_float_table(
  "key" text primary key,
  "hist" bigint[]
);

-- transpose
CREATE TABLE utils.transpose_table(
  _schema text,
  _tbl text,
  _key text,
  _val text,
  _hashed boolean
);

-- drop C functions
-- lshe
DROP FUNCTION IF EXISTS hash;
DROP FUNCTION IF EXISTS lshe_evaluate;
DROP FUNCTION IF EXISTS optkl;

-- ks
DROP FUNCTION IF EXISTS hist;
DROP FUNCTION IF EXISTS ks_evaluate;

---------------------------
-- create C functions
-- lshe
CREATE OR REPLACE FUNCTION hash(s text, numHash integer) RETURNS int2[]
  AS '/juneau_funcs/sketch/c/lshe/lshe', 'pg_hash'
  LANGUAGE C;

CREATE OR REPLACE FUNCTION lshe_evaluate(c_hash int2[], q_hash int2[], k integer, opt_k integer, opt_l integer) RETURNS boolean
  AS '/juneau_funcs/sketch/c/lshe/lshe', 'pg_evaluate'
  LANGUAGE C;

CREATE OR REPLACE FUNCTION optkl(k integer, l integer, x integer, q integer, t float8) RETURNS int2[]
  AS '/juneau_funcs/sketch/c/lshe/lshe', 'pg_optimal_kl'
  LANGUAGE C;

-- ks
CREATE OR REPLACE FUNCTION hist_int(arr bigint[], num_digits int) RETURNS bigint[]
  AS '/juneau_funcs/sketch/c/ks/ks', 'pg_hist_int'
  LANGUAGE C;

CREATE OR REPLACE FUNCTION hist_float(arr double precision[], num_digits int) RETURNS bigint[]
  AS '/juneau_funcs/sketch/c/ks/ks', 'pg_hist_float'
  LANGUAGE C;

-- CREATE OR REPLACE FUNCTION hist_int(arr bigint[], num_digits int) RETURNS bigint[]
--   AS '/home/ubuntu/projects/notebook_data_extension/postgres/sketch/c/ks/ks', 'pg_hist_int'
--   LANGUAGE C;
  
-- CREATE OR REPLACE FUNCTION hist_float(arr double precision[], num_digits int) RETURNS bigint[]
--   AS '/home/ubuntu/projects/notebook_data_extension/postgres/sketch/c/ks/ks', 'pg_hist_float'
--   LANGUAGE C;

-- CREATE OR REPLACE FUNCTION hist(arr text, arr_len int, num_digits int, _type int) RETURNS bigint[]
--   AS '/Library/PostgreSQL/12/include/postgresql/server/ks', 'pg_hist'
--   LANGUAGE C;

CREATE OR REPLACE FUNCTION ks_evaluate(h1 bigint[], h2 bigint[], t float) RETURNS boolean
  AS '/juneau_funcs/sketch/c/ks/ks', 'pg_ks_evaluate'
  LANGUAGE C;

-- drop plpgsql functions
DROP FUNCTION IF EXISTS corpus_sketch;
DROP FUNCTION IF EXISTS corpus_sketch_single;

DROP FUNCTION IF EXISTS corpus_partition;
DROP FUNCTION IF EXISTS profile_partition;

DROP FUNCTION IF EXISTS corpus_transpose;

DROP FUNCTION IF EXISTS query_lshe_col;
DROP FUNCTION IF EXISTS query_lshe_profile;

DROP FUNCTION IF EXISTS query_ks_col;
DROP FUNCTION IF EXISTS query_ks_profile(arr bigint[], t float);
DROP FUNCTION IF EXISTS query_ks_profile(arr double precision[], t float);

