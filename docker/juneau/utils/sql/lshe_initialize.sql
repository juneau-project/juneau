DROP FUNCTION format_json;
DROP FUNCTION corpus_construct_hash_single;
DROP FUNCTION corpus_construct_hash;
DROP FUNCTION corpus_construct_sig_single;
DROP FUNCTION corpus_construct_sig;
DROP FUNCTION min_hash_char;
DROP FUNCTION min_hash_array;
DROP FUNCTION q_construct_hash;
DROP FUNCTION q_construct_sig;

CREATE SCHEMA IF NOT EXISTS utils;
CREATE SCHEMA IF NOT EXISTS sig;
CREATE SCHEMA IF NOT EXISTS hash;
CREATE SCHEMA IF NOT EXISTS q_table;
CREATE SCHEMA IF NOT EXISTS q_sig;
CREATE SCHEMA IF NOT EXISTS q_hash;

CREATE TABLE IF NOT EXISTS utils.count_table (
	"schema_string" varchar,
	"table_string" varchar,
	"row_count" integer,
	"col_count" integer
);

-- this table stores tables that are ALREADY indexed
CREATE TABLE IF NOT EXISTS utils.indexed_table (
	"schema_string" varchar,
	"table_string" varchar
);

CREATE TABLE IF NOT EXISTS utils.optkl_table (
	"maxK" integer,
	"numHash" integer,
	"x" integer,
	"q" integer,
	"t" double precision,
	"optK" integer,
	"optL" integer
);

CREATE TABLE IF NOT EXISTS utils.partition_table (
	"part_idx" integer,
	"upper" integer,
	"num_domain" integer
);