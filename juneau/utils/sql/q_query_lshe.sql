/*
 * q_query_lshe.sql
 */

/*
 * compute the optimum valuse for k and l
 */
CREATE OR replace FUNCTION compute_optkl(integer, integer, integer, integer, float8) RETURNS bigint[]
	AS '/juneau_funcs/lshe', 'computeOptimalKL'
	LANGUAGE c;

/*
 * check whether two hash keys are the same
 * hqKeys are the hash keys of the query domain
 * hKeys are the hash keys of the search domain
 */
CREATE OR REPLACE FUNCTION check_hash_keys(hqKeys bigint[], hKeys bigint[], prefixSize integer) RETURNS boolean AS $$
BEGIN
	-- array is 1-indexed in PostgresSQL
	FOR i IN 1..prefixSize LOOP
		IF hqKeys[i] != hKeys[i] THEN
			RETURN false;
		END IF;
	END LOOP;
	return true;
END;
$$ LANGUAGE plpgsql;

/*
 * return a string array that includes similar tables of the format
 * `schema: xxx table_name: xxx column_name: xxx`
 *
 * max_k is the number of hash functions per band
 * num_hash is the total number of hash functions used
 * t_num is the similarity threshold
 * corpus_hash_string is the schema where the corpus tables
 * q_hash_string is the schema of the query hash table
 */
CREATE OR REPLACE FUNCTION q_query_lshe(max_k integer, num_hash integer, t_num float8, schema_string varchar, table_string varchar, domain_string varchar, corpus_hash_string varchar, q_hash_string varchar) RETURNS text[] AS $$
DECLARE
	opt_k integer = max_k;
	opt_l integer;
	hashValueSize integer = 4;
	prefixSize integer;
	opt_values integer[];
	x_num integer;
	q_num integer;

	similar_tables text[] = '{}';
	rv RECORD;
	part RECORD;
BEGIN
	execute format('select count(*) from %s.%s', schema_string, table_string) into q_num;

	-- loop through all partitions
	FOR part IN
		SELECT * FROM utils.partition_table
	LOOP
		-- compute opt_l and prefixSize
		x_num = part."upper";
		execute 'SELECT "optL" from utils.optkl_table WHERE "maxK"=$1 AND "numHash"=$2 AND x=$3 AND q=$4 AND t=$5'
			into opt_l using max_k, num_hash, x_num, q_num, t_num;
		IF opt_l is null THEN
			opt_values = compute_optkl(max_k, num_hash, x_num, q_num, t_num);
			opt_l = opt_values[2];
			execute 'INSERT INTO utils.optkl_table VALUES ($1, $2, $3, $4, $5, $6, $7)'
				using max_k, num_hash, x_num, q_num, t_num, opt_k, opt_l;
		END IF;
		prefixSize = hashValueSize * opt_k;

		-- loop through each domain in the partition
		FOR domain_idx in 0..part."num_domain"-1 LOOP
			FOR rv in
				execute format('select h."d_hk", hq."q_hk" from (select h1."d%s" as "d_hk", h1."bandIdx" from %s.partition%s h1) h
				JOIN (select h2."hashKey" as "q_hk", h2."bandIdx" from %s."%s_%s" h2) hq ON h."bandIdx"=hq."bandIdx" LIMIT $1',
				domain_idx, corpus_hash_string, part."part_idx", q_hash_string, table_string, domain_string) using opt_l
			LOOP
				IF check_hash_keys(rv."q_hk"::bigint[], (rv."d_hk"->>'hashKey')::bigint[], prefixSize) THEN
					-- insert into result table
					similar_tables = similar_tables || (rv."d_hk"->>'key');
				END IF;
			END LOOP;
		END LOOP;
	END LOOP;

	RETURN similar_tables;
END;
$$ LANGUAGE plpgsql;