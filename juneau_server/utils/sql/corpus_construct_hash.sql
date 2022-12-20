/*
 * construct_hash.sql
 * group the signature vectors according to the number of bands
 * and insert the hash keys into the hash table
 */

/*
 * a helper method for constructing a JSON object consisting of the hashKey and the key (i.e. unique ID of a domain)
 */
CREATE OR REPLACE FUNCTION format_json(hash_vector bigint[], key_string varchar) RETURNS jsonb AS $$
DECLARE
	array_string varchar;
	domain_record jsonb;
BEGIN
	array_string = concat('{', array_to_string(hash_vector, ','), '}');
	domain_record = json_build_object('hashKey', array_string, 'key', key_string);
	RETURN domain_record;
END;
$$ LANGUAGE plpgsql;


/*
 * convert the signature vector a single domain and insert it into the hash table
 * schema_string and table_string specifies the signature matrix to look at
 * domain_idx specifies the domain in the signature matrix that needs to be converted
 * max_k is the number of hash functions per band
 * max_l is the number of bands
 * part_idx speficies the partition to insert the hash values into
 * part_domain_idx specifies which domain to insert in the partition table
 */
CREATE OR REPLACE FUNCTION corpus_construct_hash_single(corpus_sig_string varchar, corpus_hash_string varchar, schema_string varchar, table_string varchar, domain_idx integer, max_k integer, max_l integer, part_idx integer, part_domain_idx integer) RETURNS void AS $$
DECLARE
	domain_id varchar;
	sig_vector bigint[];

	rv RECORD;
BEGIN
	FOR i IN 0..max_l-1 LOOP
		FOR rv in
			execute format('SELECT t."d%s" as d from %s."%s_%s" t where t."hashIdx" >= $1 AND t."hashIdx" < $2', domain_idx, corpus_sig_string, schema_string, table_string) using i*max_k, (i+1)*max_k
		LOOP
			domain_id = (rv."d"->>'key')::varchar;
			sig_vector = array_append(sig_vector, (rv."d"->>'hv')::bigint);
		END LOOP;

		execute format('UPDATE %s.partition%s SET d%s=$1 WHERE "bandIdx"=$2', corpus_hash_string, part_idx, part_domain_idx)
			using format_json(min_hash_array(sig_vector), domain_id), i;

		sig_vector = '{}';
	END LOOP;
END;
$$ LANGUAGE plpgsql;


/*
 * constructs the hash table from the signature tables
 * max_k is the number of hash functions per band
 * max_l is the number of bands
 * num_part is the number of partitions
 */
CREATE OR REPLACE FUNCTION corpus_construct_hash(corpus_sig_string varchar, corpus_hash_string varchar, max_k integer, max_l integer, num_part integer) RETURNS void AS $$
DECLARE
	dep integer;
	curr_dep integer = 0;
	curr_part integer = 0;
	curr_size integer;
	num_domain integer;
	start_part integer = 0; -- the index of the partition to start from

	d record;
BEGIN
	execute 'SELECT MAX(part_idx) from utils.partition_table' into start_part;
	IF start_part is null THEN
		start_part = 0;
	ELSE
		start_part = start_part + 1;
		curr_part = start_part;
	END IF;

	FOR i in start_part..start_part+num_part-1 LOOP
		execute format('CREATE TABLE IF NOT EXISTS %s.partition%s ("bandIdx" integer)', corpus_hash_string, i);
		FOR j in 0..max_l-1 LOOP
			execute format('INSERT INTO %s.partition%s VALUES ($1)', corpus_hash_string, i) using j;
		END LOOP;
	END LOOP;

	SELECT SUM("col_count") from utils.count_table into num_domain;
	dep = num_domain / num_part;

	FOR d in
		SELECT * FROM utils.count_table ORDER BY "row_count"
	LOOP
		FOR domain_idx IN 0..d."col_count"-1 LOOP
			IF curr_dep >= dep and curr_part < start_part+num_part-1 THEN
				execute 'INSERT INTO utils.partition_table VALUES ($1, $2, $3)' using curr_part, curr_size, curr_dep;
				curr_dep = 0;
				curr_part = curr_part + 1;
			END IF;
			execute format('ALTER TABLE %s.partition%s ADD d%s jsonb', corpus_hash_string, curr_part, curr_dep);

			perform corpus_construct_hash_single(corpus_sig_string, corpus_hash_string, d."schema_string"::varchar, d."table_string"::varchar, domain_idx, max_k, max_l, curr_part, curr_dep);
			curr_dep = curr_dep + 1;
			curr_size = d."row_count";
		END LOOP;

		execute 'INSERT INTO utils.indexed_table VALUES ($1, $2)' using d."schema_string"::varchar, d."table_string"::varchar;
	END LOOP;
	execute 'INSERT INTO utils.partition_table VALUES ($1, $2, $3)' using curr_part, curr_size, curr_dep;
END;
$$ LANGUAGE plpgsql;