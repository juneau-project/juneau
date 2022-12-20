/*
 * construct_sig.sql
 * compute minHash values from the raw table and
 * insert the signature vectors of each domain into the signature table
 */

/*
 * constructs the signature matrix of a single table specified by the input schema and table_name
 * schema_string is the name of the schema where the table is stored
 * table_string is the name of the table
 * num_hash is the number of hash functions used
 * corpus_sig_string is the name of the table that stores the signature matrices
 */
CREATE OR REPLACE FUNCTION corpus_construct_sig_single(schema_string varchar, table_string varchar, num_hash integer, corpus_sig_string varchar) RETURNS void AS $$
DECLARE
	d record;
	text_domain_count integer;
	created boolean = FALSE;
	indexed boolean;

 	min_hv bigint;
	min_json jsonb;
	domain_idx integer = 0;
	row_count integer;
BEGIN
	FOR d IN
 		SELECT column_name FROM information_schema.columns WHERE table_schema=schema_string and table_name=table_string and (data_type='character varying' or data_type='text')
	LOOP
		-- create the signature matrix
		IF NOT created THEN
			execute format('CREATE TABLE %s."%s_%s" ("hashIdx" integer)', corpus_sig_string, schema_string, table_string);

			FOR i in 0..num_hash-1 LOOP
				execute format('INSERT INTO %s."%s_%s" VALUES ($1)', corpus_sig_string, schema_string, table_string) using i;
			END LOOP;

			created = TRUE;
		END IF;

		execute format('ALTER TABLE %s."%s_%s" ADD d%s jsonb', corpus_sig_string, schema_string, table_string, domain_idx);
		FOR i IN 0..num_hash-1 LOOP
			execute format('select MIN(min_hash_char(t."%s", LENGTH(t."%s"), $1)) from %s."%s" t', d."column_name", d."column_name", schema_string, table_string) into min_hv using i;
			min_json = json_build_object('hv', min_hv, 'key', format('schema: %s table_name: %s col_name: %s', schema_string, table_string, d."column_name"));

			execute format('UPDATE %s."%s_%s" SET d%s=$1 WHERE "hashIdx"=$2', corpus_sig_string, schema_string, table_string, domain_idx) using min_json, i;
		END LOOP;
		domain_idx = domain_idx + 1;
	END LOOP;

	IF NOT created THEN
		execute 'INSERT INTO utils.indexed_table VALUES ($1, $2)' using schema_string, table_string;
	ELSE
		execute format('SELECT count(*) FROM %s."%s"', schema_string, table_string) into row_count;
		execute 'INSERT INTO utils.count_table VALUES ($1, $2, $3, $4)' using schema_string, table_string, row_count, domain_idx;
	END IF;
END;
$$ LANGUAGE plpgsql;


/*
 * constructs the signature matrices of all tables in the input schema
 * num_hash is the number of hash functions used
 * corpus_sig_string is the name of the schema that stores the signature matrices
 */
CREATE OR REPLACE FUNCTION corpus_construct_sig(schema_string varchar, num_hash integer, corpus_sig_string varchar) RETURNS void AS $$
DECLARE
	rv record;
BEGIN
	DELETE FROM utils.count_table;
	FOR rv in
		SELECT * from (SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = schema_string) it
		WHERE NOT EXISTS (SELECT FROM utils.indexed_table ui WHERE ui.table_string=it.table_name and ui.schema_string=it.table_schema)
	LOOP
		perform corpus_construct_sig_single(schema_string, rv."table_name"::varchar, num_hash, corpus_sig_string);
	END LOOP;
END;
$$ LANGUAGE plpgsql;