/*
 * q_construct_sig.sql
 * constructs the signature matrix of a query domain
 */

/*
 * from the column/ domain specified by the schema_string, table_string, and the domain_string
 * construct the corresponding signature vector
 *
 * num_hash specifies the number of hash functions used
 * q_sig_string specifies the schema to store the signature vector
 */
CREATE OR REPLACE FUNCTION q_construct_sig(schema_string varchar, table_string varchar, domain_string varchar, num_hash integer, q_sig_string varchar) RETURNS void AS $$
DECLARE
 	min_hv bigint;
BEGIN
	execute format('CREATE TABLE IF NOT EXISTS %s."%s_%s" ("hashIdx" integer, hv bigint)', q_sig_string, table_string, domain_string);
	FOR i IN 0..num_hash-1 LOOP
		execute format('select MIN(min_hash_char(t."%s", LENGTH(t."%s"), $1)) from %s.%s t', domain_string, domain_string, schema_string, table_string) into min_hv using i;
		execute format('INSERT INTO %s."%s_%s" VALUES ($1, $2)', q_sig_string, table_string, domain_string) using i, min_hv;
	END LOOP;
END;
$$ LANGUAGE plpgsql;