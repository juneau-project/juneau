/*
 * q_construct_hash.sql
 */

/*
 * construct the hash table from the signature matrix of the domain
 * specified by schema_string, table_string and the domain_string
 *
 * max_k is the number of hash functions per band
 * max_l is the number of bands
 * q_sig_string is the schema where the signature matrix is stored
 * q_hash_string is the schema where the hash table will be stored
 */
CREATE OR REPLACE FUNCTION q_construct_hash(schema_string varchar, table_string varchar, domain_string varchar, max_k integer, max_l integer, q_sig_string varchar, q_hash_string varchar) RETURNS void AS $$
DECLARE
	sig_vector bigint[];
	rv RECORD;
BEGIN
	execute format('CREATE TABLE IF NOT EXISTS %s."%s_%s" ("bandIdx" integer, "hashKey" bigint[])', q_hash_string, table_string, domain_string);

	FOR i IN 0..max_l-1 LOOP
		FOR rv in
			execute format('SELECT "hv" FROM %s."%s_%s" t where t."hashIdx" >= $1 AND t."hashIdx" < $2', q_sig_string, table_string, domain_string) using i*max_k, (i+1)*max_k
		LOOP
			sig_vector = array_append(sig_vector, rv."hv");
		END LOOP;

		execute format('INSERT INTO %s."%s_%s" VALUES ($1, $2)', q_hash_string, table_string, domain_string) using i, min_hash_array(sig_vector);
		
		sig_vector = '{}';
	END LOOP;
END;
$$ LANGUAGE plpgsql;