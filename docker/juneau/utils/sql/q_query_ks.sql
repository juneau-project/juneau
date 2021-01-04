/*
 * q_ks.sql
 * use two-sample KS test to look at the numerical distribution of
 * two columns and figure out a list of similar columns
 */

/*
 * compute the KS Unionability Score for two arrays of type bigint
 */
CREATE OR REPLACE FUNCTION compute_ks_score(a1 bigint[], a2 bigint[]) RETURNS float8
  AS '/home/juneau/utils/sql/funcs', 'computeKSInt'
  LANGUAGE c STRICT;

/*
 * compute the KS Unionability Score for two arrays of type double precision
 */
CREATE OR REPLACE FUNCTION compute_ks_score(a1 double precision[], a2 double precision[]) RETURNS float8
	AS '/home/juneau/utils/sql/funcs', 'computeKSFloat'
  LANGUAGE c STRICT;

/*
 * query over the schema specified by corpus_string
 * return a string array that includes similar tables of the format 'schema: xxx table_name: xxx column_name: xxx'
 * the column to query is specified by the schema_string, table_string, and domain_string
 * t_num is the similarity threshold
 */
CREATE OR REPLACE FUNCTION q_query_ks(schema_string varchar, table_string varchar, domain_string varchar, corpus_string varchar, t_num float8) RETURNS text[] AS $$
DECLARE
	s record;
	sa record;

	q_array bigint[];
	s_array bigint[];

	q_type varchar;

	similar_tables text[] = '{}';
BEGIN
	-- get the data type of the column (float or integer)
	select data_type into q_type from information_schema.columns where table_schema=schema_string and table_name=table_string and column_name=domain_string;
	-- store the numbers of that column into an array
	execute format('select array_agg(t."%s") from %s."%s" t', domain_string, schema_string, table_string) into q_array;

	FOR s IN
		select "table_name", "column_name" from information_schema.columns where table_schema=corpus_string and data_type=q_type
	LOOP
		execute format('select array_agg("%s") from %s."%s" where "%s" is not null', s."column_name", corpus_string, s."table_name", s."column_name") into s_array;
		IF compute_ks_score(q_array, s_array) >= t_num THEN
			similar_tables = similar_tables || format('schema: %s table_name: %s col_name: %s', corpus_string, s."table_name", s."column_name");
		END IF;
	END LOOP;
	RETURN similar_tables;
END;
$$ LANGUAGE plpgsql;