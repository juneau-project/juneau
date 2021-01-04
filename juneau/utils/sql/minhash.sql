/*
 * minhash.sql
 */



/*
 * returns the hash value of a character varying object using the minHash function
 * specified by hashIdx
 * 1st argument: the character varying object that will be hashed
 * 2nd argument: the length of the var char object
 * 3rd agrument: index of the minHash function to be used
 */
CREATE OR REPLACE FUNCTION min_hash_char(character varying, integer, integer) RETURNS bigint
  AS '/home/juneau/utils/sql/funcs', 'min_hash_char'
  LANGUAGE c STRICT;

/*
 * computes the hash keys given the input array of integers
 */
CREATE OR REPLACE FUNCTION min_hash_array(bigint[]) RETURNS bigint[]
	AS '/home/juneau/utils/sql/funcs', 'min_hash_array_new'
	LANGUAGE c STRICT;