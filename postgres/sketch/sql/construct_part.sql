CREATE OR REPLACE FUNCTION sketch.corpus_partition(num_part integer) RETURNS void AS $$
DECLARE
	dep integer;
	curr_dep integer = 0;
	curr_part integer = 0;
	num_domain integer;
	
	_cursor cursor for SELECT size FROM sketch.hash_table ORDER BY size for update;
	d record;
	d_size integer;
BEGIN
	SELECT count(*) FROM sketch.hash_table INTO num_domain;
	dep = num_domain / num_part;
	DELETE FROM sketch.partition_table;

	OPEN _cursor;
	LOOP
		FETCH _cursor INTO d;
		IF found THEN
			d_size = d."size";
		END IF;
		exit when not found;

		UPDATE sketch.hash_table SET part = curr_part WHERE current of _cursor;
		curr_dep = curr_dep + 1;

		IF curr_dep >= dep AND curr_part < num_part-1 THEN
			execute 'INSERT INTO sketch.partition_table VALUES ($1, $2)' USING curr_part, d_size;
			curr_dep = 0;
			curr_part = curr_part + 1;
		END IF;
		
	END LOOP;
	CLOSE _cursor;
	EXECUTE 'INSERT INTO sketch.partition_table VALUES ($1, $2)' USING curr_part, d_size;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sketch.profile_partition(num_part integer) RETURNS void AS $$
DECLARE
	dep integer;
	curr_dep integer = 0;
	curr_part integer = 0;
	num_domain integer;
	
	_cursor cursor for SELECT size FROM sketch.profile_hash_table ORDER BY size for update;
	d record;
	d_size integer;
BEGIN
	SELECT count(*) FROM sketch.profile_hash_table INTO num_domain;
	dep = num_domain / num_part;
	DELETE FROM sketch.profile_partition_table;

	OPEN _cursor;
	LOOP
		FETCH _cursor INTO d;
		IF found THEN
			d_size = d."size";
		END IF;
		exit when not found;

		UPDATE sketch.profile_hash_table SET part = curr_part WHERE current of _cursor;
		curr_dep = curr_dep + 1;

		IF curr_dep >= dep AND curr_part < num_part-1 THEN
			execute 'INSERT INTO sketch.profile_partition_table VALUES ($1, $2)' USING curr_part, d_size;
			curr_dep = 0;
			curr_part = curr_part + 1;
		END IF;
		
	END LOOP;
	CLOSE _cursor;
	EXECUTE 'INSERT INTO sketch.profile_partition_table VALUES ($1, $2)' USING curr_part, d_size;
END;
$$ LANGUAGE plpgsql;


-- construct the hash table for a single partition
-- CREATE OR REPLACE FUNCTION corpus_construct_part_single(part_idx integer, _sigs bigint[][], ) RETURNS void AS $$
-- DECLARE
-- 	hs bigint[][] = -- TODO: initialize;
-- 	_sig bigint[];
-- BEGIN
-- 	foreach _sig in _sigs loop
-- 		FOR  _hk in sketch.hash_key(_sig) 
-- 			hs[i] = hs[i] || _hk;
-- 		end loop;
-- 	END LOOP;

-- 	foreach h in hs loop
-- 		execute 'INSERT INTO utils.partition$1 values ($2)' using part_idx, h;
-- 	end loop;
-- END;
-- $$ LANGUAGE plpgsql;