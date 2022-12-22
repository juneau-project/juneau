CREATE OR REPLACE FUNCTION sketch.corpus_transpose(num_hash integer, num_digits integer) RETURNS void AS $$
DECLARE
	cd record;
  _type text;
  _cursor cursor for SELECT * FROM utils.transpose_table WHERE _hashed = false for update;
  index_key text;
  
  _count integer;
  d record;
BEGIN
  OPEN _cursor;
  LOOP
		FETCH _cursor INTO cd;
		exit when not found;

    SELECT data_type FROM information_schema.columns WHERE table_name = cd._tbl AND table_schema = cd._schema AND column_name = cd._val INTO _type;

    EXECUTE format('SELECT count(*) FROM %s."%s"', cd._schema, cd._tbl) into _count;

    IF _count != 0 THEN
      IF _type = 'text' THEN
        FOR d IN
          execute format('SELECT "%s" as "key", STRING_AGG(DISTINCT "%s", ''#sep#'') AS "val", COUNT(DISTINCT "%s") AS "_count" FROM %s."%s" GROUP BY "%s"', cd._key, cd._val, cd._val, cd._schema, cd._tbl, cd._key)
        LOOP
          index_key = cd._schema || '#sep#' || cd._tbl || '#sep#' || d.key || '#sep#transposefkyou';

          IF d."val" IS NOT NULL THEN
            INSERT INTO sketch.hash_table VALUES (index_key, d."_count", sketch.hash(d."val", num_hash), NULL);
          END IF;
        END LOOP;
      ELSIF _type = 'bigint' THEN
        FOR d IN
          EXECUTE format('SELECT "%s" as "key", ARRAY_AGG("%s") AS "val" FROM %s."%s" WHERE "%s" IS NOT NULL GROUP BY "%s"', cd._key, cd._val, cd._schema, cd._tbl, cd._val, cd._key)
        LOOP
          index_key = cd._schema || '#sep#' || cd._tbl || '#sep#' || d.key || '#sep#transposefkyou';

          IF d."val" IS NOT NULL THEN
            INSERT INTO sketch.hist_int_table VALUES (index_key, sketch.hist_int(d."val", num_digits));
          END IF;
        END LOOP;
      ELSIF _type = 'double precision' THEN
        FOR d IN
          EXECUTE format('SELECT "%s" as "key", ARRAY_AGG("%s") AS "val" FROM %s."%s" WHERE "%s" IS NOT NULL GROUP BY "%s"', cd._key, cd._val, cd._schema, cd._tbl, cd._val, cd._key)
        LOOP
          index_key = cd._schema || '#sep#' || cd._tbl || '#sep#' || d.key || '#sep#transposefkyou';

          IF d."val" IS NOT NULL THEN
            INSERT INTO sketch.hist_float_table VALUES (index_key, sketch.hist_float(d."val", num_digits));
          END IF;
        END LOOP;
      END IF;
    END IF;

    UPDATE utils.transpose_table SET _hashed = true WHERE current of _cursor;
	END LOOP;
	CLOSE _cursor;
END;
$$ LANGUAGE plpgsql;