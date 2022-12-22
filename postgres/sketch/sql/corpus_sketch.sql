CREATE OR REPLACE FUNCTION sketch.corpus_sketch(_sch text, num_hash integer, num_digits integer, limit_num integer) RETURNS void AS $$
DECLARE
	d record;
BEGIN
	FOR d in
		-- exclude views + transpose tables
		-- LIMIT limit_num
		SELECT table_name FROM information_schema.tables WHERE table_schema = _sch AND table_type <> 'VIEW' EXCEPT (SELECT _tbl FROM sketch.sketched_table WHERE "_schema"=_sch) EXCEPT (SELECT _tbl from utils.transpose_table WHERE "_schema"=_sch) LIMIT limit_num
	LOOP
		perform sketch.corpus_sketch_single(_sch, d."table_name"::text, num_hash, num_digits);
	END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sketch.corpus_sketch_single(_schema text, _tbl text, num_hash integer, num_digits integer) RETURNS void AS $$
DECLARE
	d record;
	_count integer;
	_d_count integer;

	_str text;
	arr_int bigint[];
	arr_float double precision[];
	_col_name text;
BEGIN
	execute format('SELECT count(*) FROM %s."%s"', _schema, _tbl) into _count;

	-- add empty tables to sketched_table directly and return
	IF _count = 0 THEN
		INSERT INTO sketch.sketched_table VALUES (_schema, _tbl);
		RETURN;
	END IF;

	FOR d IN
 		SELECT column_name, data_type FROM information_schema.columns WHERE table_name = _tbl AND table_schema = _schema
	LOOP
    -- ignore #self and #par
    CONTINUE WHEN d."column_name" = '#self' OR d."column_name" = '#par';
		SELECT REPLACE(d."column_name", '"', '""') INTO _col_name;

    IF d."data_type" = 'text' THEN
      -- take distinct values only
      EXECUTE format('SELECT STRING_AGG("%s", ''#sep#''), COUNT(*) FROM (SELECT DISTINCT "%s" FROM %s."%s") t0', _col_name, _col_name, _schema, _tbl) into _str, _d_count;

      -- handle string_agg = null
      IF _str IS NOT NULL THEN
        INSERT INTO sketch.hash_table VALUES (_schema || '#sep#' || _tbl || '#sep#' || _col_name, _d_count, sketch.hash(_str, num_hash), NULL);
      END IF;
    ELSIF d."data_type" = 'bigint' THEN
      EXECUTE format('SELECT ARRAY_AGG("%s") FROM %s."%s" WHERE "%s" IS NOT NULL', _col_name, _schema, _tbl, _col_name) INTO arr_int;

      IF arr_int IS NOT NULL THEN
				INSERT INTO sketch.hist_int_table VALUES (_schema || '#sep#' || _tbl || '#sep#' || _col_name, sketch.hist_int(arr_int, num_digits));
      END IF;
		ELSIF d."data_type" = 'double precision' THEN
			EXECUTE format('SELECT ARRAY_AGG("%s") FROM %s."%s" WHERE "%s" IS NOT NULL', _col_name, _schema, _tbl, _col_name) INTO arr_float;

			IF arr_float IS NOT NULL THEN
				INSERT INTO sketch.hist_float_table VALUES (_schema || '#sep#' || _tbl || '#sep#' || _col_name, sketch.hist_float(arr_float, num_digits));
			END IF;
    END IF;
  END LOOP;

	INSERT INTO sketch.sketched_table VALUES (_schema, _tbl);
END;
$$ LANGUAGE plpgsql;