CREATE OR REPLACE FUNCTION sketch.query_lshe_col(_schema text, _tbl text, _col text, k integer, l integer, t float) RETURNS text[] AS $$
DECLARE
  d record;

  part integer;
  -- params = [opt_k_part0, opt_l_part0, opt_k_part1, opt_l_part1, ...]
  params integer[] = '{}';
  q_hash int2[];
  q integer;
  _key text;
  opt_k integer;
  opt_l integer;
  opt_values integer[];

  result text[] = '{}';
BEGIN
  -- compute hash keys and query size of the query domain
  SELECT "hashKey", size FROM sketch.hash_table WHERE "key" = _schema || '#sep#' || _tbl || '#sep#' || _col || '#sep#' into q_hash, q;
  -- EXECUTE format('SELECT sketch.hash(STRING_AGG("%s", ''#sep#''), $1), COUNT(*) FROM (SELECT DISTINCT "%s" FROM %s."%s") t0', _col, _col, _schema, _tbl) into q_hash, q using k*l;

  IF q_hash IS NULL THEN
    RETURN result;
  END IF;

  -- compute the params for each partition
  FOR d in SELECT * from sketch.partition_table LOOP
    part = d."part";

    -- k;l;x;q;t
    _key = k::text || ';' || l::text || ';' || d."upper"::text || ';' || q::text || ';' || t::text;
    EXECUTE format('SELECT optk, optl from sketch.optkl_table WHERE key=''%s''', _key) INTO opt_k, opt_l;
    params = params || opt_k || opt_l;
    
    IF opt_k IS NULL THEN
      opt_values = sketch.optkl(k, l, d."upper", q, t);
      params[2*part+1] = opt_values[1];
      params[2*part+2] = opt_values[2];
			EXECUTE format('INSERT INTO sketch.optkl_table VALUES (''%s'', $1, $2)', _key) USING opt_values[1], opt_values[2];
    END IF;
  END LOOP;

  -- raise notice 'params is %', params;

  -- loop through each hashed signature in the index table
  FOR d in SELECT * FROM sketch.hash_table LOOP
    -- raise notice 'hash key for % is %', d.key, d."hashKey";
    -- raise notice 'hash key for query is %', q_hash;
    IF sketch.lshe_evaluate(d."hashKey", q_hash, k, params[2*part+1], params[2*part+2]) THEN
      result = result || d.key;
    END IF;
  END LOOP;

  RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sketch.query_lshe_profile(string text, k integer, l integer, q integer, t float) RETURNS text[] AS $$
DECLARE
  d record;

  part integer;
  params integer[] = '{}';
  q_hash int2[];
  _key text;
  opt_k integer;
  opt_l integer;
  opt_values integer[];

  result text[] = '{}';
BEGIN
  -- compute the params for each partition
  FOR d in SELECT * from sketch.profile_partition_table LOOP
    part = d."part";

    -- k;l;x;q;t
    _key = k::text || ';' || l::text || ';' || d."upper"::text || ';' || q::text || ';' || t::text;
    EXECUTE format('SELECT optk, optl from sketch.optkl_table WHERE key=''%s''', _key) INTO opt_k, opt_l;
    params = params || opt_k || opt_l;
    
    IF opt_k IS NULL THEN
      opt_values = sketch.optkl(k, l, d."upper", q, t);
      params[2*part+1] = opt_values[1];
      params[2*part+2] = opt_values[2];
			EXECUTE format('INSERT INTO sketch.optkl_table VALUES (''%s'', $1, $2)', _key) USING opt_values[1], opt_values[2];
    END IF;
  END LOOP;

  -- compute hash keys of the query domain
  q_hash = sketch.hash(string, k*l);

  -- loop through each hashed signature in the index table
  FOR d in SELECT * FROM sketch.profile_hash_table LOOP
    -- raise notice 'hash key for % is %', d.key, d."hashKey";
    -- raise notice 'hash key for query is %', q_hash;
    IF sketch.lshe_evaluate(d."hashKey", q_hash, k, params[2*part+1], params[2*part+2]) THEN
      result = result || d.key;
    END IF;
  END LOOP;

  RETURN result;
  -- RETURN '{p1, p2}';
END;
$$ LANGUAGE plpgsql;