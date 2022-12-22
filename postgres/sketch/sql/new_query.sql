CREATE OR REPLACE FUNCTION sketch.query_lshe_col(_schema text, _tbl text, _col text, k integer, l integer, t float) RETURNS text[] AS $$
DECLARE
  d record;

  _part integer;
  -- params = [opt_k_part0, opt_l_part0, opt_k_part1, opt_l_part1, ...]
  params integer[] = '{}';
  q_hash int2[];
  q_hash_grouped text[] = '{}';
  q integer;
  _key text;
  opt_k integer;
  opt_l integer;
  opt_values integer[];

  l integer = 32;
  s_index integer;
  interval integer = 32;

  id_idxs integer[];
  id_idx integer;

  result text[] = '{}';

  -- TODO: change num_part
  -- num_part integer = 800;
  query_parts int[];


  lower_size integer;
  upper_size integer;
BEGIN
  -- compute hash keys and query size of the query domain
  SELECT "hashKey", size FROM sketch.hash_table WHERE "key" = _schema || '#sep#' || _tbl || '#sep#' || _col into q_hash, q;
  -- EXECUTE format('SELECT sketch.hash(STRING_AGG("%s", ''#sep#''), $1), COUNT(*) FROM (SELECT DISTINCT "%s" FROM %s."%s") t0', _col, _col, _schema, _tbl) into q_hash, q using k*l;

  IF q_hash IS NULL THEN
    RETURN result;
  END IF;

  lower_size = power(10, floor(log(q)));
  upper_size = power(10, floor(log(q))+1) - 1;

  -- find partition that's lower than 
  SELECT array_agg("part") FROM sketch.partition_table WHERE "lower" BETWEEN lower_size and upper_size INTO query_parts;

  FOR i IN 1..l LOOP
    s_index = (i-1) * interval + 1;
    q_hash_grouped[i] = array_to_string(q_hash[s_index:s_index + interval - 1], '');
  END LOOP;

  -- TODO: can store this?
  -- compute the params for each partition
  FOR d in SELECT * from sketch.partition_table LOOP
    _part = d."part";

    -- k;l;x;q;t
    _key = k::text || ';' || l::text || ';' || d."upper"::text || ';' || q::text || ';' || t::text;
    EXECUTE format('SELECT optk, optl from sketch.optkl_table WHERE key=''%s''', _key) INTO opt_k, opt_l;
    params = params || opt_k || opt_l;
    
    IF opt_k IS NULL THEN
      opt_values = sketch.optkl(k, l, d."upper", q, t);
      params[2*_part+1] = opt_values[1];
      params[2*_part+2] = opt_values[2];
			EXECUTE format('INSERT INTO sketch.optkl_table VALUES (''%s'', $1, $2)', _key) USING opt_values[1], opt_values[2];
    END IF;
  END LOOP;

  -- raise notice 'params is %', params;
  
  -- opt_k, opt_l

  raise notice 'query % partitions', array_length(query_parts, 1);

  -- TODO: change part start num
  -- loop through each hashed signature in the index table
  FOREACH _part IN ARRAY query_parts[1:250] LOOP
    -- raise notice 'k is % l is %', params[2*part+1], params[2*part+2];
    FOR i IN 1..params[2*_part+2] LOOP
      SELECT "hashkey_str", "sorted_key_array" FROM sketch.hash_group_table where "part-hash" = _part::text || '-' || i::text INTO d;

      id_idxs = sketch.lshe_evaluate(d."hashkey_str", substring(q_hash_grouped[i] from 1 for params[2*_part+1] * 8), params[2*_part+1]);

      FOREACH id_idx in ARRAY id_idxs LOOP
        result = result || d."sorted_key_array"[id_idx+1];
      END LOOP;
    END LOOP;
  END LOOP;

  RETURN result;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sketch.opt_params(k integer, l integer, q integer, t float) RETURNS integer[] AS $$
DECLARE
  opt_values integer[];
  params integer[] = '{}';
  d record;
  _part integer;
  _key text;
  opt_k integer;
  opt_l integer;
BEGIN
  FOR d in SELECT * from sketch.partition_table LOOP
    _part = d."part";

    -- k;l;x;q;t
    _key = k::text || ';' || l::text || ';' || d."upper"::text || ';' || q::text || ';' || t::text;
    EXECUTE format('SELECT optk, optl from sketch.optkl_table WHERE key=''%s''', _key) INTO opt_k, opt_l;
    params = params || opt_k || opt_l;
    
    IF opt_k IS NULL THEN
      opt_values = sketch.optkl(k, l, d."upper", q, t);
      params[2*_part+1] = opt_values[1];
      params[2*_part+2] = opt_values[2];
			EXECUTE format('INSERT INTO sketch.optkl_table VALUES (''%s'', $1, $2)', _key) USING opt_values[1], opt_values[2];
    END IF;
  END LOOP;

  RETURN params;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION sketch.query_partition(q_hash_grouped text[], _part integer, opt_k integer, opt_l integer) RETURNS text[] AS $$
DECLARE
  d record;
  id_idxs integer[];
  id_idx integer;
  result text[] = '{}';
BEGIN
  -- raise notice 'k is % l is %', params[2*part+1], params[2*part+2];
  FOR i IN 1..opt_l LOOP
    SELECT "hashkey_str", "sorted_key_array" FROM sketch.hash_group_table where "part-hash" = _part::text || '-' || i::text INTO d;

    id_idxs = sketch.lshe_evaluate(d."hashkey_str", substring(q_hash_grouped[i] from 1 for opt_k * 8), opt_k);

    FOREACH id_idx in ARRAY id_idxs LOOP
      result = result || d."sorted_key_array"[id_idx+1];
    END LOOP;
  END LOOP;

  RETURN result;
END;
$$ LANGUAGE plpgsql;