CREATE OR REPLACE FUNCTION sketch.query_lshe_col(_schema text, _tbl text, _col text, k integer, l integer, t float) RETURNS text[] AS $$
DECLARE
  d record;

  _part integer;
  q_hash_grouped text[];
  q integer;
  _key text;

  opt_k integer;
  opt_l integer;
  opt_values integer[];

  i integer;
  j integer;

  optk_ls integer[];
  optl_ls integer[];

  l integer = 32;
  s_index integer;
  interval integer = 32;

  id_idxs integer[];
  id_idx integer;

  result text[] = '{}';

  -- TODO: change num_part
  -- num_part integer = 800;
  query_parts int[];
  upper_ls int[];
  d_upper_ls int[];

  optk_dict jsonb = '{}';
  optl_dict jsonb = '{}';

  _keys_str text = '';

  lower_size integer;
  upper_size integer;

  result_count integer = 0;
BEGIN
  -- compute hash keys and query size of the query domain
  SELECT "hashkey_grouped", size FROM sketch.hash_table WHERE "key" = _schema || '#sep#' || _tbl || '#sep#' || _col into q_hash_grouped, q;

  IF q_hash_grouped IS NULL THEN
    RETURN result;
  END IF;

  lower_size = power(10, floor(log(q)));
  upper_size = power(10, floor(log(q)) + 1) - 1;

  -- find partition that's lower than
  -- limit 50 partitions
  execute format('SELECT array_agg("part"), array_agg("upper"), array_agg(distinct "upper") FROM (SELECT "part", "upper" FROM sketch.partition_table WHERE "lower" BETWEEN %s and %s limit 100) t1', lower_size, upper_size) INTO query_parts, upper_ls, d_upper_ls;

  -- compute the params for each distinct upper_bound
  FOR i IN 1..array_length(d_upper_ls, 1) LOOP
    _key = k::text || ';' || l::text || ';' || d_upper_ls[i]::text || ';' || q::text || ';' || t::text;
    -- raise notice '_key is %', _key;
    _keys_str = _keys_str || '''' || _key || '''';
    IF i != array_length(d_upper_ls, 1) THEN
      _keys_str = _keys_str || ',';
    END IF;
  END LOOP;

  -- raise notice '_keys_str is %', _keys_str;

  -- TODO: add what if optk and optl are not in the table and we need to compute them
  -- raise notice '_key in %', _keys_str;
  -- TODO: see if no array_agg is faster
  EXECUTE format('SELECT array_agg(optk), array_agg(optl) FROM sketch.optkl_table WHERE key IN (%s)', _keys_str) INTO optk_ls, optl_ls;

  FOR i IN 1..array_length(d_upper_ls, 1) LOOP
    -- raise notice 'error %', optk_ls[i];
    optk_dict = jsonb_set(optk_dict, array[d_upper_ls[i]]::text[], format('%s', optk_ls[i])::jsonb, true);
    optl_dict = jsonb_set(optl_dict, array[d_upper_ls[i]]::text[], format('%s', optl_ls[i])::jsonb, true);
  END LOOP;

  -- loop through each partition
  FOR i IN 1..array_length(query_parts, 1) LOOP
    _part = query_parts[i];
    opt_k = optk_dict->>(upper_ls[i])::text;
    opt_l = optl_dict->>(upper_ls[i])::text;
    -- raise notice 'upper is %', upper_ls[i];

    FOR d IN
      select "hash", "hashkey_str", "sorted_key_array" FROM sketch.hash_group_table WHERE "part" = _part AND ("hash" BETWEEN 1 AND opt_l)
    LOOP
      id_idxs = sketch.lshe_evaluate(d."hashkey_str", substring(q_hash_grouped[d."hash"] from 1 for opt_k * 8), opt_k);

      FOREACH id_idx in ARRAY id_idxs LOOP
        result_count = result_count + 1;
        result = result || d."sorted_key_array"[id_idx+1];
        IF result_count = 500 THEN
          RETURN result;
        END IF;
      END LOOP;
    END LOOP;
  END LOOP;

  RETURN result;
END;
$$ LANGUAGE plpgsql;