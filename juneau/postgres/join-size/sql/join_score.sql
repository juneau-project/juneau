CREATE OR REPLACE FUNCTION utils.join_score(_sch1 text, _tbl1 text, _col1 text, _sch2 text, _tbl2 text, _col2 text) RETURNS double precision AS $$
DECLARE
  _type1 text;
  _type2 text;

  _key1 text;
  _key2 text;
  _key text;

  arr1 bigint[];
  arr2 bigint[];

  str1 text;
  str2 text;
  _count1 int;
  _count2 int;

  score double precision;
BEGIN
  -- fetch the types of the two columns
  SELECT data_type FROM information_schema.columns WHERE table_schema = _sch1 AND table_name = _tbl1 AND column_name = _col1 INTO _type1;
  SELECT data_type FROM information_schema.columns WHERE table_schema = _sch2 AND table_name = _tbl2 AND column_name = _col2 INTO _type2;

  _key1 = _sch1 || '#sep#' || _tbl1 || '#sep#' || _col1;
  _key2 = _sch2 || '#sep#' || _tbl2 || '#sep#' || _col2;

  IF _key1 <= _key2 THEN
    _key = _key1 || '#sep' || _key2;
  ELSE
    _key = _key2 || '#sep' || _key1;
  END IF;

  IF _type1 != _type2 THEN
    INSERT INTO utils.join_score_table VALUES (_key, 0.0);
    RETURN 0.0;
  END IF;

  EXECUTE FORMAT('SELECT score FROM utils.join_score_table WHERE "key"=''%s''', _key) INTO score;

  IF score IS NOT NULL THEN
    RETURN score;
  END IF;

  IF _type1 = 'bigint'::text THEN
    EXECUTE format('SELECT ARRAY_AGG("%s") FROM %s."%s" WHERE "%s" IS NOT NULL', _col1, _sch1, _tbl1, _col1) into arr1;
    EXECUTE format('SELECT ARRAY_AGG("%s") FROM %s."%s" WHERE "%s" IS NOT NULL', _col2, _sch2, _tbl2, _col2) into arr2;

    IF arr1 IS NOT NULL AND arr2 IS NOT NULL THEN
      IF array_length(arr1, 1) = 0 OR array_length(arr2, 1) = 0 THEN
        INSERT INTO utils.join_score_table VALUES (_key, 0.0);
        RETURN 0.0;
      END IF;

      score = utils.c_join_score_int(arr1, arr2);
      INSERT INTO utils.join_score_table VALUES (_key, score);
      RETURN score;
    ELSE
      INSERT INTO utils.join_score_table VALUES (_key, 0.0);
      RETURN 0.0;
    END IF;
  ELSIF _type1 = 'text' THEN
    EXECUTE format('SELECT STRING_AGG("%s", ''#sep#''), count(*) FROM %s."%s" WHERE "%s" IS NOT NULL', _col1, _sch1, _tbl1, _col1) INTO str1, _count1;
    EXECUTE format('SELECT STRING_AGG("%s", ''#sep#''), count(*) FROM %s."%s" WHERE "%s" IS NOT NULL', _col2, _sch2, _tbl2, _col2) INTO str2, _count2;

    IF str1 IS NOT NULL AND str2 IS NOT NULL THEN
      IF str1 = '' OR str2 = '' THEN
        INSERT INTO utils.join_score_table VALUES (_key, 0.0);
        RETURN 0.0;
      END IF;

      score = utils.c_join_score_str(str1, _count1, str2, _count2);
      INSERT INTO utils.join_score_table VALUES (_key, score);
      RETURN score;
    ELSE
      INSERT INTO utils.join_score_table VALUES (_key, 0.0);
      RETURN 0.0;
    END IF;
  END IF;

  INSERT INTO utils.join_score_table VALUES (_key, 0.0);
  RETURN 0.0;
END;
$$ LANGUAGE plpgsql;