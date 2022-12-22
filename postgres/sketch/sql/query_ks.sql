CREATE OR REPLACE FUNCTION sketch.query_ks_col(_schema text, _tbl text, _col text, t float) RETURNS text[] AS $$
DECLARE
  d record;
  _key text;
  _type text;
  result text[] = '{}';
  h bigint[];
  _score float;
  _exec_string text;
  result_count integer = 0;

  _max_hist integer;
BEGIN
  SELECT hist_group, max_hist FROM sketch.hist_int_table WHERE "key" = _schema || '#sep#' || _tbl || '#sep#' || _col INTO h, _max_hist;

  IF h IS NULL THEN
    SELECT hist FROM sketch.hist_float_table WHERE "key" = _schema || '#sep#' || _tbl || '#sep#' || _col INTO h;

    IF h IS NULL THEN
      RETURN result;
    ELSE
      FOR d IN SELECT * FROM sketch.hist_float_table LOOP
        _score = sketch.ks_evaluate(h, d."hist");
        IF _score >= t THEN
          result_count = result_count + 1;
          result = result || (d.key || '#sep#' || _score::text);
        END IF;

        EXIT WHEN result_count = 500;
      END LOOP;
    END IF;
  ELSE
    IF _max_hist < 500 THEN
      _exec_string = 'select key, "hist_group" from sketch.hist_int_table where "max_hist" < 500';
    ELSIF _max_hist between 500 and 1000 THEN
      _exec_string = 'select key, "hist_group" from sketch.hist_int_table where "max_hist" between 500 and 1000';
    ELSE
      _exec_string = 'select key, "hist_group" from sketch.hist_int_table where "max_hist" > 1000';
    END IF;

    FOR d IN execute(_exec_string) LOOP
      _score = sketch.ks_evaluate(h, d."hist_group");
      IF _score >= t THEN
        result_count = result_count + 1;
        result = result || (d.key || '#sep#' || _score::text);
      END IF;

      EXIT WHEN result_count = 500;
    END LOOP;
  END IF;

  RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sketch.query_ks_profile_int(arr bigint[], num_digits integer, t float) RETURNS text[] AS $$
DECLARE
  d record;

  h bigint[];
  result text[] = '{}';

  _score float;
BEGIN
  h = sketch.hist_int(arr, num_digits);

  FOR d IN
    SELECT * FROM sketch.profile_hist_int_table
  LOOP
    _score = sketch.ks_evaluate(h, d."hist");
    IF _score >= t THEN
      result = result || (d.key || '#sep#' || _score::text);
    END IF;
  END LOOP;

  RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sketch.query_ks_profile_float(arr double precision[], num_digits integer, t float) RETURNS text[] AS $$
DECLARE
  d record;

  h bigint[];
  result text[] = '{}';

  _score float;
BEGIN
  h = sketch.hist_float(arr, num_digits);

  FOR d IN
    SELECT * FROM sketch.profile_hist_float_table
  LOOP
    _score = sketch.ks_evaluate(h, d."hist");
    IF _score >= t THEN
      result = result || (d.key || '#sep#' || _score::text);
    END IF;
  END LOOP;

  RETURN result;
END;
$$ LANGUAGE plpgsql;