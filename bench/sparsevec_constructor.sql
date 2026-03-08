-- Benchmark: sparsevec constructor performance
--
-- Compares two ways to build a sparsevec at varying nnz sizes (10, 100, 500, 1000):
--
--   A1  Pre-built text string  -> cast to sparsevec
--   A2  Pre-built arrays       -> sparsevec(int[], real[], int) constructor
--   C1  Pre-built arrays       -> build text string -> cast  (round-trip via text)
--   C2  Pre-built arrays       -> sparsevec(int[], real[], int) constructor  (direct)
--
-- Groups A and C use the same pre-built inputs so they measure conversion cost only,
-- with no aggregation or string-building overhead mixed in.
--
-- Usage:
--   psql -d <dbname> -f bench/sparsevec_constructor.sql
--
-- Results are printed via RAISE NOTICE in ns/call (N=100,000 iterations).

DROP TABLE IF EXISTS _bench_sparsevec;

CREATE TEMP TABLE _bench_sparsevec AS
WITH sizes(n) AS (VALUES (10),(100),(500),(1000))
SELECT
    n,
    '{' || string_agg(s.gs::text || ':' || (s.gs * 0.1 + 0.1)::real::text, ',' ORDER BY s.gs) || '}/' || n AS txt,
    array_agg(s.gs ORDER BY s.gs)::int[]              AS idxarr,
    array_agg((s.gs * 0.1 + 0.1)::real ORDER BY s.gs)::real[]             AS valarr,
    array_agg(s.gs ORDER BY s.gs)::integer[]          AS intarr,
    array_agg((s.gs * 0.1 + 0.1) ORDER BY s.gs)::double precision[]       AS f8arr,
    array_agg((s.gs * 0.1 + 0.1)::numeric ORDER BY s.gs)::numeric[]       AS numarr
FROM sizes
JOIN LATERAL (SELECT gs FROM generate_series(1, sizes.n) gs) s ON true
GROUP BY n;

DO $$
DECLARE
    t0     timestamp;
    v      sparsevec;
    txt10  text;  txt100  text;  txt500  text;  txt1000  text;
    idx10  int[]; idx100  int[]; idx500  int[]; idx1000  int[];
    val10  real[]; val100 real[]; val500 real[]; val1000 real[];
    int10  integer[]; int100 integer[]; int500 integer[]; int1000 integer[];
    f8_10  double precision[]; f8_100 double precision[]; f8_500 double precision[]; f8_1000 double precision[];
    num10  numeric[]; num100 numeric[]; num500 numeric[]; num1000 numeric[];
    N      int := 100000;
    ns10   numeric; ns100 numeric; ns500 numeric; ns1000 numeric;
    dim10  int := 10; dim100 int := 100; dim500 int := 500; dim1000 int := 1000;
BEGIN
    SELECT b.txt, b.idxarr, b.valarr, b.intarr, b.f8arr, b.numarr
        INTO txt10,   idx10,   val10,   int10,   f8_10,   num10   FROM _bench_sparsevec b WHERE b.n = 10;
    SELECT b.txt, b.idxarr, b.valarr, b.intarr, b.f8arr, b.numarr
        INTO txt100,  idx100,  val100,  int100,  f8_100,  num100  FROM _bench_sparsevec b WHERE b.n = 100;
    SELECT b.txt, b.idxarr, b.valarr, b.intarr, b.f8arr, b.numarr
        INTO txt500,  idx500,  val500,  int500,  f8_500,  num500  FROM _bench_sparsevec b WHERE b.n = 500;
    SELECT b.txt, b.idxarr, b.valarr, b.intarr, b.f8arr, b.numarr
        INTO txt1000, idx1000, val1000, int1000, f8_1000, num1000 FROM _bench_sparsevec b WHERE b.n = 1000;

    RAISE NOTICE 'sparsevec constructor benchmark  (N=% iterations)', N;
	RAISE NOTICE '';
	RAISE NOTICE 'A: input already available as text or array (conversion cost to sparsevec only)';
	RAISE NOTICE 'C: starting from arrays via text intermediate (C1) vs direct constructor (C2)';
    RAISE NOTICE '';
    RAISE NOTICE 'Scenario             n=10     n=100    n=500    n=1000   (ns/call)';
    RAISE NOTICE '-------------------- -------- -------- -------- --------';

    -- A1: pre-built text -> sparsevec (measures text parser only)
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := txt10::sparsevec;   END LOOP;
    ns10   := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := txt100::sparsevec;  END LOOP;
    ns100  := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := txt500::sparsevec;  END LOOP;
    ns500  := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := txt1000::sparsevec; END LOOP;
    ns1000 := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    RAISE NOTICE 'A1 text->sv          % % % %',
        lpad(ns10::text,8), lpad(ns100::text,8), lpad(ns500::text,8), lpad(ns1000::text,8);

    -- A2: pre-built arrays -> sparsevec (measures constructor only)
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx10,   val10,   dim10);   END LOOP;
    ns10   := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx100,  val100,  dim100);  END LOOP;
    ns100  := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx500,  val500,  dim500);  END LOOP;
    ns500  := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx1000, val1000, dim1000); END LOOP;
    ns1000 := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    RAISE NOTICE 'A2 real[]->sv        % % % %',
        lpad(ns10::text,8), lpad(ns100::text,8), lpad(ns500::text,8), lpad(ns1000::text,8);

    -- A3: integer[] values
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx10,   int10,   dim10);   END LOOP;
    ns10   := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx100,  int100,  dim100);  END LOOP;
    ns100  := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx500,  int500,  dim500);  END LOOP;
    ns500  := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx1000, int1000, dim1000); END LOOP;
    ns1000 := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    RAISE NOTICE 'A3 integer[]->sv     % % % %',
        lpad(ns10::text,8), lpad(ns100::text,8), lpad(ns500::text,8), lpad(ns1000::text,8);

    -- A4: double precision[] values
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx10,   f8_10,   dim10);   END LOOP;
    ns10   := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx100,  f8_100,  dim100);  END LOOP;
    ns100  := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx500,  f8_500,  dim500);  END LOOP;
    ns500  := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx1000, f8_1000, dim1000); END LOOP;
    ns1000 := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    RAISE NOTICE 'A4 float8[]->sv      % % % %',
        lpad(ns10::text,8), lpad(ns100::text,8), lpad(ns500::text,8), lpad(ns1000::text,8);

    -- A5: numeric[] values
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx10,   num10,   dim10);   END LOOP;
    ns10   := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx100,  num100,  dim100);  END LOOP;
    ns100  := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx500,  num500,  dim500);  END LOOP;
    ns500  := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx1000, num1000, dim1000); END LOOP;
    ns1000 := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    RAISE NOTICE 'A5 numeric[]->sv     % % % %',
        lpad(ns10::text,8), lpad(ns100::text,8), lpad(ns500::text,8), lpad(ns1000::text,8);

    RAISE NOTICE '';

    -- C1: pre-built arrays -> build text string -> cast
    --     Models the case where data is in arrays but must go via text
    t0 := clock_timestamp();
    FOR i IN 1..N LOOP
        v := ('{' || (SELECT string_agg(idx10[j]::text || ':' || val10[j]::text, ',')
                      FROM generate_series(1, 10) j) || '}/10')::sparsevec;
    END LOOP;
    ns10 := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);

    t0 := clock_timestamp();
    FOR i IN 1..N LOOP
        v := ('{' || (SELECT string_agg(idx100[j]::text || ':' || val100[j]::text, ',')
                      FROM generate_series(1, 100) j) || '}/100')::sparsevec;
    END LOOP;
    ns100 := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);

    t0 := clock_timestamp();
    FOR i IN 1..N LOOP
        v := ('{' || (SELECT string_agg(idx500[j]::text || ':' || val500[j]::text, ',')
                      FROM generate_series(1, 500) j) || '}/500')::sparsevec;
    END LOOP;
    ns500 := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);

    t0 := clock_timestamp();
    FOR i IN 1..N LOOP
        v := ('{' || (SELECT string_agg(idx1000[j]::text || ':' || val1000[j]::text, ',')
                      FROM generate_series(1, 1000) j) || '}/1000')::sparsevec;
    END LOOP;
    ns1000 := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    RAISE NOTICE 'C1 arr->txt->sv      % % % %',
        lpad(ns10::text,8), lpad(ns100::text,8), lpad(ns500::text,8), lpad(ns1000::text,8);

    -- C2: pre-built arrays -> sparsevec directly (direct comparison to C1)
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx10,   val10,   dim10);   END LOOP;
    ns10   := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx100,  val100,  dim100);  END LOOP;
    ns100  := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx500,  val500,  dim500);  END LOOP;
    ns500  := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    t0 := clock_timestamp(); FOR i IN 1..N LOOP v := sparsevec(idx1000, val1000, dim1000); END LOOP;
    ns1000 := round(extract(epoch from (clock_timestamp()-t0))*1e9/N, 0);
    RAISE NOTICE 'C2 arr->sv (direct)  % % % %',
        lpad(ns10::text,8), lpad(ns100::text,8), lpad(ns500::text,8), lpad(ns1000::text,8);

	RAISE NOTICE '-------------------- -------- -------- -------- --------';
END;
$$ LANGUAGE plpgsql;

DROP TABLE _bench_sparsevec;