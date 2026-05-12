\c postgres
SET client_min_messages = warning;
DROP DATABASE IF EXISTS pg_opt_wrappers;
CREATE DATABASE pg_opt_wrappers;
\c pg_opt_wrappers

SET client_min_messages = warning;
CREATE TABLE t1(id int, col int);
CREATE TABLE t2(id int, col int);
INSERT INTO t1
SELECT i,
       CASE
         WHEN i <= 20 THEN 1
         WHEN i <= 30 THEN 2
         ELSE i
       END
FROM generate_series(1, 100) AS g(i);
INSERT INTO t2
SELECT i,
       CASE
         WHEN i <= 10 THEN NULL::int
         ELSE i
       END
FROM generate_series(1, 50) AS g(i);
ANALYZE t1;
ANALYZE t2;

LOAD 'pg_duckdbopt';
SET client_min_messages = error;
\pset format unaligned
\t on

CREATE FUNCTION explain_filter(text) RETURNS SETOF text
LANGUAGE plpgsql AS
$$
DECLARE
    ln text;
BEGIN
    FOR ln IN EXECUTE $1 LOOP
        ln := regexp_replace(ln, 'relid=[0-9]+', 'relid=NNN', 'g');
        ln := regexp_replace(ln, '#[0-9]+', '#NNN', 'g');
        IF ln ~ '^[[:space:]]*explain_filter[[:space:]]*$' THEN
            ln := 'explain_filter';
        ELSIF ln ~ '^[[:space:]-]+$' THEN
            ln := '----------------';
        ELSIF ln <> '' THEN
            ln := ' ' || ln;
        END IF;
        RETURN NEXT ln;
    END LOOP;
END;
$$;

SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT DISTINCT col FROM t1 ORDER BY col LIMIT 5$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT col FROM t1) s$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) WITH c AS (SELECT col FROM t1) SELECT * FROM c$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT col AS c FROM t1) s WHERE c = 1$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) WITH c AS (SELECT col AS c FROM t1) SELECT * FROM c WHERE c = 1$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT t1.col AS c1, t2.col AS c2 FROM t1 JOIN t2 ON t1.id = t2.id) s WHERE c1 = 1$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.id, t2.col FROM t1 LEFT JOIN t2 ON t1.id = t2.id$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT col + 1 AS c FROM t1) s WHERE c = 2$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT t1.id, EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id) AS hit FROM t1$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT t1.id, EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id) AS hit FROM t1) s WHERE hit$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT q.id, q.hit, row_number() OVER (ORDER BY q.id) AS rn FROM (SELECT t1.id, EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id) AS hit FROM t1) q) s WHERE hit$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT col, count(*) AS cnt FROM t1 GROUP BY col) s WHERE col = 1$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT col, count(*) AS cnt FROM t1 GROUP BY col) s WHERE cnt > 10$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT col, count(*) AS cnt FROM t1 GROUP BY col) s WHERE col = 1 AND cnt > 10$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT col + 1 AS c, count(*) AS cnt FROM t1 GROUP BY col + 1) s WHERE c = 2$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT col, count(*) FROM t1 GROUP BY col HAVING col = 1$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT col, count(*) FROM t1 GROUP BY col HAVING count(*) > 10$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT col, count(*) FROM t1 GROUP BY col HAVING col = 1 AND count(*) > 10$$);

\t off
\pset format aligned
\c postgres
DROP DATABASE pg_opt_wrappers;
