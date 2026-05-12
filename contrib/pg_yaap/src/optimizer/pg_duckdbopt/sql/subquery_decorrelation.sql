\c postgres
SET client_min_messages = warning;
DROP DATABASE IF EXISTS pg_opt_subquery;
CREATE DATABASE pg_opt_subquery;
\c pg_opt_subquery

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

SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 JOIN t2 AS t3 ON t2.id = t3.id WHERE t2.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE (EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)) IS TRUE$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE (EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)) IS NOT TRUE$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t1.col = 1 AND t2.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t1.col = 1 AND t2.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE id = (SELECT max(id) FROM t2 WHERE t2.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE col = (SELECT max(t2.col) FROM t2 WHERE t2.id = t1.id AND EXISTS (SELECT 1 FROM t2 AS t3 WHERE t3.id = t2.id AND t3.col = t1.col))$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT (SELECT max(id) FROM t2 WHERE t2.id = t1.id) FROM t1$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT (SELECT max(t2.col) FROM t2 WHERE EXISTS (SELECT 1 FROM t2 AS t3 WHERE t3.id = t2.id AND t3.col = t1.col)) FROM t1$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT t2.id, (SELECT max(t3.col) FROM t2 AS t3 WHERE t3.id = t2.id AND t3.col = t1.col) AS mx FROM t2) s WHERE s.id = t1.id AND s.mx = t1.col)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT max(t2.col) AS mx FROM t2 WHERE t2.id = t1.id) s WHERE s.mx = t1.col)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT max(t2.col) AS mx FROM t2 WHERE t2.id = t1.id) s WHERE s.mx > 0)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE col IN (SELECT id FROM t2)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE col NOT IN (SELECT id FROM t2)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE (col IN (SELECT id FROM t2)) IS TRUE$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE (id, col) = (SELECT id, col FROM t2 WHERE t2.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT DISTINCT t2.id FROM t2 WHERE t2.id = t1.id) s WHERE s.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT DISTINCT t2.id FROM t2 WHERE t2.col = t1.col) s WHERE s.id > 10)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE id = (SELECT id FROM t2 WHERE t2.col = t1.col ORDER BY t2.id LIMIT 1)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT t2.id FROM t2 WHERE t2.col = t1.col ORDER BY t2.id LIMIT 1) s WHERE s.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT t2.id, row_number() OVER (PARTITION BY t2.col ORDER BY t2.id) AS rn FROM t2 WHERE t2.col = t1.col) s WHERE s.rn = 1 AND s.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT t2.id, rank() OVER (PARTITION BY t2.col ORDER BY t2.id) AS rk FROM t2 WHERE t2.col = t1.col) s WHERE s.rk = 1 AND s.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT t2.id, dense_rank() OVER (PARTITION BY t2.col ORDER BY t2.id) AS dr FROM t2 WHERE t2.col = t1.col) s WHERE s.dr = 1 AND s.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT t2.id, percent_rank() OVER (PARTITION BY t2.col ORDER BY t2.id) AS pr FROM t2 WHERE t2.col = t1.col) s WHERE s.pr = 0 AND s.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT t2.id, cume_dist() OVER (PARTITION BY t2.col ORDER BY t2.id) AS cd FROM t2 WHERE t2.col = t1.col) s WHERE s.cd < 0.5 AND s.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT DISTINCT id, pr FROM (SELECT t2.id, percent_rank() OVER (PARTITION BY t2.col ORDER BY t2.id) AS pr FROM t2 WHERE t2.col = t1.col) w) s WHERE s.pr = 0 AND s.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT id FROM (SELECT t2.id, rank() OVER (PARTITION BY t2.col ORDER BY t2.id) AS rk FROM t2 WHERE t2.col = t1.col) w WHERE rk = 1) s WHERE s.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT DISTINCT id FROM (SELECT t2.id, dense_rank() OVER (PARTITION BY t2.col ORDER BY t2.id) AS dr FROM t2 WHERE t2.col = t1.col) w WHERE dr = 1) s WHERE s.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT id FROM t2 WHERE t2.id = t1.id UNION ALL SELECT id FROM t2 WHERE t2.id = t1.id) s WHERE s.id = t1.id)$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM (SELECT id FROM t2 WHERE t2.id = t1.id UNION ALL SELECT id FROM t2 WHERE t2.col = t1.col) s WHERE s.id = t1.id)$$);

\t off
\pset format aligned
\c postgres
DROP DATABASE pg_opt_subquery;
