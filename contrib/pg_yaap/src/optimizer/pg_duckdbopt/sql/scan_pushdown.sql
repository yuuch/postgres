\c postgres
SET client_min_messages = warning;
DROP DATABASE IF EXISTS pg_opt_scan;
CREATE DATABASE pg_opt_scan;
\c pg_opt_scan

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

SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT col FROM t1 WHERE col = 1$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT col FROM t1 WHERE col < 50$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t2 WHERE col IS NULL$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1, t2 WHERE t1.id = t2.id AND t1.col = 1 AND t2.col > 40$$);
SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM t1 JOIN t2 ON t1.id = t2.id AND t1.col = 1 AND t2.col > 40$$);

\t off
\pset format aligned
\c postgres
DROP DATABASE pg_opt_scan;
