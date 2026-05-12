\c postgres
SET client_min_messages = warning;
DROP DATABASE IF EXISTS pg_opt_join_hyper;
CREATE DATABASE pg_opt_join_hyper;
\c pg_opt_join_hyper

SET client_min_messages = warning;
CREATE TABLE t1(id int, col int);
CREATE TABLE t2(id int, col int);
CREATE TABLE t3(id int, col int);
CREATE TABLE t4(id int, col int);

INSERT INTO t1
SELECT i, i
FROM generate_series(1, 20) AS g(i);
INSERT INTO t2
SELECT i, i
FROM generate_series(1, 20) AS g(i);
INSERT INTO t3
SELECT i, i * 2
FROM generate_series(1, 100) AS g(i);
INSERT INTO t4
SELECT i, i
FROM generate_series(1, 100) AS g(i);

ANALYZE t1;
ANALYZE t2;
ANALYZE t3;
ANALYZE t4;

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

SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF)
SELECT *
FROM t1
JOIN t2 ON true
JOIN t3 ON t1.id + t2.id = t3.col$$);

SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF)
SELECT *
FROM t1
JOIN t2 ON t1.id = t2.id
JOIN t3 ON t1.col + t2.col = t3.col
JOIN t4 ON t3.id = t4.id
WHERE t1.col = t4.col$$);

\t off
\pset format aligned
\c postgres
DROP DATABASE pg_opt_join_hyper;
