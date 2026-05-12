\c postgres
SET client_min_messages = warning;
DROP DATABASE IF EXISTS pg_opt_join_semi_anti;
CREATE DATABASE pg_opt_join_semi_anti;
\c pg_opt_join_semi_anti

SET client_min_messages = warning;
CREATE TABLE t1(id int, col int);
CREATE TABLE t2(id int, col int);

INSERT INTO t1
SELECT i, i
FROM generate_series(1, 1000) AS g(i);
INSERT INTO t2
SELECT i, i
FROM generate_series(1, 10) AS g(i);

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

SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF)
SELECT *
FROM t1
WHERE id = ALL (SELECT id FROM t2)$$);

SELECT explain_filter($$EXPLAIN (VERBOSE, COSTS OFF)
SELECT *
FROM t1
WHERE NOT (id = ALL (SELECT id FROM t2))$$);

\t off
\pset format aligned
\c postgres
DROP DATABASE pg_opt_join_semi_anti;
