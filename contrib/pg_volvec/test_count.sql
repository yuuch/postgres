SET max_parallel_workers_per_gather = 0;
SET max_parallel_workers = 0;
SET min_parallel_table_scan_size = '100GB';
SET parallel_setup_cost = 1000000000;
SET parallel_tuple_cost = 1000000000;

LOAD 'llvmjit';
LOAD 'pg_volvec';
SET pg_volvec.enabled = on;
SET pg_volvec.trace_hooks = on;
SET client_min_messages = LOG;

\timing on
SELECT
    count(*)
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-12-01' - 90;
\timing off
