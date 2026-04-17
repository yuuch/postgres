SET max_parallel_workers_per_gather = 0;
SET max_parallel_workers = 0;
SET min_parallel_table_scan_size = '1000GB';
SET parallel_setup_cost = 1000000000;
SET parallel_tuple_cost = 1000000000;
SET enable_parallel_append = off;
SET enable_parallel_hash = off;
SET force_parallel_mode = off;

LOAD 'llvmjit';
LOAD 'pg_volvec';
SET pg_volvec.enabled = on;
SET pg_volvec.trace_hooks = on;

\echo '--- Testing Simple Scan (Strict No Parallel) ---'
\timing on
SELECT
    sum(l_quantity)
FROM
    lineitem;
\timing off
