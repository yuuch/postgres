SET max_parallel_workers_per_gather = 0;
SET max_parallel_workers = 0;
SET min_parallel_table_scan_size = '1000GB';
SET parallel_setup_cost = 1000000000;
SET parallel_tuple_cost = 1000000000;
SET debug_parallel_query = off;

LOAD 'llvmjit';
LOAD 'pg_volvec';
SET pg_volvec.enabled = on;
SET pg_volvec.trace_hooks = on;

\echo '--- Testing Page-wise Scan Power (TPC-H Q6 Logic) ---'
\timing on
SELECT
    sum(l_extendedprice * l_discount) as revenue
FROM
    lineitem
WHERE
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1995-01-01'
    and l_discount >= 0.05
    and l_discount <= 0.07
    and l_quantity < 24;
\timing off
