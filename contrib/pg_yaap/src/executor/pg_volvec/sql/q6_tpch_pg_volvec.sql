LOAD 'pg_volvec';

SET client_min_messages = notice;
SET statement_timeout = '180s';
SET jit = off;
SET enable_eager_aggregate = off;

SET pg_volvec.enabled = on;
SET pg_volvec.trace_hooks = off;
SET pg_volvec.parallel = on;
SET pg_volvec.parallel_max_workers = 14;
SET pg_volvec.parallel_leader_participation = off;

SET max_parallel_workers_per_gather = 0;
SET max_parallel_workers = 0;
SET min_parallel_table_scan_size = '1000GB';
SET parallel_setup_cost = 1000000000;
SET parallel_tuple_cost = 1000000000;
\timing
SELECT sum(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= date '1994-01-01'
  AND l_shipdate < date '1995-01-01'
  AND l_discount >= 0.05
  AND l_discount <= 0.07
  AND l_quantity < 24;
