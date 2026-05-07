-- Canonical TPC-H Q1 on tpch.lineitem (SF=10, ~60M rows) via pg_volvec
LOAD 'llvmjit';
CREATE EXTENSION IF NOT EXISTS pg_volvec;

SET pg_volvec.enabled        = on;
SET pg_volvec.parallel       = on;
SET pg_volvec.parallel_max_workers = 8;
SET pg_volvec.trace_hooks    = off;

-- Disable PG's own parallel so we measure pg_volvec only
SET max_parallel_workers_per_gather = 0;
SET max_parallel_workers            = 0;
SET min_parallel_table_scan_size    = '1000GB';
SET parallel_setup_cost             = 1000000000;
SET parallel_tuple_cost             = 1000000000;

\timing on

EXPLAIN (VERBOSE, COSTS OFF)
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity)                                       as sum_qty,
    sum(l_extendedprice)                                  as sum_base_price,
    sum(l_extendedprice * (1 - l_discount))               as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity)                                       as avg_qty,
    avg(l_extendedprice)                                  as avg_price,
    avg(l_discount)                                       as avg_disc,
    count(*)                                              as count_order
FROM lineitem
WHERE l_shipdate <= date '1998-12-01' - interval '90 day'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity)                                       as sum_qty,
    sum(l_extendedprice)                                  as sum_base_price,
    sum(l_extendedprice * (1 - l_discount))               as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity)                                       as avg_qty,
    avg(l_extendedprice)                                  as avg_price,
    avg(l_discount)                                       as avg_disc,
    count(*)                                              as count_order
FROM lineitem
WHERE l_shipdate <= date '1998-12-01' - interval '90 day'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
