LOAD 'llvmjit';
CREATE EXTENSION IF NOT EXISTS pg_volvec;
SET pg_volvec.enabled = on;
SET pg_volvec.trace_hooks = off;

CREATE TABLE lineitem_q1 (
    l_returnflag char(1),
    l_linestatus char(1),
    l_quantity numeric(15,2),
    l_extendedprice numeric(15,2),
    l_discount numeric(15,2),
    l_tax numeric(15,2),
    l_shipdate date
);

INSERT INTO lineitem_q1 VALUES ('A', 'F', 10.0, 100.0, 0.1, 0.05, '1998-01-01');
INSERT INTO lineitem_q1 VALUES ('A', 'F', 20.0, 200.0, 0.2, 0.08, '1998-05-01');
INSERT INTO lineitem_q1 VALUES ('B', 'O', 30.0, 300.0, 0.0, 0.02, '1998-09-01');
INSERT INTO lineitem_q1 VALUES ('B', 'O', 40.0, 400.0, 0.05, 0.01, '1999-01-01');

SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
FROM
    lineitem_q1
WHERE
    l_shipdate <= date '1998-12-01'
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus;

DROP TABLE lineitem_q1;
