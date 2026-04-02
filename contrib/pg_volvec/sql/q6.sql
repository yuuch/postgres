LOAD 'llvmjit';
CREATE EXTENSION IF NOT EXISTS pg_volvec;
SET pg_volvec.enabled = on;
SET pg_volvec.trace_hooks = off;

CREATE TABLE lineitem_q6 (
    l_extendedprice numeric(15,2),
    l_discount numeric(15,2),
    l_quantity numeric(15,2),
    l_shipdate date
);

INSERT INTO lineitem_q6 VALUES (100.0, 0.06, 10.0, '1994-01-01');
INSERT INTO lineitem_q6 VALUES (200.0, 0.05, 20.0, '1994-02-01');
INSERT INTO lineitem_q6 VALUES (300.0, 0.07, 30.0, '1994-03-01');
INSERT INTO lineitem_q6 VALUES (400.0, 0.08, 40.0, '1994-04-01'); -- Discount too high
INSERT INTO lineitem_q6 VALUES (500.0, 0.06, 50.0, '1995-01-01'); -- Date too late

SELECT
    sum(l_extendedprice * l_discount) as revenue
FROM
    lineitem_q6
WHERE
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1995-01-01'
    and l_discount >= 0.05
    and l_discount <= 0.07
    and l_quantity < 24;

DROP TABLE lineitem_q6;
