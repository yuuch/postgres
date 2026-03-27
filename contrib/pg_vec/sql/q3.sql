SET client_min_messages = warning;
SET datestyle = 'ISO, MDY';
CREATE EXTENSION pg_vec;
SET pg_vec.enabled = on;

CREATE TABLE customer (
	c_custkey integer NOT NULL,
	c_mktsegment char(10) NOT NULL
);

CREATE TABLE orders (
	o_orderkey integer NOT NULL,
	o_custkey integer NOT NULL,
	o_orderdate date NOT NULL,
	o_shippriority integer NOT NULL
);

CREATE TABLE lineitem (
	l_orderkey integer NOT NULL,
	l_extendedprice numeric(15,2) NOT NULL,
	l_discount numeric(15,2) NOT NULL,
	l_shipdate date NOT NULL
);

INSERT INTO customer (c_custkey, c_mktsegment)
VALUES
	(1, 'BUILDING'),
	(2, 'BUILDING'),
	(3, 'AUTOMOBILE'),
	(4, 'BUILDING');

INSERT INTO orders (o_orderkey, o_custkey, o_orderdate, o_shippriority)
VALUES
	(1, 1, date '1995-03-10', 0),
	(2, 1, date '1995-03-12', 0),
	(3, 2, date '1995-03-11', 1),
	(4, 3, date '1995-03-10', 0),
	(5, 2, date '1995-03-16', 0),
	(6, 4, date '1995-03-09', 2);

INSERT INTO lineitem (l_orderkey, l_extendedprice, l_discount, l_shipdate)
VALUES
	(1, 100.00, 0.05, date '1995-03-16'),
	(1, 50.00, 0.10, date '1995-03-20'),
	(2, 200.00, 0.00, date '1995-03-16'),
	(3, 120.00, 0.00, date '1995-03-16'),
	(3, 500.00, 0.00, date '1995-03-10'),
	(4, 300.00, 0.00, date '1995-03-16'),
	(5, 400.00, 0.00, date '1995-03-16'),
	(6, 200.00, 0.00, date '1995-03-16');

SELECT
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) AS revenue,
	o_orderdate,
	o_shippriority
FROM
	customer,
	orders,
	lineitem
WHERE
	c_mktsegment = 'BUILDING'
	AND c_custkey = o_custkey
	AND l_orderkey = o_orderkey
	AND o_orderdate < date '1995-03-15'
	AND l_shipdate > date '1995-03-15'
GROUP BY
	l_orderkey,
	o_orderdate,
	o_shippriority
ORDER BY
	revenue DESC,
	o_orderdate
LIMIT 10;

DROP TABLE lineitem;
DROP TABLE orders;
DROP TABLE customer;
DROP EXTENSION pg_vec;
