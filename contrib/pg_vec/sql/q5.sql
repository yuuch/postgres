SET client_min_messages = warning;
SET datestyle = 'ISO, MDY';
SET join_collapse_limit = 1;
CREATE EXTENSION pg_vec;
SET pg_vec.enabled = on;

CREATE TABLE region (
	r_regionkey integer NOT NULL,
	r_name text NOT NULL
);

CREATE TABLE nation (
	n_nationkey integer NOT NULL,
	n_name text NOT NULL,
	n_regionkey integer NOT NULL
);

CREATE TABLE supplier (
	s_suppkey integer NOT NULL,
	s_nationkey integer NOT NULL
);

CREATE TABLE customer (
	c_custkey integer NOT NULL,
	c_nationkey integer NOT NULL
);

CREATE TABLE orders (
	o_orderkey integer NOT NULL,
	o_custkey integer NOT NULL,
	o_orderdate date NOT NULL
);

CREATE TABLE lineitem (
	l_orderkey integer NOT NULL,
	l_suppkey integer NOT NULL,
	l_extendedprice numeric(15,2) NOT NULL,
	l_discount numeric(15,2) NOT NULL
);

INSERT INTO region (r_regionkey, r_name)
VALUES
	(1, 'ASIA'),
	(2, 'EUROPE');

INSERT INTO nation (n_nationkey, n_name, n_regionkey)
VALUES
	(1, 'INDIA', 1),
	(2, 'CHINA', 1),
	(3, 'FRANCE', 2);

INSERT INTO supplier (s_suppkey, s_nationkey)
VALUES
	(1, 1),
	(2, 2),
	(3, 3);

INSERT INTO customer (c_custkey, c_nationkey)
VALUES
	(1, 1),
	(2, 2),
	(3, 3);

INSERT INTO orders (o_orderkey, o_custkey, o_orderdate)
VALUES
	(1, 1, date '1994-03-15'),
	(2, 2, date '1994-07-01'),
	(3, 3, date '1994-02-01'),
	(4, 1, date '1995-01-01'),
	(5, 1, date '1994-09-09');

INSERT INTO lineitem (l_orderkey, l_suppkey, l_extendedprice, l_discount)
VALUES
	(1, 1, 100.00, 0.10),
	(1, 2, 50.00, 0.00),
	(2, 2, 200.00, 0.25),
	(3, 3, 300.00, 0.10),
	(4, 1, 500.00, 0.20),
	(5, 1, 80.00, 0.50);

SELECT
	n_name,
	sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
WHERE
	c_custkey = o_custkey
	AND l_orderkey = o_orderkey
	AND l_suppkey = s_suppkey
	AND c_nationkey = s_nationkey
	AND s_nationkey = n_nationkey
	AND n_regionkey = r_regionkey
	AND r_name = 'ASIA'
	AND o_orderdate >= date '1994-01-01'
	AND o_orderdate < date '1994-01-01' + interval '1' year
GROUP BY
	n_name
ORDER BY
	revenue DESC;

DROP TABLE lineitem;
DROP TABLE orders;
DROP TABLE customer;
DROP TABLE supplier;
DROP TABLE nation;
DROP TABLE region;
DROP EXTENSION pg_vec;
