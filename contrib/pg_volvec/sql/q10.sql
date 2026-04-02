SET client_min_messages = warning;
SET datestyle = 'ISO, MDY';
SET join_collapse_limit = 1;
CREATE EXTENSION pg_vec;
SET pg_vec.enabled = on;

CREATE TABLE nation (
	n_nationkey integer NOT NULL,
	n_name text NOT NULL
);

CREATE TABLE customer (
	c_custkey integer NOT NULL,
	c_name text NOT NULL,
	c_acctbal numeric(15,2) NOT NULL,
	c_nationkey integer NOT NULL,
	c_address text NOT NULL,
	c_phone text NOT NULL,
	c_comment text NOT NULL
);

CREATE TABLE orders (
	o_orderkey integer NOT NULL,
	o_custkey integer NOT NULL,
	o_orderdate date NOT NULL
);

CREATE TABLE lineitem (
	l_orderkey integer NOT NULL,
	l_extendedprice numeric(15,2) NOT NULL,
	l_discount numeric(15,2) NOT NULL,
	l_returnflag char(1) NOT NULL
);

INSERT INTO nation (n_nationkey, n_name)
VALUES
	(1, 'USA'),
	(2, 'CHINA');

INSERT INTO customer (
	c_custkey,
	c_name,
	c_acctbal,
	c_nationkey,
	c_address,
	c_phone,
	c_comment
)
VALUES
	(1, 'Alice', 1200.00, 1, '1 Main St', '10-000-000-0001', 'alpha'),
	(2, 'Bob', 800.00, 2, '2 Main St', '20-000-000-0002', 'beta'),
	(3, 'Carol', 300.00, 1, '3 Main St', '30-000-000-0003', 'gamma'),
	(4, 'Dave', 1500.00, 2, '4 Main St', '40-000-000-0004', 'delta');

INSERT INTO orders (o_orderkey, o_custkey, o_orderdate)
VALUES
	(1, 1, date '1993-10-05'),
	(2, 1, date '1993-11-01'),
	(3, 2, date '1993-10-20'),
	(4, 3, date '1994-01-02'),
	(5, 4, date '1993-12-15');

INSERT INTO lineitem (l_orderkey, l_extendedprice, l_discount, l_returnflag)
VALUES
	(1, 100.00, 0.10, 'R'),
	(1, 50.00, 0.00, 'A'),
	(2, 200.00, 0.05, 'R'),
	(3, 300.00, 0.20, 'R'),
	(4, 400.00, 0.00, 'R'),
	(5, 70.00, 0.10, 'R'),
	(5, 80.00, 0.00, 'N');

SELECT
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) AS revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
FROM
	customer
	JOIN orders ON c_custkey = o_custkey
	JOIN lineitem ON l_orderkey = o_orderkey
	JOIN nation ON c_nationkey = n_nationkey
WHERE
	o_orderdate >= date '1993-10-01'
	AND o_orderdate < date '1993-10-01' + interval '3' month
	AND l_returnflag = 'R'
GROUP BY
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
ORDER BY
	revenue DESC
LIMIT 20;

DROP TABLE lineitem;
DROP TABLE orders;
DROP TABLE customer;
DROP TABLE nation;
DROP EXTENSION pg_vec;
