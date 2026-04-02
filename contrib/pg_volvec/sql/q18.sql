SET client_min_messages = warning;
CREATE EXTENSION pg_vec;
SET pg_vec.enabled = on;

CREATE TABLE customer (
	c_custkey integer NOT NULL,
	c_name varchar(25) NOT NULL
);

CREATE TABLE orders (
	o_orderkey integer NOT NULL,
	o_custkey integer NOT NULL,
	o_orderdate date NOT NULL,
	o_totalprice numeric(15,2) NOT NULL
);

CREATE TABLE lineitem (
	l_orderkey integer NOT NULL,
	l_quantity numeric(15,2) NOT NULL
);

INSERT INTO customer VALUES
	(1, 'Customer#000000001'),
	(2, 'Customer#000000002'),
	(3, 'Customer#000000003'),
	(4, 'Customer#000000004'),
	(5, 'Customer#000000005'),
	(6, 'Customer#000000006'),
	(7, 'Customer#000000007');

INSERT INTO orders VALUES
	(101, 1, date '1995-01-01', 1000.00),
	(102, 2, date '1995-01-02', 900.00),
	(103, 3, date '1995-01-03', 800.00),
	(104, 4, date '1995-01-04', 700.00),
	(105, 5, date '1995-01-05', 600.00),
	(106, 6, date '1995-01-06', 500.00),
	(107, 7, date '1995-01-07', 1100.00);

INSERT INTO lineitem VALUES
	(101, 150.00),
	(101, 160.00),
	(102, 301.00),
	(103, 305.00),
	(104, 320.00),
	(105, 330.00),
	(106, 340.00),
	(107, 200.00);

SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
FROM customer, orders, lineitem
WHERE o_orderkey IN (
	SELECT l_orderkey
	FROM lineitem
	GROUP BY l_orderkey
	HAVING sum(l_quantity) > 300
)
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 5;

DROP TABLE lineitem;
DROP TABLE orders;
DROP TABLE customer;
DROP EXTENSION pg_vec;
