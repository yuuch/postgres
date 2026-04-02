SET client_min_messages = warning;
CREATE EXTENSION pg_vec;
SET pg_vec.enabled = on;

CREATE TABLE part (
	p_partkey integer NOT NULL,
	p_brand char(10) NOT NULL,
	p_size integer NOT NULL,
	p_container char(10) NOT NULL
);

CREATE TABLE lineitem (
	l_partkey integer NOT NULL,
	l_quantity numeric(15,2) NOT NULL,
	l_extendedprice numeric(15,2) NOT NULL,
	l_discount numeric(15,2) NOT NULL,
	l_shipmode char(10) NOT NULL,
	l_shipinstruct char(25) NOT NULL
);

INSERT INTO part (p_partkey, p_brand, p_size, p_container)
VALUES
	(1, 'Brand#12', 5, 'SM BOX'),
	(2, 'Brand#23', 10, 'MED BAG'),
	(3, 'Brand#34', 15, 'LG PKG'),
	(4, 'Brand#12', 3, 'SM CASE');

INSERT INTO lineitem
	(l_partkey, l_quantity, l_extendedprice, l_discount, l_shipmode, l_shipinstruct)
VALUES
	(1, 5.00, 100.00, 0.10, 'AIR', 'DELIVER IN PERSON'),
	(1, 15.00, 200.00, 0.10, 'AIR', 'DELIVER IN PERSON'),
	(2, 15.00, 300.00, 0.20, 'AIR REG', 'DELIVER IN PERSON'),
	(2, 5.00, 400.00, 0.20, 'AIR', 'DELIVER IN PERSON'),
	(3, 25.00, 500.00, 0.00, 'AIR', 'DELIVER IN PERSON'),
	(3, 15.00, 600.00, 0.00, 'AIR', 'DELIVER IN PERSON'),
	(4, 8.00, 50.00, 0.00, 'RAIL', 'DELIVER IN PERSON'),
	(4, 8.00, 70.00, 0.00, 'AIR', 'TAKE BACK RETURN');

SELECT
	sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
	lineitem,
	part
WHERE
	(
		p_partkey = l_partkey
		AND p_brand = 'Brand#12'
		AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		AND l_quantity >= 1 AND l_quantity <= 1 + 10
		AND p_size BETWEEN 1 AND 5
		AND l_shipmode IN ('AIR', 'AIR REG')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	)
	OR
	(
		p_partkey = l_partkey
		AND p_brand = 'Brand#23'
		AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		AND l_quantity >= 10 AND l_quantity <= 10 + 10
		AND p_size BETWEEN 1 AND 10
		AND l_shipmode IN ('AIR', 'AIR REG')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	)
	OR
	(
		p_partkey = l_partkey
		AND p_brand = 'Brand#34'
		AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		AND l_quantity >= 20 AND l_quantity <= 20 + 10
		AND p_size BETWEEN 1 AND 15
		AND l_shipmode IN ('AIR', 'AIR REG')
		AND l_shipinstruct = 'DELIVER IN PERSON'
	);

DROP TABLE lineitem;
DROP TABLE part;
DROP EXTENSION pg_vec;
