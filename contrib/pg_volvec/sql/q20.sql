SET client_min_messages = warning;
SET max_parallel_workers_per_gather = 0;
SET max_parallel_workers = 0;
SET jit = off;
SET max_stack_depth = '7MB';
CREATE EXTENSION pg_vec;
SET pg_vec.enabled = on;
SET pg_vec.jit_deform = off;

CREATE TABLE nation (
	n_nationkey integer NOT NULL,
	n_name text NOT NULL
);

CREATE TABLE supplier (
	s_suppkey integer NOT NULL,
	s_name text NOT NULL,
	s_address text NOT NULL,
	s_nationkey integer NOT NULL
);

CREATE TABLE part (
	p_partkey integer NOT NULL,
	p_name text NOT NULL
);

CREATE TABLE partsupp (
	ps_partkey integer NOT NULL,
	ps_suppkey integer NOT NULL,
	ps_availqty integer NOT NULL
);

CREATE TABLE lineitem (
	l_partkey integer NOT NULL,
	l_suppkey integer NOT NULL,
	l_quantity numeric(15,2) NOT NULL,
	l_shipdate date NOT NULL
);

INSERT INTO nation VALUES
	(1, 'CANADA'),
	(2, 'BRAZIL');

INSERT INTO supplier VALUES
	(10, 'SuppA', 'AddrA', 1),
	(20, 'SuppB', 'AddrB', 1),
	(30, 'SuppC', 'AddrC', 2);

INSERT INTO part VALUES
	(100, 'forest green widget'),
	(200, 'desert widget');

INSERT INTO partsupp VALUES
	(100, 10, 100),
	(100, 20, 40),
	(200, 10, 100);

INSERT INTO lineitem VALUES
	(100, 10, 10.00, '1994-03-01'),
	(100, 10, 20.00, '1994-04-01'),
	(100, 20, 100.00, '1994-05-01'),
	(200, 10, 1.00, '1994-03-01');

SET join_collapse_limit = 1;
SET enable_nestloop = off;
SET enable_mergejoin = off;

SELECT
	s_name,
	s_address
FROM
	supplier,
	nation
WHERE
	s_suppkey IN (
		SELECT
			ps_suppkey
		FROM
			partsupp
		WHERE
			ps_partkey IN (
				SELECT
					p_partkey
				FROM
					part
				WHERE
					p_name LIKE 'forest%'
			)
			AND ps_availqty > (
				SELECT
					0.5 * sum(l_quantity)
				FROM
					lineitem
				WHERE
					l_partkey = ps_partkey
					AND l_suppkey = ps_suppkey
					AND l_shipdate >= date '1994-01-01'
					AND l_shipdate < date '1994-01-01' + interval '1' year
			)
	)
	AND s_nationkey = n_nationkey
	AND n_name = 'CANADA'
ORDER BY
	s_name;

DROP TABLE lineitem;
DROP TABLE partsupp;
DROP TABLE part;
DROP TABLE supplier;
DROP TABLE nation;
DROP EXTENSION pg_vec;
