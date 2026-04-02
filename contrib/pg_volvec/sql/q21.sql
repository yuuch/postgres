SET client_min_messages = warning;
SET max_parallel_workers_per_gather = 0;
SET max_parallel_workers = 0;
SET jit = off;
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
	s_nationkey integer NOT NULL
);

CREATE TABLE orders (
	o_orderkey integer NOT NULL,
	o_orderstatus char(1) NOT NULL
);

CREATE TABLE lineitem (
	l_orderkey integer NOT NULL,
	l_suppkey integer NOT NULL,
	l_receiptdate date NOT NULL,
	l_commitdate date NOT NULL
);

INSERT INTO nation VALUES
	(1, 'SAUDI ARABIA'),
	(2, 'GERMANY');

INSERT INTO supplier VALUES
	(1, 'SuppA', 1),
	(2, 'SuppB', 1),
	(3, 'SuppC', 2);

INSERT INTO orders VALUES
	(10, 'F'),
	(20, 'F'),
	(30, 'O');

INSERT INTO lineitem VALUES
	(10, 1, date '1996-01-05', date '1996-01-01'),
	(10, 2, date '1996-01-03', date '1996-01-04'),
	(20, 1, date '1996-01-05', date '1996-01-01'),
	(20, 2, date '1996-01-06', date '1996-01-04'),
	(30, 1, date '1996-01-05', date '1996-01-01'),
	(30, 3, date '1996-01-03', date '1996-01-04');

SET join_collapse_limit = 1;
SET enable_hashjoin = off;
SET enable_mergejoin = off;

select
	s_name,
	count(*) as numwait
from
	supplier,
	lineitem l1,
	orders,
	nation
where
	s_suppkey = l1.l_suppkey
	and o_orderkey = l1.l_orderkey
	and o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and exists (
		select
			*
		from
			lineitem l2
		where
			l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey
	)
	and not exists (
		select
			*
		from
			lineitem l3
		where
			l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.l_receiptdate > l3.l_commitdate
	)
	and s_nationkey = n_nationkey
	and n_name = 'SAUDI ARABIA'
group by
	s_name
order by
	numwait desc,
	s_name
limit 100;

DROP TABLE lineitem;
DROP TABLE orders;
DROP TABLE supplier;
DROP TABLE nation;
DROP EXTENSION pg_vec;
