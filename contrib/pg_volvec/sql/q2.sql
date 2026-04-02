SET client_min_messages = warning;
SET max_parallel_workers_per_gather = 0;
SET max_parallel_workers = 0;
SET jit = off;
SET max_stack_depth = '7MB';
CREATE EXTENSION pg_vec;
SET pg_vec.enabled = on;
SET pg_vec.jit_deform = off;

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
	s_name text NOT NULL,
	s_address text NOT NULL,
	s_nationkey integer NOT NULL,
	s_phone text NOT NULL,
	s_acctbal numeric(15,2) NOT NULL,
	s_comment text NOT NULL
);

CREATE TABLE part (
	p_partkey integer NOT NULL,
	p_mfgr text NOT NULL,
	p_size integer NOT NULL,
	p_type text NOT NULL
);

CREATE TABLE partsupp (
	ps_partkey integer NOT NULL,
	ps_suppkey integer NOT NULL,
	ps_supplycost numeric(15,2) NOT NULL
);

INSERT INTO region VALUES
	(1, 'EUROPE'),
	(2, 'ASIA');

INSERT INTO nation VALUES
	(10, 'GERMANY', 1),
	(20, 'CHINA', 2);

INSERT INTO supplier VALUES
	(1, 'SuppA', 'AddrA', 10, '111', 100.00, 'CommentA'),
	(2, 'SuppB', 'AddrB', 10, '222', 200.00, 'CommentB'),
	(3, 'SuppC', 'AddrC', 20, '333', 300.00, 'CommentC');

INSERT INTO part VALUES
	(100, 'MFGR#1', 15, 'SMALL BRASS'),
	(200, 'MFGR#2', 10, 'MEDIUM TIN');

INSERT INTO partsupp VALUES
	(100, 1, 50.00),
	(100, 2, 40.00),
	(100, 3, 30.00),
	(200, 1, 5.00);

SET join_collapse_limit = 1;
SET enable_hashjoin = off;
SET enable_mergejoin = off;

select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	part,
	supplier,
	partsupp,
	nation,
	region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 15
	and p_type like '%BRASS'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'EUROPE'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			partsupp,
			supplier,
			nation,
			region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'EUROPE'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100;

DROP TABLE partsupp;
DROP TABLE part;
DROP TABLE supplier;
DROP TABLE nation;
DROP TABLE region;
DROP EXTENSION pg_vec;
