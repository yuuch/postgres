SET client_min_messages = warning;
SET join_collapse_limit = 1;
SET enable_nestloop = off;
SET enable_mergejoin = off;
CREATE EXTENSION pg_vec;
SET pg_vec.enabled = on;
SET pg_vec.jit_deform = off;

CREATE TABLE customer (
	c_custkey integer NOT NULL
);

CREATE TABLE orders (
	o_orderkey integer NOT NULL,
	o_custkey integer NOT NULL,
	o_comment varchar(79) NOT NULL
);

INSERT INTO customer VALUES
	(1),
	(2),
	(3);

INSERT INTO orders VALUES
	(10, 1, 'normal'),
	(11, 1, 'special requests'),
	(12, 2, 'x');

select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey)
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%special%requests%'
		group by
			c_custkey
	) as c_orders (c_custkey, c_count)
group by
	c_count
order by
	custdist desc,
	c_count desc;

DROP TABLE orders;
DROP TABLE customer;
DROP EXTENSION pg_vec;
