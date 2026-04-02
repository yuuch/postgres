SET client_min_messages = warning;
CREATE EXTENSION pg_vec;
SET pg_vec.enabled = on;

CREATE TABLE lineitem (
	l_shipdate date NOT NULL,
	l_discount numeric(15,2) NOT NULL,
	l_quantity numeric(15,2) NOT NULL,
	l_extendedprice numeric(15,2) NOT NULL,
	l_tax numeric(15,2) NOT NULL
);

INSERT INTO lineitem (l_shipdate, l_discount, l_quantity, l_extendedprice, l_tax)
VALUES
	(date '1994-01-02', 0.05, 10.00, 100.00, 0.10),
	(date '1994-07-14', 0.05, 23.00, 220.00, 0.05),
	(date '1995-01-01', 0.05, 20.00, 999.00, 0.00),
	(date '1994-02-10', 0.08, 12.00, 300.00, 0.02),
	(date '1994-03-20', 0.06, 24.00, 400.00, 0.03),
	(date '1994-06-15', 0.06, 12.00, 50.00, 0.02);

SELECT
	sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge
FROM lineitem
WHERE (l_shipdate >= date '1994-01-01' AND l_shipdate < date '1995-01-01')
  AND (l_discount = 0.05 OR l_discount = 0.06)
  AND l_quantity < 24;

DROP TABLE lineitem;
DROP EXTENSION pg_vec;
