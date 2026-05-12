DROP TABLE IF EXISTS pgvv_j_a;
DROP TABLE IF EXISTS pgvv_j_b;
DROP TABLE IF EXISTS pgvv_j_c;

CREATE TABLE pgvv_j_a (k int4, a_val int4);
CREATE TABLE pgvv_j_b (k int4, b_val int4);
CREATE TABLE pgvv_j_c (k int4, c_val int4);

INSERT INTO pgvv_j_a VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO pgvv_j_b VALUES (1, 100), (2, 200), (4, 400);
INSERT INTO pgvv_j_c VALUES (1, 1000), (2, 2000), (5, 5000);

SET max_parallel_workers_per_gather = 0;
SET max_parallel_workers = 0;
SET min_parallel_table_scan_size = '1000GB';
SET parallel_setup_cost = 1000000000;
SET parallel_tuple_cost = 1000000000;
SET enable_mergejoin = off;
SET enable_nestloop = off;

SELECT a.k, a.a_val, b.b_val, c.c_val
FROM pgvv_j_a a
JOIN pgvv_j_b b ON a.k = b.k
JOIN pgvv_j_c c ON b.k = c.k
ORDER BY 1;
