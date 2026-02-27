SELECT COALESCE(t8.R_NAME, t8.R_NAME) AS R_NAME, t8.S_NAME, t8.TOTAL_AVAILABLE, t8.TOTAL_SUPPLY_VALUE
FROM (SELECT t6.r_name AS R_NAME, t2.S_NAME, t2.TOTAL_AVAILABLE, t2.TOTAL_SUPPLY_VALUE
FROM (SELECT *
FROM s1
WHERE TOTAL_SUPPLY_VALUE > 1000000.00) AS t2
INNER JOIN (SELECT *
FROM partsupp
WHERE ps_suppkey = ps_partkey) AS t3 ON t2.S_SUPPKEY = t3.ps_suppkey
INNER JOIN part ON t3.ps_partkey = part.p_partkey
CROSS JOIN ((SELECT nation.n_regionkey, region.r_name, SUM(orders.o_totalprice) AS TOTAL_ORDER_VALUE
FROM region
INNER JOIN nation ON region.r_regionkey = nation.n_regionkey
INNER JOIN customer ON nation.n_nationkey = customer.c_nationkey
INNER JOIN orders ON customer.c_custkey = orders.o_custkey
GROUP BY region.r_name, nation.n_regionkey
ORDER BY 3 DESC NULLS FIRST
LIMIT 5) AS t6 INNER JOIN nation AS nation0 ON t6.n_regionkey = nation0.n_regionkey INNER JOIN supplier AS supplier0 ON nation0.n_nationkey = supplier0.s_nationkey)
ORDER BY t6.r_name, t2.TOTAL_SUPPLY_VALUE DESC NULLS FIRST) AS t8