SELECT COALESCE("t3"."p_name", "t3"."p_name") AS "p_name", "t3"."TOTAL_QUANTITY", "t3"."TOTAL_REVENUE", "t3"."TOTAL_ORDERS"
FROM (SELECT "part"."p_name", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", SUM("lineitem"."l_extendedprice") AS "TOTAL_REVENUE", COUNT(DISTINCT "t1"."o_orderkey") AS "TOTAL_ORDERS"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" <= DATE '1997-12-31') AS "t1"
INNER JOIN "TPCH"."lineitem" ON "t1"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("s1" INNER JOIN "TPCH"."nation" ON "s1"."r_regionkey" = "nation"."n_regionkey" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "part"."p_partkey"
GROUP BY "part"."p_name"
ORDER BY 3 DESC NULLS FIRST) AS "t3"