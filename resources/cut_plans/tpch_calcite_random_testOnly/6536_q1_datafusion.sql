SELECT COALESCE(CASE WHEN SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) >= 1000000.0000 THEN 'High  ' WHEN SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) >= 500000.0000 THEN 'Medium' ELSE 'Low   ' END, CASE WHEN SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) >= 1000000.0000 THEN 'High  ' WHEN SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) >= 500000.0000 THEN 'Medium' ELSE 'Low   ' END) AS "REVENUE_TIER", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "t"."o_orderkey") AS "TOTAL_ORDERS"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" < DATE '1997-01-01') AS "t"
INNER JOIN "TPCH"."lineitem" ON "t"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("TPCH"."nation" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "part"."p_partkey"
GROUP BY "nation"."n_name"