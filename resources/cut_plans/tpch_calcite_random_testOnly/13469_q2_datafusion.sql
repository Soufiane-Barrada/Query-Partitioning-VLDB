SELECT COALESCE("t1"."p_partkey", "t1"."p_partkey") AS "p_partkey", "t1"."p_name", "t1"."TOTAL_QUANTITY", "t1"."TOTAL_REVENUE", "t1"."TOTAL_ORDERS"
FROM (SELECT "s1"."p_partkey", "s1"."p_name", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", SUM("lineitem"."l_extendedprice") AS "TOTAL_REVENUE", COUNT(DISTINCT "orders"."o_orderkey") AS "TOTAL_ORDERS"
FROM "TPCH"."orders"
INNER JOIN "TPCH"."lineitem" ON "orders"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN ("TPCH"."supplier" INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey") ON "lineitem"."l_partkey" = "s1"."p_partkey"
GROUP BY "s1"."p_partkey", "s1"."p_name"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t1"