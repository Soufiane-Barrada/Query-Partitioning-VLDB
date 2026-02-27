SELECT COALESCE("t4"."NATION_NAME", "t4"."NATION_NAME") AS "NATION_NAME", "t4"."TOTAL_REVENUE", "t4"."ORDER_COUNT"
FROM (SELECT "nation"."n_name", ANY_VALUE("nation"."n_name") AS "NATION_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE", COUNT(DISTINCT "s1"."o_orderkey") AS "ORDER_COUNT"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'ASIA') AS "t1"
INNER JOIN "TPCH"."nation" ON "t1"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN "TPCH"."lineitem" ON "supplier"."s_suppkey" = "lineitem"."l_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN "s1" ON "customer"."c_custkey" = "s1"."o_custkey") ON "lineitem"."l_orderkey" = "s1"."o_orderkey"
GROUP BY "nation"."n_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"