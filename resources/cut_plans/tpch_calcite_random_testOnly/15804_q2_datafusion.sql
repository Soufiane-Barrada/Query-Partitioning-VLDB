SELECT COALESCE("t3"."P_NAME", "t3"."P_NAME") AS "P_NAME", "t3"."TOTAL_SALES"
FROM (SELECT "part"."p_name" AS "P_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_SALES"
FROM "s1"
INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."lineitem" ON "part"."p_partkey" = "lineitem"."l_partkey") ON "supplier"."s_suppkey" = "lineitem"."l_suppkey"
GROUP BY "part"."p_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"