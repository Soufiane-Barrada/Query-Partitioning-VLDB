SELECT COALESCE("t3"."P_NAME", "t3"."P_NAME") AS "P_NAME", "t3"."TOTAL_QUANTITY", "t3"."REVENUE", "t3"."TOTAL_TAX", "t3"."TOTAL_DISCOUNT"
FROM (SELECT "s1"."p_name" AS "P_NAME", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "REVENUE", SUM("lineitem"."l_tax") AS "TOTAL_TAX", SUM("lineitem"."l_discount") AS "TOTAL_DISCOUNT"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'ASIA') AS "t0"
INNER JOIN "TPCH"."nation" ON "t0"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN "s1" ON "lineitem"."l_partkey" = "s1"."ps_partkey") ON "supplier"."s_suppkey" = "s1"."ps_suppkey"
GROUP BY "s1"."p_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 100 ROWS ONLY) AS "t3"