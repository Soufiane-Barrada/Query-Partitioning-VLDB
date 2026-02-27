SELECT COALESCE("t3"."P_NAME", "t3"."P_NAME") AS "P_NAME", "t3"."TOTAL_QUANTITY", "t3"."TOTAL_REVENUE"
FROM (SELECT "s1"."p_name" AS "P_NAME", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'ASIA') AS "t0"
INNER JOIN "TPCH"."nation" ON "t0"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN "TPCH"."lineitem" ON "supplier"."s_suppkey" = "lineitem"."l_suppkey"
INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey" AND "lineitem"."l_partkey" = "s1"."p_partkey"
GROUP BY "s1"."p_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"