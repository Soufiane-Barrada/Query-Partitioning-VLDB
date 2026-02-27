SELECT COALESCE("t2"."p_name", "t2"."p_name") AS "p_name", "t2"."TOTAL_QUANTITY", "t2"."TOTAL_SALES"
FROM (SELECT "s1"."p_name", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", SUM("lineitem"."l_extendedprice") AS "TOTAL_SALES"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'ASIA') AS "t0"
INNER JOIN "TPCH"."nation" ON "t0"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN "s1" ON "lineitem"."l_partkey" = "s1"."p_partkey") ON "supplier"."s_suppkey" = "s1"."ps_suppkey"
GROUP BY "s1"."p_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"