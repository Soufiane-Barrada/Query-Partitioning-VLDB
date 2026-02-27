SELECT COALESCE("t4"."P_PARTKEY", "t4"."P_PARTKEY") AS "P_PARTKEY", "t4"."REVENUE"
FROM (SELECT "s1"."p_partkey" AS "P_PARTKEY", SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")) AS "REVENUE"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'ASIA') AS "t1"
INNER JOIN "TPCH"."nation" ON "t1"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey"
GROUP BY "s1"."p_partkey"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"