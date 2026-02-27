SELECT COALESCE("t4"."P_BRAND", "t4"."P_BRAND") AS "P_BRAND", "t4"."P_TYPE", "t4"."REVENUE"
FROM (SELECT "s1"."p_brand" AS "P_BRAND", "s1"."p_type" AS "P_TYPE", SUM("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "REVENUE"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'ASIA') AS "t0"
INNER JOIN "TPCH"."nation" ON "t0"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1995-01-01' AND "l_shipdate" < DATE '1995-02-01') AS "t1" INNER JOIN "s1" ON "t1"."l_partkey" = "s1"."ps_partkey") ON "supplier"."s_suppkey" = "s1"."ps_suppkey"
GROUP BY "s1"."p_brand", "s1"."p_type"
ORDER BY 3 DESC NULLS FIRST) AS "t4"