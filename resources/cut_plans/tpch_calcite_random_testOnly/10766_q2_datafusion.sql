SELECT COALESCE("t4"."P_NAME", "t4"."P_NAME") AS "P_NAME", "t4"."REVENUE"
FROM (SELECT "s1"."p_name" AS "P_NAME", SUM("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "REVENUE"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'EUROPE') AS "t0"
INNER JOIN "TPCH"."nation" ON "t0"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1995-01-01' AND "l_shipdate" < DATE '1995-02-01') AS "t1" INNER JOIN "s1" ON "t1"."l_partkey" = "s1"."p_partkey") ON "supplier"."s_suppkey" = "s1"."ps_suppkey"
GROUP BY "s1"."p_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"