SELECT COALESCE("t2"."P_BRAND", "t2"."P_BRAND") AS "P_BRAND", "t2"."P_TYPE", "t2"."TOTAL_COST"
FROM (SELECT "s1"."p_brand" AS "P_BRAND", "s1"."p_type" AS "P_TYPE", SUM("s1"."ps_supplycost" * "s1"."ps_availqty") AS "TOTAL_COST"
FROM "TPCH"."region"
INNER JOIN "TPCH"."nation" ON "region"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN "s1" ON "supplier"."s_suppkey" = "s1"."ps_suppkey"
GROUP BY "s1"."p_brand", "s1"."p_type"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"