SELECT COALESCE("t3"."P_BRAND", "t3"."P_BRAND") AS "P_BRAND", "t3"."P_TYPE", "t3"."TOTAL_COST"
FROM (SELECT "part"."p_brand" AS "P_BRAND", "part"."p_type" AS "P_TYPE", SUM("partsupp"."ps_supplycost" * "partsupp"."ps_availqty") AS "TOTAL_COST"
FROM "s1"
INNER JOIN "TPCH"."supplier" ON "s1"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "part"."p_brand", "part"."p_type"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"