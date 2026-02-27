SELECT COALESCE("t4"."P_BRAND", "t4"."P_BRAND") AS "P_BRAND", "t4"."P_TYPE", "t4"."TOTAL_QUANTITY", "t4"."TOTAL_SALES"
FROM (SELECT "part"."p_brand" AS "P_BRAND", "part"."p_type" AS "P_TYPE", SUM("t1"."l_quantity") AS "TOTAL_QUANTITY", SUM("t1"."l_extendedprice" * (1 - "t1"."l_discount")) AS "TOTAL_SALES"
FROM "s1"
INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1996-01-01' AND "l_shipdate" < DATE '1996-01-31') AS "t1" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "t1"."l_partkey" = "part"."p_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "part"."p_brand", "part"."p_type"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"