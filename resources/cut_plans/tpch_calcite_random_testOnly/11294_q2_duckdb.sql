SELECT COALESCE("t3"."P_PARTKEY", "t3"."P_PARTKEY") AS "P_PARTKEY", "t3"."P_NAME", "t3"."TOTAL_QUANTITY", "t3"."TOTAL_REVENUE"
FROM (SELECT "part"."p_partkey" AS "P_PARTKEY", "part"."p_name" AS "P_NAME", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "s1"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "lineitem"."l_partkey" = "part"."p_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "part"."p_partkey", "part"."p_name"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"