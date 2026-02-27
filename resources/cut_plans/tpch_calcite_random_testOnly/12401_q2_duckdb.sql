SELECT COALESCE("t4"."P_NAME", "t4"."P_NAME") AS "P_NAME", "t4"."REVENUE"
FROM (SELECT "part"."p_name" AS "P_NAME", SUM("s1"."l_extendedprice" * (1 - "s1"."l_discount")) AS "REVENUE"
FROM "s1"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey" AND "s1"."l_partkey" = "part"."p_partkey"
GROUP BY "part"."p_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"