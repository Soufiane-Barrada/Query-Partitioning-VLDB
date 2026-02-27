SELECT COALESCE("t2"."N_NAME", "t2"."N_NAME") AS "N_NAME", "t2"."REVENUE"
FROM (SELECT "s1"."n_name" AS "N_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "REVENUE"
FROM "s1"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "lineitem"."l_partkey" = "part"."p_partkey") ON "s1"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "s1"."n_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"