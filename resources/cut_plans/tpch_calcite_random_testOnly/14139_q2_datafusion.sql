SELECT COALESCE("t2"."N_NAME", "t2"."N_NAME") AS "N_NAME", "t2"."TOTAL_REVENUE"
FROM (SELECT "s1"."n_name" AS "N_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "s1"
INNER JOIN "TPCH"."partsupp" ON "s1"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."orders" INNER JOIN "TPCH"."lineitem" ON "orders"."o_orderkey" = "lineitem"."l_orderkey") ON "partsupp"."ps_partkey" = "lineitem"."l_partkey"
GROUP BY "s1"."n_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"