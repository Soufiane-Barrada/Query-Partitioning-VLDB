SELECT COALESCE("t3"."N_NAME", "t3"."N_NAME") AS "N_NAME", "t3"."TOTAL_REVENUE"
FROM (SELECT "nation"."n_name" AS "N_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "s1"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."nation" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "partsupp"."ps_partkey" AND "lineitem"."l_suppkey" = "supplier"."s_suppkey") ON "s1"."o_orderkey" = "lineitem"."l_orderkey"
GROUP BY "nation"."n_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"