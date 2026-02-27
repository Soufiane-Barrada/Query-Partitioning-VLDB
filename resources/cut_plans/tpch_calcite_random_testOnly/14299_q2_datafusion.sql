SELECT COALESCE("t3"."N_NAME", "t3"."N_NAME") AS "N_NAME", "t3"."TOTAL_REVENUE"
FROM (SELECT "nation"."n_name" AS "N_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."customer" INNER JOIN "s1" ON "customer"."c_custkey" = "s1"."o_custkey") ON "lineitem"."l_orderkey" = "s1"."o_orderkey") ON "part"."p_partkey" = "lineitem"."l_partkey"
GROUP BY "nation"."n_name"
ORDER BY 2 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"