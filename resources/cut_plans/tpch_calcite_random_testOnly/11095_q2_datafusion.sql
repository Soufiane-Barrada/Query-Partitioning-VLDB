SELECT COALESCE("t2"."p_partkey", "t2"."p_partkey") AS "p_partkey", "t2"."p_name", "t2"."TOTAL_QUANTITY", "t2"."TOTAL_REVENUE"
FROM (SELECT "part"."p_partkey", "part"."p_name", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", SUM("lineitem"."l_extendedprice") AS "TOTAL_REVENUE"
FROM "s1"
INNER JOIN "TPCH"."nation" ON "s1"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "lineitem"."l_partkey" = "part"."p_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "part"."p_partkey", "part"."p_name"
ORDER BY 4 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t2"