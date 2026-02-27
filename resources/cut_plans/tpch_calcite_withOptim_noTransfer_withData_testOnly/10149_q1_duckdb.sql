SELECT COALESCE("part"."p_name", "part"."p_name") AS "p_name", SUM("lineitem"."l_quantity") AS "TOTAL_QUANTITY", SUM("lineitem"."l_extendedprice") AS "TOTAL_REVENUE"
FROM (SELECT *
FROM "TPCH"."region"
WHERE "r_name" = 'ASIA') AS "t"
INNER JOIN "TPCH"."nation" ON "t"."r_regionkey" = "nation"."n_regionkey"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "lineitem"."l_partkey" = "part"."p_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
GROUP BY "part"."p_name"