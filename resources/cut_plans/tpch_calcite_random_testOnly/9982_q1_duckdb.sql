SELECT COALESCE("supplier"."s_suppkey", "supplier"."s_suppkey") AS "S_SUPPKEY", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."lineitem"
INNER JOIN ("TPCH"."supplier" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "partsupp"."ps_partkey"
GROUP BY "supplier"."s_suppkey"