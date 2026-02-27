SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "N_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1994-01-01' AND "o_orderdate" < DATE '1995-01-01') AS "t"
INNER JOIN ("TPCH"."nation" INNER JOIN "TPCH"."customer" ON "nation"."n_nationkey" = "customer"."c_nationkey") ON "t"."o_custkey" = "customer"."c_custkey"
INNER JOIN ("TPCH"."supplier" INNER JOIN "TPCH"."lineitem" ON "supplier"."s_suppkey" = "lineitem"."l_suppkey" INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "t"."o_orderkey" = "lineitem"."l_orderkey"
GROUP BY "nation"."n_name"