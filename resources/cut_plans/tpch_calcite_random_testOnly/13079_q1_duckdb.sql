SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "N_NAME", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."customer"
INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1997-01-01' AND "o_orderdate" < DATE '1997-12-31') AS "t" ON "customer"."c_custkey" = "t"."o_custkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."nation" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "partsupp"."ps_partkey" AND "lineitem"."l_suppkey" = "supplier"."s_suppkey") ON "t"."o_orderkey" = "lineitem"."l_orderkey"
GROUP BY "nation"."n_name"