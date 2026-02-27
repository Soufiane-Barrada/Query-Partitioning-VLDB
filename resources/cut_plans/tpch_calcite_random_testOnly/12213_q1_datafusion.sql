SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "n_name", "partsupp"."ps_supplycost" * "lineitem"."l_quantity" AS "FD_COL_1"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
INNER JOIN ("TPCH"."lineitem" INNER JOIN ("TPCH"."nation" INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey" INNER JOIN "TPCH"."partsupp" ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey") ON "lineitem"."l_partkey" = "partsupp"."ps_partkey") ON "orders"."o_orderkey" = "lineitem"."l_orderkey"