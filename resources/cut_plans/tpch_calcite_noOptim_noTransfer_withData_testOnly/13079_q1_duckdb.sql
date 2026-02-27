SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "N_NAME", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_1"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
INNER JOIN "TPCH"."lineitem" ON "orders"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN "TPCH"."supplier" ON "lineitem"."l_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."partsupp" ON "lineitem"."l_partkey" = "partsupp"."ps_partkey" AND "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
WHERE "orders"."o_orderdate" >= DATE '1997-01-01' AND "orders"."o_orderdate" < DATE '1997-12-31'