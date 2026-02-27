SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "n_name", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_1", "supplier"."s_acctbal", "orders"."o_orderkey", "customer"."c_custkey"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."nation" ON "customer"."c_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
INNER JOIN "TPCH"."lineitem" ON "orders"."o_orderkey" = "lineitem"."l_orderkey"
INNER JOIN "TPCH"."partsupp" ON "lineitem"."l_partkey" = "partsupp"."ps_partkey"
INNER JOIN "TPCH"."supplier" ON "partsupp"."ps_suppkey" = "supplier"."s_suppkey"
WHERE "lineitem"."l_shipdate" >= DATE '1997-01-01' AND "lineitem"."l_shipdate" < DATE '1997-12-31'