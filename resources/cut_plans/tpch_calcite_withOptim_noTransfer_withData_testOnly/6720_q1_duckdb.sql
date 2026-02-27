SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "n_name", "t"."l_extendedprice" * (1 - "t"."l_discount") AS "FD_COL_1", "orders"."o_orderkey", "customer"."c_acctbal"
FROM "TPCH"."nation"
INNER JOIN "TPCH"."supplier" ON "nation"."n_nationkey" = "supplier"."s_nationkey"
INNER JOIN ("TPCH"."part" INNER JOIN "TPCH"."partsupp" ON "part"."p_partkey" = "partsupp"."ps_partkey") ON "supplier"."s_suppkey" = "partsupp"."ps_suppkey"
INNER JOIN ("TPCH"."customer" INNER JOIN ((SELECT *
FROM "TPCH"."lineitem"
WHERE "l_shipdate" >= DATE '1997-01-01' AND "l_shipdate" < DATE '1997-12-31') AS "t" INNER JOIN "TPCH"."orders" ON "t"."l_orderkey" = "orders"."o_orderkey") ON "customer"."c_custkey" = "orders"."o_custkey") ON "partsupp"."ps_partkey" = "t"."l_partkey" AND "supplier"."s_suppkey" = "t"."l_suppkey"