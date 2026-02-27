SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "N_NAME", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_1"
FROM "TPCH"."lineitem"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN "TPCH"."customer" ON "orders"."o_custkey" = "customer"."c_custkey"
INNER JOIN "TPCH"."supplier" ON "lineitem"."l_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
WHERE "orders"."o_orderdate" >= DATE '1997-01-01' AND "orders"."o_orderdate" < DATE '1998-01-01'