SELECT COALESCE("nation"."n_name", "nation"."n_name") AS "n_name", "region"."r_name", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_2"
FROM "TPCH"."lineitem"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
INNER JOIN "TPCH"."customer" ON "orders"."o_custkey" = "customer"."c_custkey"
INNER JOIN "TPCH"."supplier" ON "lineitem"."l_suppkey" = "supplier"."s_suppkey"
INNER JOIN "TPCH"."nation" ON "supplier"."s_nationkey" = "nation"."n_nationkey"
INNER JOIN "TPCH"."region" ON "nation"."n_regionkey" = "region"."r_regionkey"
WHERE "orders"."o_orderdate" >= '1997-01-01' AND "orders"."o_orderdate" < '1997-12-31'