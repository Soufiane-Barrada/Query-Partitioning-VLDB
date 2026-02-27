SELECT COALESCE("orders"."o_orderkey", "orders"."o_orderkey") AS "O_ORDERKEY", "customer"."c_name", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_2"
FROM "TPCH"."lineitem"
INNER JOIN ("TPCH"."customer" INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey") ON "lineitem"."l_orderkey" = "orders"."o_orderkey"