SELECT COALESCE("lineitem"."l_shipmode", "lineitem"."l_shipmode") AS "L_SHIPMODE", "orders"."o_orderkey", "lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount") AS "FD_COL_2"
FROM "TPCH"."lineitem"
INNER JOIN "TPCH"."orders" ON "lineitem"."l_orderkey" = "orders"."o_orderkey"
WHERE "lineitem"."l_shipdate" >= DATE '1996-01-01' AND "lineitem"."l_shipdate" < DATE '1996-12-31'