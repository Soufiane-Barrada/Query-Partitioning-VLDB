SELECT COALESCE("customer"."c_custkey", "customer"."c_custkey") AS "C_CUSTKEY", "customer"."c_name" AS "C_NAME", SUM("orders"."o_totalprice") AS "TOTAL_ORDER_VALUE", COUNT(*) AS "ORDER_COUNT"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "customer"."c_custkey", "customer"."c_name"