SELECT COALESCE("customer"."c_custkey", "customer"."c_custkey") AS "c_custkey", "customer"."c_name", SUM("orders"."o_totalprice") AS "CUSTOMER_TOTAL"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "customer"."c_custkey", "customer"."c_name"