SELECT COALESCE("customer"."c_custkey", "customer"."c_custkey") AS "C_CUSTKEY", SUM("orders"."o_totalprice") AS "TOTAL_ORDERVALUE", SUM("orders"."o_totalprice") > 100000.00 AS "FD_COL_2"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
GROUP BY "customer"."c_custkey"