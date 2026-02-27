SELECT COALESCE("customer"."c_custkey", "customer"."c_custkey") AS "C_CUSTKEY", "customer"."c_name" AS "C_NAME", "t"."o_orderkey" AS "O_ORDERKEY", "t"."o_orderdate" AS "O_ORDERDATE", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_SPENT", SUM("lineitem"."l_quantity") AS "TOTAL_ITEMS"
FROM "TPCH"."lineitem"
INNER JOIN ("TPCH"."customer" INNER JOIN (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1996-01-01' AND "o_orderdate" <= DATE '1996-12-31') AS "t" ON "customer"."c_custkey" = "t"."o_custkey") ON "lineitem"."l_orderkey" = "t"."o_orderkey"
GROUP BY "customer"."c_custkey", "customer"."c_name", "t"."o_orderkey", "t"."o_orderdate"