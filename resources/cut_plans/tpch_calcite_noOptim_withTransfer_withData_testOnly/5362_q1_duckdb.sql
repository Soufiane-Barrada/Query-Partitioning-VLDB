SELECT COALESCE("t4"."C_CUSTKEY", "t4"."C_CUSTKEY") AS "C_CUSTKEY", "t4"."C_NAME", "t4"."REVENUE"
FROM (SELECT "customer"."c_custkey" AS "C_CUSTKEY", "customer"."c_name" AS "C_NAME", SUM("t1"."TOTAL_REVENUE") AS "REVENUE"
FROM "TPCH"."customer"
INNER JOIN "TPCH"."orders" ON "customer"."c_custkey" = "orders"."o_custkey"
INNER JOIN (SELECT "orders0"."o_orderkey" AS "O_ORDERKEY", "orders0"."o_orderdate" AS "O_ORDERDATE", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM "TPCH"."orders" AS "orders0"
INNER JOIN "TPCH"."lineitem" ON "orders0"."o_orderkey" = "lineitem"."l_orderkey"
WHERE "orders0"."o_orderdate" >= DATE '1995-01-01' AND "orders0"."o_orderdate" < DATE '1996-01-01'
GROUP BY "orders0"."o_orderkey", "orders0"."o_orderdate") AS "t1" ON "orders"."o_orderkey" = "t1"."O_ORDERKEY"
GROUP BY "customer"."c_custkey", "customer"."c_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t4"