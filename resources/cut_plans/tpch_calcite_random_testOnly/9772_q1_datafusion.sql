SELECT COALESCE("t3"."C_CUSTKEY", "t3"."C_CUSTKEY") AS "C_CUSTKEY", "t3"."C_NAME", "t3"."TOTAL_REVENUE"
FROM (SELECT "customer"."c_custkey", "customer"."c_name", SUM("t1"."REVENUE") AS "TOTAL_REVENUE"
FROM (SELECT "t"."o_orderkey" AS "O_ORDERKEY", "t"."o_orderdate" AS "O_ORDERDATE", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "REVENUE", COUNT(DISTINCT "lineitem"."l_suppkey") AS "SUPPLIER_COUNT"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1994-01-01' AND "o_orderdate" <= DATE '1996-12-31') AS "t"
INNER JOIN "TPCH"."lineitem" ON "t"."o_orderkey" = "lineitem"."l_orderkey"
GROUP BY "t"."o_orderkey", "t"."o_orderdate") AS "t1"
INNER JOIN ("TPCH"."customer" INNER JOIN "TPCH"."orders" AS "orders0" ON "customer"."c_custkey" = "orders0"."o_custkey") ON "t1"."O_ORDERKEY" = "orders0"."o_orderkey"
GROUP BY "customer"."c_custkey", "customer"."c_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 5 ROWS ONLY) AS "t3"