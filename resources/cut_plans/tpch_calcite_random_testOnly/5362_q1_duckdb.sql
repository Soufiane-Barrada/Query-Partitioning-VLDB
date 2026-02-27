SELECT COALESCE("t3"."c_custkey", "t3"."c_custkey") AS "C_CUSTKEY", "t3"."c_name" AS "C_NAME", "lineitem0"."l_extendedprice" * (1 - "lineitem0"."l_discount") AS "FD_COL_2", "orders1"."o_orderkey", "lineitem0"."l_extendedprice", "lineitem0"."l_shipdate"
FROM (SELECT "customer"."c_custkey", "customer"."c_name", SUM("t1"."TOTAL_REVENUE") AS "REVENUE"
FROM (SELECT "t"."o_orderkey" AS "O_ORDERKEY", "t"."o_orderdate" AS "O_ORDERDATE", SUM("lineitem"."l_extendedprice" * (1 - "lineitem"."l_discount")) AS "TOTAL_REVENUE"
FROM (SELECT *
FROM "TPCH"."orders"
WHERE "o_orderdate" >= DATE '1995-01-01' AND "o_orderdate" < DATE '1996-01-01') AS "t"
INNER JOIN "TPCH"."lineitem" ON "t"."o_orderkey" = "lineitem"."l_orderkey"
GROUP BY "t"."o_orderkey", "t"."o_orderdate") AS "t1"
INNER JOIN ("TPCH"."customer" INNER JOIN "TPCH"."orders" AS "orders0" ON "customer"."c_custkey" = "orders0"."o_custkey") ON "t1"."O_ORDERKEY" = "orders0"."o_orderkey"
GROUP BY "customer"."c_custkey", "customer"."c_name"
ORDER BY 3 DESC NULLS FIRST
FETCH NEXT 10 ROWS ONLY) AS "t3"
INNER JOIN "TPCH"."orders" AS "orders1" ON "t3"."c_custkey" = "orders1"."o_custkey"
INNER JOIN "TPCH"."lineitem" AS "lineitem0" ON "orders1"."o_orderkey" = "lineitem0"."l_orderkey"